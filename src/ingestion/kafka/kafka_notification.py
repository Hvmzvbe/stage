"""
===========================================================================
Module : kafka_notification.py
Projet : Plateforme MLOps - Détection de Fraude (Al Barid Bank)
Auteur : Hamza (PFE EMSI 2025-2026)
---------------------------------------------------------------------------
Rôle   : Consumer Kafka — Écoute le topic `quarantine_transactions`
         et envoie des e-mails d'alerte professionnels à l'équipe support.

         Flux :
           quarantine_transactions (topic Kafka — JSON)
               │
           ┌───▼──────────────┐
           │ DÉSÉRIALISATION  │  JSON → dict Python
           └───┬──────────────┘
               │
           ┌───▼──────────────┐
           │ CLASSIFICATION   │  Sévérité (CRITICAL / HIGH / MEDIUM)
           └───┬──────────────┘
               │
           ┌───▼──────────────┐
           │ COMPOSITION MAIL │  HTML professionnel + résumé texte
           └───┬──────────────┘
               │
           ┌───▼──────────────┐
           │ ENVOI SMTP       │  smtplib (TLS) + vérification delivery
           └───┬──────────────┘
               │
           ┌───▼──────────────┐
           │ LOGGING / RETRY  │  Log local + retry si échec SMTP
           └──────────────────┘

Usage  :
    python kafka_notification.py
    python kafka_notification.py --bootstrap 10.0.0.5:9092
    python kafka_notification.py --smtp-host smtp.gmail.com --smtp-port 587
    python kafka_notification.py --dry-run   # Affiche le mail sans envoyer
    python kafka_notification.py --test-smtp  # Teste la connexion SMTP

Requires : pip install confluent-kafka jinja2
===========================================================================
"""

import argparse
import json
import logging
import os
import signal
import socket
import smtplib
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from enum import Enum
from pathlib import Path
from typing import Optional

from confluent_kafka import Consumer, KafkaError, KafkaException

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(name)s | %(message)s",
)
logger = logging.getLogger("kafka_notification")


# ===========================================================================
#  CONFIGURATION
# ===========================================================================

class NotificationConfig:
    """Configuration centralisée du consumer notification."""

    def __init__(
        self,
        bootstrap: str = "127.0.0.1:9092",
        group_id: str = "notification-alert-group",
        topic: str = "quarantine_transactions",
        # ── SMTP ──
        smtp_host: str = "smtp.gmail.com",
        smtp_port: int = 587,
        smtp_user: str = "",
        smtp_password: str = "",
        smtp_use_tls: bool = True,
        # ── Destinataires ──
        sender_email: str = "",
        sender_name: str = "Système Détection Fraude - Al Barid Bank",
        recipients: list[str] = None,
        cc_recipients: list[str] = None,
        # ── Comportement ──
        dry_run: bool = False,
        batch_window_sec: int = 30,
        max_batch_size: int = 10,
        retry_max: int = 3,
        retry_delay_sec: float = 5.0,
        log_dir: str = "./data/notification_logs",
    ):
        self.bootstrap = bootstrap
        self.group_id = group_id
        self.topic = topic

        # SMTP
        self.smtp_host = smtp_host
        self.smtp_port = smtp_port
        self.smtp_user = smtp_user or os.getenv("SMTP_USER", "")
        self.smtp_password = smtp_password or os.getenv("SMTP_PASSWORD", "")
        self.smtp_use_tls = smtp_use_tls

        # Destinataires
        self.sender_email = sender_email
        self.sender_name = sender_name
        self.recipients = recipients or [
            ""
        ]
        self.cc_recipients = cc_recipients or []

        # Comportement
        self.dry_run = dry_run
        self.batch_window_sec = batch_window_sec
        self.max_batch_size = max_batch_size
        self.retry_max = retry_max
        self.retry_delay_sec = retry_delay_sec

        # Log local
        self.log_dir = Path(log_dir)
        self.log_dir.mkdir(parents=True, exist_ok=True)

    @property
    def consumer_config(self) -> dict:
        return {
            "bootstrap.servers": self.bootstrap,
            "group.id": self.group_id,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": True,
            "session.timeout.ms": 30000,
        }


# ===========================================================================
#  CLASSIFICATION DE SÉVÉRITÉ
# ===========================================================================

class AlertSeverity(Enum):
    CRITICAL = "CRITICAL"   # Fraude probable, montant élevé, carte expirée
    HIGH     = "HIGH"       # Échec Luhn, montant aberrant, PAN corrompu
    MEDIUM   = "MEDIUM"     # Warning ML, champ manquant, incohérence POS

    @property
    def color(self) -> str:
        return {
            "CRITICAL": "#DC2626",
            "HIGH":     "#EA580C",
            "MEDIUM":   "#CA8A04",
        }[self.value]

    @property
    def emoji(self) -> str:
        return {
            "CRITICAL": "🔴",
            "HIGH":     "🟠",
            "MEDIUM":   "🟡",
        }[self.value]


# Règles → Sévérité
SEVERITY_MAP: dict[str, AlertSeverity] = {
    # CRITICAL
    "LUHN_CHECK":           AlertSeverity.CRITICAL,
    "CARD_EXPIRED":         AlertSeverity.CRITICAL,
    "AMOUNT_MAX":           AlertSeverity.CRITICAL,
    "MTI_SUPPORTED":        AlertSeverity.CRITICAL,
    # HIGH
    "AMOUNT_ZERO":          AlertSeverity.HIGH,
    "CURRENCY_VALID":       AlertSeverity.HIGH,
    "PAN_LENGTH":           AlertSeverity.HIGH,
    "MTI_FORMAT":           AlertSeverity.HIGH,
    "MTI_PRESENT":          AlertSeverity.HIGH,
    "NUMERIC_ONLY":         AlertSeverity.HIGH,
    "FIXED_LENGTH":         AlertSeverity.HIGH,
    "DE43_CHARSET":         AlertSeverity.HIGH,
    # MEDIUM
    "ML_FIELD_REQUIRED":    AlertSeverity.MEDIUM,
    "TIMESTAMP_FUTURE":     AlertSeverity.MEDIUM,
    "TIMESTAMP_STALE":      AlertSeverity.MEDIUM,
    "DE43_LENGTH":          AlertSeverity.MEDIUM,
    "DE43_COUNTRY":         AlertSeverity.MEDIUM,
    "DE43_CITY":            AlertSeverity.MEDIUM,
    "TERMINAL_ID_EMPTY":    AlertSeverity.MEDIUM,
    "BIN_UNKNOWN":          AlertSeverity.MEDIUM,
    "CHANNEL_POS_COHERENCE": AlertSeverity.MEDIUM,
    "PROCESSING_CODE_PREFIX": AlertSeverity.MEDIUM,
    "PC_MTI_COHERENCE":     AlertSeverity.MEDIUM,
}


def classify_severity(errors: list[dict]) -> AlertSeverity:
    """Détermine la sévérité globale à partir des erreurs de validation."""
    max_severity = AlertSeverity.MEDIUM
    for err in errors:
        rule = err.get("rule", "")
        sev = SEVERITY_MAP.get(rule, AlertSeverity.MEDIUM)
        if sev == AlertSeverity.CRITICAL:
            return AlertSeverity.CRITICAL
        if sev == AlertSeverity.HIGH:
            max_severity = AlertSeverity.HIGH
    return max_severity


# ===========================================================================
#  HELPERS — Extraction et formatage
# ===========================================================================

def mask_pan(pan: Optional[str]) -> str:
    """Masque PAN pour conformité PCI DSS."""
    if not pan or not isinstance(pan, str) or len(pan) < 8:
        return pan or "N/A"
    return f"{pan[:6]}****{pan[-4:]}"


def format_amount(amount_str: Optional[str]) -> str:
    """Convertit le montant centimes → MAD lisible."""
    if not amount_str or not isinstance(amount_str, str) or not amount_str.isdigit():
        return "N/A"
    return f"{int(amount_str) / 100:,.2f} MAD"


def format_datetime_de7(dt_str: Optional[str]) -> str:
    """Formate DE 7 (MMDDHHmmss) en date lisible."""
    if not dt_str or len(dt_str) != 10:
        return dt_str or "N/A"
    try:
        month, day = dt_str[0:2], dt_str[2:4]
        hour, minute, sec = dt_str[4:6], dt_str[6:8], dt_str[8:10]
        return f"{day}/{month}/{datetime.now().year} à {hour}:{minute}:{sec}"
    except Exception:
        return dt_str


def get_layer_label(layer: Optional[str]) -> str:
    """Traduit le code couche en label lisible."""
    labels = {
        "STRUCTURAL": "Couche 1 — Conformité Structurelle (Protocole ISO 8583)",
        "BUSINESS":   "Couche 2 — Validité Monétique (Règles Métier)",
        "ML_QUALITY": "Couche 3 — Intégrité ML (Qualité Données)",
    }
    return labels.get(layer, layer or "Inconnue")


# ===========================================================================
#  COMPOSITION DU MAIL HTML
# ===========================================================================

def compose_alert_email(
    transaction: dict,
    severity: AlertSeverity,
    alert_id: str,
) -> tuple[str, str, str]:
    """
    Compose un e-mail d'alerte professionnel.

    Returns
    -------
    tuple[str, str, str]
        (subject, html_body, text_body)
    """
    # ── Extraction des champs ──
    validation = transaction.get("_validation", {})
    errors = validation.get("errors", [])
    warnings = validation.get("warnings", [])
    layer_failed = validation.get("layer_failed", "UNKNOWN")

    pan_raw = transaction.get("pan")
    pan_display = mask_pan(pan_raw)
    mti = transaction.get("mti", "N/A")
    amount = format_amount(transaction.get("amount_transaction"))
    timestamp = format_datetime_de7(transaction.get("transmission_datetime"))
    terminal = transaction.get("terminal_id", "N/A")
    merchant = transaction.get("card_acceptor_name_location", "N/A")
    currency = transaction.get("currency_code_transaction", "N/A")
    proc_code = transaction.get("processing_code", "N/A")
    stan = transaction.get("stan", "N/A")
    quarantined_at = transaction.get("_quarantined_at", datetime.now().isoformat())

    # Déterminer le canal
    channel = "N/A"
    if terminal:
        if terminal.startswith("GAB"):
            channel = "GAB (Guichet Automatique)"
        elif terminal.startswith("TPE"):
            channel = "TPE (Terminal de Paiement)"
        elif terminal.startswith("ECOM"):
            channel = "E-Commerce"

    # Extraire ville/pays depuis DE 43
    city, country = "N/A", "N/A"
    if merchant and len(merchant) >= 40:
        city = merchant[25:38].strip() or "N/A"
        country = merchant[38:40].strip() or "N/A"

    # ══════════════════════════════════════════
    #  SUJET
    # ══════════════════════════════════════════
    subject = (
        f"{severity.emoji} [{severity.value}] Transaction Rejetée — "
        f"{layer_failed} | PAN {pan_display} | {amount} | {channel}"
    )

    # ══════════════════════════════════════════
    #  CORPS HTML
    # ══════════════════════════════════════════
    errors_html = ""
    for i, err in enumerate(errors, 1):
        errors_html += f"""
        <tr style="border-bottom: 1px solid #E5E7EB;">
            <td style="padding: 8px 12px; font-weight: 600; color: #DC2626;">{i}</td>
            <td style="padding: 8px 12px;"><code style="background: #FEE2E2; padding: 2px 6px; border-radius: 3px; color: #991B1B;">{err.get('rule', 'N/A')}</code></td>
            <td style="padding: 8px 12px;">{err.get('field', 'N/A')}</td>
            <td style="padding: 8px 12px;">{err.get('message', 'N/A')}</td>
            <td style="padding: 8px 12px; font-family: monospace; font-size: 12px;">{err.get('value', '—')}</td>
        </tr>"""

    warnings_html = ""
    if warnings:
        warnings_html = """
        <div style="margin-top: 20px; padding: 12px 16px; background: #FFFBEB; border-left: 4px solid #F59E0B; border-radius: 4px;">
            <strong style="color: #B45309;">⚠️ Warnings associés :</strong>
            <ul style="margin: 8px 0 0 0; padding-left: 20px;">"""
        for w in warnings:
            warnings_html += f'<li style="margin: 4px 0; color: #92400E;">{w.get("rule", "")} — {w.get("message", "")}</li>'
        warnings_html += "</ul></div>"

    html_body = f"""
<!DOCTYPE html>
<html lang="fr">
<head><meta charset="UTF-8"></head>
<body style="margin: 0; padding: 0; font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; background-color: #F3F4F6;">
<div style="max-width: 720px; margin: 20px auto; background: #FFFFFF; border-radius: 8px; overflow: hidden; box-shadow: 0 2px 8px rgba(0,0,0,0.08);">

    <!-- ── HEADER ── -->
    <div style="background: {severity.color}; padding: 20px 24px; color: white;">
        <div style="font-size: 12px; text-transform: uppercase; letter-spacing: 1px; opacity: 0.9;">
            Al Barid Bank — Système Détection Fraude
        </div>
        <div style="font-size: 22px; font-weight: 700; margin-top: 6px;">
            {severity.emoji} Alerte {severity.value} — Transaction Rejetée
        </div>
        <div style="font-size: 13px; margin-top: 4px; opacity: 0.85;">
            ID Alerte : {alert_id} | {quarantined_at}
        </div>
    </div>

    <!-- ── RÉSUMÉ RAPIDE ── -->
    <div style="padding: 20px 24px; background: #F9FAFB; border-bottom: 1px solid #E5E7EB;">
        <table style="width: 100%; border-collapse: collapse;">
            <tr>
                <td style="padding: 6px 0; width: 50%;">
                    <span style="color: #6B7280; font-size: 12px;">PAN (masqué)</span><br>
                    <strong style="font-size: 16px; font-family: monospace;">{pan_display}</strong>
                </td>
                <td style="padding: 6px 0; width: 50%;">
                    <span style="color: #6B7280; font-size: 12px;">Montant</span><br>
                    <strong style="font-size: 16px; color: #DC2626;">{amount}</strong>
                </td>
            </tr>
            <tr>
                <td style="padding: 6px 0;">
                    <span style="color: #6B7280; font-size: 12px;">Canal</span><br>
                    <strong>{channel}</strong>
                </td>
                <td style="padding: 6px 0;">
                    <span style="color: #6B7280; font-size: 12px;">Localisation</span><br>
                    <strong>{city}, {country}</strong>
                </td>
            </tr>
        </table>
    </div>

    <!-- ── DÉTAILS TRANSACTION ── -->
    <div style="padding: 20px 24px;">
        <h3 style="margin: 0 0 12px 0; color: #111827; font-size: 16px;">
            📋 Détails de la Transaction
        </h3>
        <table style="width: 100%; border-collapse: collapse; font-size: 14px;">
            <tr style="border-bottom: 1px solid #F3F4F6;">
                <td style="padding: 8px 0; color: #6B7280; width: 40%;">MTI (Type Message)</td>
                <td style="padding: 8px 0; font-weight: 600; font-family: monospace;">{mti}</td>
            </tr>
            <tr style="border-bottom: 1px solid #F3F4F6;">
                <td style="padding: 8px 0; color: #6B7280;">Processing Code (DE 3)</td>
                <td style="padding: 8px 0; font-family: monospace;">{proc_code}</td>
            </tr>
            <tr style="border-bottom: 1px solid #F3F4F6;">
                <td style="padding: 8px 0; color: #6B7280;">STAN (DE 11)</td>
                <td style="padding: 8px 0; font-family: monospace;">{stan}</td>
            </tr>
            <tr style="border-bottom: 1px solid #F3F4F6;">
                <td style="padding: 8px 0; color: #6B7280;">Horodatage (DE 7)</td>
                <td style="padding: 8px 0;">{timestamp}</td>
            </tr>
            <tr style="border-bottom: 1px solid #F3F4F6;">
                <td style="padding: 8px 0; color: #6B7280;">Terminal ID (DE 41)</td>
                <td style="padding: 8px 0; font-family: monospace;">{terminal}</td>
            </tr>
            <tr style="border-bottom: 1px solid #F3F4F6;">
                <td style="padding: 8px 0; color: #6B7280;">Code Devise (DE 49)</td>
                <td style="padding: 8px 0; font-family: monospace;">{currency}</td>
            </tr>
            <tr>
                <td style="padding: 8px 0; color: #6B7280;">Commerçant (DE 43)</td>
                <td style="padding: 8px 0; font-size: 12px; font-family: monospace;">{merchant[:50] if merchant else 'N/A'}</td>
            </tr>
        </table>
    </div>

    <!-- ── MOTIF DE REJET ── -->
    <div style="padding: 20px 24px; background: #FEF2F2; border-top: 1px solid #FECACA;">
        <h3 style="margin: 0 0 8px 0; color: #991B1B; font-size: 16px;">
            🚫 Motif de Rejet
        </h3>
        <div style="margin-bottom: 12px; padding: 8px 12px; background: white; border-radius: 4px; border: 1px solid #FECACA;">
            <span style="color: #6B7280; font-size: 12px;">Couche de validation ayant échoué :</span><br>
            <strong style="color: #DC2626;">{get_layer_label(layer_failed)}</strong>
        </div>

        <table style="width: 100%; border-collapse: collapse; font-size: 13px; background: white; border-radius: 4px; overflow: hidden;">
            <thead>
                <tr style="background: #991B1B; color: white;">
                    <th style="padding: 8px 12px; text-align: left;">#</th>
                    <th style="padding: 8px 12px; text-align: left;">Règle</th>
                    <th style="padding: 8px 12px; text-align: left;">Champ</th>
                    <th style="padding: 8px 12px; text-align: left;">Description</th>
                    <th style="padding: 8px 12px; text-align: left;">Valeur</th>
                </tr>
            </thead>
            <tbody>
                {errors_html}
            </tbody>
        </table>

        {warnings_html}
    </div>

    

    <!-- ── FOOTER ── -->
    <div style="padding: 16px 24px; background: #1F2937; color: #9CA3AF; font-size: 11px; text-align: center;">
        <div>Al Barid Bank — Plateforme MLOps Détection de Fraude</div>
        <div style="margin-top: 4px;">
            Ce message est généré automatiquement par le pipeline d'ingestion.
            Ne pas répondre à cette adresse.
        </div>
        <div style="margin-top: 4px; color: #6B7280;">
            Alerte ID : {alert_id} | Sévérité : {severity.value} | Couche : {layer_failed}
        </div>
    </div>

</div>
</body>
</html>"""

    # ══════════════════════════════════════════
    #  CORPS TEXTE (fallback)
    # ══════════════════════════════════════════
    errors_text = ""
    for i, err in enumerate(errors, 1):
        errors_text += (
            f"  {i}. [{err.get('rule', '?')}] Champ: {err.get('field', '?')}\n"
            f"     {err.get('message', '')}\n"
            f"     Valeur: {err.get('value', '—')}\n\n"
        )

    text_body = f"""
{'=' * 65}
  ALERTE {severity.value} — TRANSACTION REJETÉE
  Al Barid Bank — Système Détection Fraude
{'=' * 65}

  ID Alerte       : {alert_id}
  Date            : {quarantined_at}
  Sévérité        : {severity.value}

── RÉSUMÉ ──
  PAN (masqué)    : {pan_display}
  Montant         : {amount}
  Canal           : {channel}
  Localisation    : {city}, {country}

── DÉTAILS TRANSACTION ──
  MTI             : {mti}
  Processing Code : {proc_code}
  STAN            : {stan}
  Horodatage      : {timestamp}
  Terminal ID     : {terminal}
  Code Devise     : {currency}

── MOTIF DE REJET ──
  Couche échouée  : {get_layer_label(layer_failed)}

  Erreurs :
{errors_text}

── ACTIONS RECOMMANDÉES ──
  1. Vérifier la transaction dans le système Quarantine.
  2. Consulter l'historique du PAN {pan_display}.
  3. Si fraude confirmée : bloquer la carte.
  4. Documenter l'incident.
  5. Escalader si CRITICAL ou pattern récurrent.

{'=' * 65}
  Message automatique — Ne pas répondre.
{'=' * 65}
"""

    return subject, html_body, text_body


# ===========================================================================
#  ENVOI SMTP AVEC VÉRIFICATION
# ===========================================================================

@dataclass
class DeliveryResult:
    """Résultat de l'envoi d'un e-mail."""
    success: bool
    alert_id: str
    recipients: list[str]
    smtp_response: Optional[str] = None
    error: Optional[str] = None
    attempts: int = 1
    sent_at: Optional[str] = None

    def to_dict(self) -> dict:
        return {
            "success": self.success,
            "alert_id": self.alert_id,
            "recipients": self.recipients,
            "smtp_response": self.smtp_response,
            "error": self.error,
            "attempts": self.attempts,
            "sent_at": self.sent_at,
        }


class EmailSender:
    """
    Gère l'envoi SMTP avec :
    - Connexion TLS sécurisée
    - Vérification de la livraison (SMTP response code)
    - Retry automatique en cas d'échec
    - Log local de chaque envoi
    """

    def __init__(self, config: NotificationConfig):
        self.config = config
        self._log_file = config.log_dir / "email_delivery.jsonl"

    def send_alert(
        self,
        subject: str,
        html_body: str,
        text_body: str,
        alert_id: str,
    ) -> DeliveryResult:
        """
        Envoie un e-mail d'alerte avec retry.

        Vérification de livraison :
        - Code SMTP 250 = accepté par le serveur (delivery confirmée)
        - Code SMTP 4xx = erreur temporaire → retry
        - Code SMTP 5xx = erreur permanente → log + abandon
        """
        cfg = self.config

        if cfg.dry_run:
            logger.info(
                "DRY RUN — E-mail NON envoyé\n"
                "  Subject: %s\n"
                "  To: %s",
                subject, ", ".join(cfg.recipients),
            )
            print("\n" + "=" * 65)
            print("  DRY RUN — CONTENU DU MAIL (texte)")
            print("=" * 65)
            print(text_body)
            result = DeliveryResult(
                success=True,
                alert_id=alert_id,
                recipients=cfg.recipients,
                smtp_response="DRY_RUN — aucun envoi",
                sent_at=datetime.now().isoformat(),
            )
            self._log_delivery(result)
            return result

        # ── Construction du message MIME ──
        msg = MIMEMultipart("alternative")
        msg["From"] = f"{cfg.sender_name} <{cfg.sender_email}>"
        msg["To"] = ", ".join(cfg.recipients)
        if cfg.cc_recipients:
            msg["Cc"] = ", ".join(cfg.cc_recipients)
        msg["Subject"] = subject
        msg["X-Alert-ID"] = alert_id
        msg["X-Priority"] = "1"  # Haute priorité
        msg["Importance"] = "high"

        msg.attach(MIMEText(text_body, "plain", "utf-8"))
        msg.attach(MIMEText(html_body, "html", "utf-8"))

        all_recipients = cfg.recipients + cfg.cc_recipients

        # ── Envoi avec retry ──
        last_error = None
        for attempt in range(1, cfg.retry_max + 1):
            try:
                logger.info(
                    "SMTP ENVOI [tentative %d/%d] → %s",
                    attempt, cfg.retry_max, ", ".join(all_recipients),
                )

                with smtplib.SMTP(cfg.smtp_host, cfg.smtp_port, timeout=30) as server:
                    server.ehlo()
                    if cfg.smtp_use_tls:
                        server.starttls()
                        server.ehlo()
                    if cfg.smtp_user and cfg.smtp_password:
                        server.login(cfg.smtp_user, cfg.smtp_password)

                    # sendmail retourne un dict des rejets
                    rejected = server.sendmail(
                        cfg.sender_email,
                        all_recipients,
                        msg.as_string(),
                    )

                    if rejected:
                        # Certains destinataires rejetés
                        logger.warning(
                            "SMTP PARTIAL — Rejetés : %s", rejected,
                        )
                        result = DeliveryResult(
                            success=False,
                            alert_id=alert_id,
                            recipients=all_recipients,
                            smtp_response=f"Partial delivery — rejected: {rejected}",
                            attempts=attempt,
                            sent_at=datetime.now().isoformat(),
                        )
                    else:
                        # Tous acceptés — Code 250
                        logger.info(
                            "SMTP OK [250] — E-mail délivré à %d destinataire(s)",
                            len(all_recipients),
                        )
                        result = DeliveryResult(
                            success=True,
                            alert_id=alert_id,
                            recipients=all_recipients,
                            smtp_response="250 OK — Accepted for delivery",
                            attempts=attempt,
                            sent_at=datetime.now().isoformat(),
                        )

                    self._log_delivery(result)
                    return result

            except smtplib.SMTPRecipientsRefused as exc:
                last_error = f"Tous les destinataires refusés : {exc}"
                logger.error("SMTP 5xx — %s", last_error)
                break  # Erreur permanente, pas de retry

            except smtplib.SMTPAuthenticationError as exc:
                last_error = f"Échec authentification SMTP : {exc}"
                logger.error("SMTP AUTH ERROR — %s", last_error)
                break  # Pas de retry sur auth

            except (smtplib.SMTPServerDisconnected, smtplib.SMTPConnectError,
                    smtplib.SMTPException, ConnectionError, TimeoutError,
                    socket.gaierror, OSError) as exc:
                last_error = f"Erreur SMTP temporaire : {exc}"
                logger.warning(
                    "SMTP ERROR [tentative %d/%d] — %s",
                    attempt, cfg.retry_max, exc,
                )
                if attempt < cfg.retry_max:
                    time.sleep(cfg.retry_delay_sec * attempt)  # Backoff

        # Tous les retries échoués
        result = DeliveryResult(
            success=False,
            alert_id=alert_id,
            recipients=all_recipients,
            error=last_error,
            attempts=cfg.retry_max,
            sent_at=datetime.now().isoformat(),
        )
        self._log_delivery(result)
        logger.error(
            "SMTP ÉCHEC FINAL [%d tentatives] — Alert %s non délivrée : %s",
            cfg.retry_max, alert_id, last_error,
        )
        return result

    def test_connection(self) -> bool:
        """Teste la connexion SMTP sans envoyer de mail."""
        cfg = self.config
        try:
            logger.info("TEST SMTP → %s:%d", cfg.smtp_host, cfg.smtp_port)
            with smtplib.SMTP(cfg.smtp_host, cfg.smtp_port, timeout=15) as server:
                server.ehlo()
                if cfg.smtp_use_tls:
                    server.starttls()
                    server.ehlo()
                if cfg.smtp_user and cfg.smtp_password:
                    server.login(cfg.smtp_user, cfg.smtp_password)

                # VRFY pour vérifier les destinataires (si supporté)
                for recipient in cfg.recipients:
                    code, message = server.vrfy(recipient)
                    logger.info(
                        "  VRFY %s → %d %s",
                        recipient, code, message.decode("utf-8", errors="replace"),
                    )

                logger.info("TEST SMTP — Connexion OK ✓")
                return True

        except Exception as exc:
            logger.error("TEST SMTP — Échec : %s", exc)
            return False

    def _log_delivery(self, result: DeliveryResult):
        """Log chaque tentative d'envoi dans un fichier JSONL."""
        try:
            with open(self._log_file, "a", encoding="utf-8") as f:
                f.write(json.dumps(result.to_dict(), ensure_ascii=False) + "\n")
        except Exception as exc:
            logger.error("Erreur log delivery : %s", exc)


# ===========================================================================
#  MÉTRIQUES
# ===========================================================================

class NotificationMetrics:
    """Compteurs pour le rapport final."""

    def __init__(self):
        self.started_at: Optional[datetime] = None
        self.messages_received = 0
        self.alerts_sent = 0
        self.alerts_failed = 0
        self.by_severity: dict[str, int] = {}
        self.by_layer: dict[str, int] = {}
        self.by_rule: dict[str, int] = {}

    def print_report(self):
        elapsed = (datetime.now() - self.started_at).total_seconds() if self.started_at else 0
        print()
        print("=" * 65)
        print("  RAPPORT FINAL — NOTIFICATION CONSUMER")
        print("=" * 65)
        print(f"  Durée              : {elapsed:.1f} sec")
        print(f"  Messages reçus     : {self.messages_received}")
        print(f"  Alertes envoyées   : {self.alerts_sent}")
        print(f"  Alertes échouées   : {self.alerts_failed}")
        print()
        if self.by_severity:
            print("  ── PAR SÉVÉRITÉ ──")
            for sev, cnt in sorted(self.by_severity.items()):
                print(f"    {sev:<12s} : {cnt}")
        if self.by_layer:
            print("  ── PAR COUCHE ──")
            for layer, cnt in sorted(self.by_layer.items()):
                print(f"    {layer:<18s} : {cnt}")
        if self.by_rule:
            print("  ── PAR RÈGLE ──")
            for rule, cnt in sorted(self.by_rule.items(), key=lambda x: -x[1]):
                print(f"    {rule:<30s} : {cnt}")
        print("=" * 65)


# ===========================================================================
#  BOUCLE PRINCIPALE DU CONSUMER
# ===========================================================================

def run_notification_consumer(config: NotificationConfig):
    """
    Consumer Kafka principal :
    1. Écoute quarantine_transactions
    2. Désérialise le JSON
    3. Classifie la sévérité
    4. Compose le mail HTML
    5. Envoie via SMTP avec vérification
    6. Log le résultat
    """
    consumer = Consumer(config.consumer_config)
    sender = EmailSender(config)
    metrics = NotificationMetrics()
    metrics.started_at = datetime.now()

    consumer.subscribe([config.topic])

    logger.info("=" * 65)
    logger.info("  NOTIFICATION CONSUMER — AL BARID BANK")
    logger.info("=" * 65)
    logger.info("  Bootstrap      : %s", config.bootstrap)
    logger.info("  Group ID       : %s", config.group_id)
    logger.info("  Topic IN       : %s", config.topic)
    logger.info("  Destinataires  : %s", ", ".join(config.recipients))
    if config.cc_recipients:
        logger.info("  CC             : %s", ", ".join(config.cc_recipients))
    logger.info("  SMTP           : %s:%d (TLS=%s)", config.smtp_host, config.smtp_port, config.smtp_use_tls)
    logger.info("  Dry Run        : %s", "OUI" if config.dry_run else "NON")
    logger.info("  Log dir        : %s", config.log_dir)
    logger.info("=" * 65)
    logger.info("En attente de transactions rejetées sur [%s]...", config.topic)

    # Arrêt propre
    running = True

    def on_signal(sig, frame):
        nonlocal running
        running = False
        logger.info("Signal d'arrêt reçu — fermeture propre...")

    signal.signal(signal.SIGINT, on_signal)
    signal.signal(signal.SIGTERM, on_signal)

    try:
        while running:
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                raise KafkaException(msg.error())

            # ── 1. Désérialisation ──
            metrics.messages_received += 1
            try:
                raw_value = msg.value().decode("utf-8")
                transaction = json.loads(raw_value)
            except (json.JSONDecodeError, UnicodeDecodeError) as exc:
                logger.error(
                    "ERREUR DÉSÉRIALISATION [#%d] : %s",
                    metrics.messages_received, exc,
                )
                continue

            # ── 2. Classification sévérité ──
            validation = transaction.get("_validation", {})
            errors = validation.get("errors", [])
            layer_failed = validation.get("layer_failed", "UNKNOWN")
            severity = classify_severity(errors)

            # Métriques
            metrics.by_severity[severity.value] = metrics.by_severity.get(severity.value, 0) + 1
            metrics.by_layer[layer_failed] = metrics.by_layer.get(layer_failed, 0) + 1
            for err in errors:
                rule = err.get("rule", "UNKNOWN")
                metrics.by_rule[rule] = metrics.by_rule.get(rule, 0) + 1

            # ── 3. Composition du mail ──
            alert_id = f"ALB-{datetime.now().strftime('%Y%m%d%H%M%S')}-{uuid.uuid4().hex[:8].upper()}"
            subject, html_body, text_body = compose_alert_email(
                transaction, severity, alert_id,
            )

            # ── 4. Log console ──
            pan_display = mask_pan(transaction.get("pan"))
            logger.info(
                "ALERTE [#%d] %s %s | %s | PAN %s | Couche %s | %d erreur(s)",
                metrics.messages_received,
                severity.emoji, severity.value,
                alert_id, pan_display, layer_failed,
                len(errors),
            )

            # ── 5. Envoi SMTP ──
            result = sender.send_alert(subject, html_body, text_body, alert_id)

            if result.success:
                metrics.alerts_sent += 1
            else:
                metrics.alerts_failed += 1

    finally:
        consumer.close()
        metrics.print_report()
        logger.info("Notification consumer arrêté proprement.")


# ===========================================================================
#  MODE TEST LOCAL (sans Kafka)
# ===========================================================================

def run_local_test(config: NotificationConfig):
    """
    Simule la réception d'une transaction rejetée (sans Kafka).
    Utile pour tester le rendu du mail et la connexion SMTP.
    """
    logger.info("=" * 65)
    logger.info("  MODE TEST LOCAL — Simulation de notification")
    logger.info("=" * 65)

    # Transaction rejetée simulée
    fake_transaction = {
        "mti": "0200",
        "pan": "4147331234567890",
        "processing_code": "010000",
        "amount_transaction": "000000010000",
        "transmission_datetime": "0413130000",
        "stan": "000001",
        "time_local": "130000",
        "date_local": "0413",
        "date_expiration": "2312",
        "mcc": "6011",
        "terminal_id": "GAB10042",
        "merchant_id": "BARIDBANK000042",
        "card_acceptor_name_location": "GAB BARID BANK MEDINA      RABAT         MA",
        "currency_code_transaction": "504",
        "pos_entry_mode": "051",
        "_quarantined_at": datetime.now().isoformat(),
        "_validation": {
            "is_valid": False,
            "validated_at": datetime.now().isoformat(),
            "layers_passed": ["STRUCTURAL"],
            "layer_failed": "BUSINESS",
            "error_count": 1,
            "warning_count": 1,
            "errors": [
                {
                    "layer": "BUSINESS",
                    "severity": "ERROR",
                    "field": "pan",
                    "rule": "LUHN_CHECK",
                    "message": "Le PAN échoue au test de Luhn (check digit invalide). Possibilité de PAN corrompu ou forgé.",
                    "value": "414733****7890",
                }
            ],
            "warnings": [
                {
                    "layer": "ML_QUALITY",
                    "severity": "WARNING",
                    "field": "pan",
                    "rule": "BIN_UNKNOWN",
                    "message": "BIN '414733' non référencé chez Al Barid Bank. Transaction possiblement inter-bancaire.",
                    "value": "414733",
                }
            ],
        },
    }

    severity = classify_severity(fake_transaction["_validation"]["errors"])
    alert_id = f"ALB-TEST-{uuid.uuid4().hex[:8].upper()}"

    subject, html_body, text_body = compose_alert_email(
        fake_transaction, severity, alert_id,
    )

    logger.info("Sévérité classifiée : %s %s", severity.emoji, severity.value)
    logger.info("Sujet : %s", subject)

    sender = EmailSender(config)

    if config.dry_run:
        # En dry-run, on affiche juste le mail
        sender.send_alert(subject, html_body, text_body, alert_id)

        # Sauvegarder le HTML pour prévisualisation
        html_path = config.log_dir / "test_alert_preview.html"
        with open(html_path, "w", encoding="utf-8") as f:
            f.write(html_body)
        logger.info("Prévisualisation HTML sauvegardée : %s", html_path)
    else:
        result = sender.send_alert(subject, html_body, text_body, alert_id)
        if result.success:
            logger.info("TEST OK — E-mail envoyé avec succès !")
        else:
            logger.error("TEST ÉCHOUÉ — %s", result.error)


# ===========================================================================
#  CLI
# ===========================================================================

if __name__ == "__main__":
    ap = argparse.ArgumentParser(
        description="Notification Consumer — Alertes Transactions Rejetées (Al Barid Bank)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Exemples :
  python kafka_notification.py                            # Mode Kafka (production)
  python kafka_notification.py --dry-run                  # Affiche sans envoyer
  python kafka_notification.py --test-smtp                # Teste la connexion SMTP
  python kafka_notification.py --local-test --dry-run     # Test local + preview
  python kafka_notification.py --bootstrap 10.0.0.5:9092  # Kafka distant
  python kafka_notification.py --recipients a@b.com c@d.com
        """,
    )

    # Kafka
    ap.add_argument("--bootstrap", default="127.0.0.1:9092", help="Kafka bootstrap servers")
    ap.add_argument("--group-id", default="notification-alert-group", help="Consumer group ID")
    ap.add_argument("--topic", default="quarantine_transactions", help="Topic Kafka à écouter")

    # SMTP
    ap.add_argument("--smtp-host", default="smtp.gmail.com", help="Serveur SMTP (default: smtp.gmail.com)")
    ap.add_argument("--smtp-port", type=int, default=587, help="Port SMTP (default: 587)")
    ap.add_argument("--smtp-user", default="", help="Utilisateur SMTP (ou env SMTP_USER)")
    ap.add_argument("--smtp-password", default="", help="Mot de passe SMTP (ou env SMTP_PASSWORD)")

    # Destinataires
    ap.add_argument("--recipients", nargs="+", default=None, help="Adresses e-mail destinataires")
    ap.add_argument("--cc", nargs="+", default=None, help="Adresses en copie (CC)")

    # Modes
    ap.add_argument("--dry-run", action="store_true", help="Affiche le mail sans envoyer")
    ap.add_argument("--test-smtp", action="store_true", help="Teste la connexion SMTP et quitte")
    ap.add_argument("--local-test", action="store_true", help="Simule une transaction rejetée")
    ap.add_argument("--log-dir", default="./data/notification_logs", help="Répertoire des logs")

    args = ap.parse_args()

    cfg = NotificationConfig(
        bootstrap=args.bootstrap,
        group_id=args.group_id,
        topic=args.topic,
        smtp_host=args.smtp_host,
        smtp_port=args.smtp_port,
        smtp_user=args.smtp_user,
        smtp_password=args.smtp_password,
        recipients=args.recipients,
        cc_recipients=args.cc,
        dry_run=args.dry_run,
        log_dir=args.log_dir,
    )

    if args.test_smtp:
        success = EmailSender(cfg).test_connection()
        exit(0 if success else 1)
    elif args.local_test:
        run_local_test(cfg)
    else:
        run_notification_consumer(cfg)
