"""
===========================================================================
Module : transaction_validator.py
Projet : Plateforme MLOps - Détection de Fraude (Al Barid Bank)
Auteur : Hamza (PFE EMSI 2025-2026)
---------------------------------------------------------------------------
Rôle   : Validateur Quality Check — 3 couches de validation sur chaque
         trame parsée (JSON dict) issue du Smart Parser ISO 8583.

         Couche 1 — Conformité Structurelle (Protocole ISO 8583)
         Couche 2 — Validité Monétique (Règles métier Al Barid Bank)
         Couche 3 — Intégrité ML (Data Quality pour le Feature Engineering)

         Sortie :
           ✅ CLEAN  → Curated Zone (Parquet) — prêt pour Spark / ML
           ❌ REJECT → Quarantine Zone (HDFS raw) + alerte système

         Le validateur agit **par trame** (streaming, pas de batch).
         Il est conçu pour s'intégrer dans le Consumer Kafka existant.

Usage  :
    from transaction_validator import TransactionValidator

    validator = TransactionValidator()
    result = validator.validate(parsed_dict)

    if result.is_valid:
        # → Curated Zone
    else:
        # → Quarantine Zone + Alert
        print(result.errors)

Requires : (stdlib uniquement — aucune dépendance externe)
===========================================================================
"""

import logging
import re
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Optional

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(name)s | %(message)s",
)
logger = logging.getLogger("transaction_validator")


# ===========================================================================
#  DATA CLASSES & ENUMS
# ===========================================================================

class ValidationLayer(Enum):
    """Identifie la couche de validation qui a rejeté la trame."""
    STRUCTURAL = "STRUCTURAL"   # Couche 1 : Conformité protocole
    BUSINESS   = "BUSINESS"     # Couche 2 : Validité monétique
    ML_QUALITY = "ML_QUALITY"   # Couche 3 : Intégrité ML


class Severity(Enum):
    """Gravité de l'erreur de validation."""
    ERROR   = "ERROR"    # Bloquant — trame rejetée
    WARNING = "WARNING"  # Non-bloquant — trame acceptée avec flag


@dataclass
class ValidationError:
    """Représente une erreur de validation individuelle."""
    layer: ValidationLayer
    severity: Severity
    field: str
    rule: str
    message: str
    value: Optional[str] = None

    def to_dict(self) -> dict:
        return {
            "layer": self.layer.value,
            "severity": self.severity.value,
            "field": self.field,
            "rule": self.rule,
            "message": self.message,
            "value": self.value,
        }


@dataclass
class ValidationResult:
    """Résultat complet de la validation d'une trame."""
    is_valid: bool = True
    errors: list[ValidationError] = field(default_factory=list)
    warnings: list[ValidationError] = field(default_factory=list)
    validated_at: str = ""
    layers_passed: list[str] = field(default_factory=list)
    layer_failed: Optional[str] = None

    def add_error(self, error: ValidationError):
        """Ajoute une erreur et marque la trame comme invalide."""
        if error.severity == Severity.ERROR:
            self.errors.append(error)
            self.is_valid = False
        else:
            self.warnings.append(error)

    def to_dict(self) -> dict:
        return {
            "_validation": {
                "is_valid": self.is_valid,
                "validated_at": self.validated_at,
                "layers_passed": self.layers_passed,
                "layer_failed": self.layer_failed,
                "error_count": len(self.errors),
                "warning_count": len(self.warnings),
                "errors": [e.to_dict() for e in self.errors],
                "warnings": [w.to_dict() for w in self.warnings],
            }
        }


# ===========================================================================
#  CONSTANTES DE VALIDATION
# ===========================================================================

# ── MTI supportés par Al Barid Bank ──
SUPPORTED_MTI = {
    # ISO 8583:1987 (spec87)
    "0100", "0110",  # Authorization request / response
    "0200", "0210",  # Financial transaction request / response
    "0220", "0230",  # Financial transaction advice / response
    "0400", "0410",  # Reversal request / response
    "0420", "0430",  # Reversal advice / response
    "0800", "0810",  # Network management request / response
    # ISO 8583:1993 (spec93)
    "1100", "1110",
    "1200", "1210",
    "1220", "1230",
    "1400", "1410",
    "1420", "1430",
    "1800", "1810",
    # ISO 8583:2003 (spec03)
    "2100", "2110",
    "2200", "2210",
    "2220", "2230",
    "2400", "2410",
    "2420", "2430",
    "2800", "2810",
}

# ── Codes devise ISO 4217 valides ──
VALID_CURRENCY_CODES = {
    "504",  # MAD — Dirham Marocain (principal)
    "978",  # EUR — Euro
    "840",  # USD — Dollar US
    "826",  # GBP — Livre Sterling
    "682",  # SAR — Riyal Saoudien
    "784",  # AED — Dirham Emirati
    "012",  # DZD — Dinar Algérien
    "788",  # TND — Dinar Tunisien
    "952",  # XOF — Franc CFA BCEAO
}

# ── Codes MCC (Merchant Category Code) valides ──
# Plage ISO standard : 0001-9999
MCC_RANGE = (1, 9999)

# ── Processing codes valides ──
VALID_PROCESSING_CODES_PREFIX = {
    "00",  # Purchase / Goods and services
    "01",  # Cash withdrawal (ATM)
    "02",  # Debit adjustment
    "09",  # Purchase with cashback
    "20",  # Refund / Return
    "30",  # Balance inquiry
    "31",  # Balance inquiry (savings)
    "40",  # Transfer (card-to-card)
}

# ── Champs numériques stricts (N) ──
NUMERIC_FIELDS = {
    "pan": "DE 2 - PAN",
    "processing_code": "DE 3 - Processing Code",
    "amount_transaction": "DE 4 - Amount",
    "transmission_datetime": "DE 7 - Transmission DateTime",
    "stan": "DE 11 - STAN",
    "time_local": "DE 12 - Time Local",
    "date_local": "DE 13 - Date Local",
    "date_expiration": "DE 14 - Date Expiration",
    "mcc": "DE 18 - MCC",
    "currency_code_transaction": "DE 49 - Currency Code",
}

# ── Longueurs fixes attendues ──
FIXED_LENGTH_FIELDS = {
    "mti": 4,
    "processing_code": 6,
    "amount_transaction": 12,
    "transmission_datetime": 10,
    "stan": 6,
    "time_local": 6,
    "date_local": 4,
    "date_expiration": 4,
    "currency_code_transaction": 3,
}

# ── Champs critiques pour le ML ──
ML_CRITICAL_FIELDS = {
    "pan":                          "Velocity check, card profiling",
    "amount_transaction":           "Z-Score, amount anomaly",
    "transmission_datetime":        "Geo-velocity, time-based features",
    "card_acceptor_name_location":  "Geo-velocity, impossible travel",
    "terminal_id":                  "Terminal profiling, card testing",
    "mcc":                          "Time/MCC coherence, merchant profiling",
    "processing_code":              "Transaction type classification",
    "currency_code_transaction":    "Cross-border detection",
}

# ── Montant max raisonnable (en centimes) — 500 000 MAD ──
MAX_AMOUNT_CENTIMES = 50_000_000

# ── Tolérance horodatage (heures) ──
TIMESTAMP_FUTURE_TOLERANCE_HOURS = 1
TIMESTAMP_PAST_TOLERANCE_HOURS = 72

# ── Regex pour caractères autorisés dans DE 43 ──
# AN (Alphanumeric) + espaces, tirets, points, slashs
DE43_PATTERN = re.compile(r"^[A-Za-z0-9 \-\.\/&,']+$")

# ── PAN : longueur entre 13 et 19 digits (standard ISO/IEC 7812) ──
PAN_MIN_LENGTH = 13
PAN_MAX_LENGTH = 19

# ── BIN (6 premiers digits du PAN) — Préfixes Al Barid Bank connus ──
# Adapte cette liste selon les BINs réels de la banque
KNOWN_BIN_PREFIXES = {
    "414733",  # BIN simulé Al Barid Bank (d'après tes trames)
}


# ===========================================================================
#  COUCHE 1 — CONFORMITÉ STRUCTURELLE (PROTOCOLE)
# ===========================================================================

class StructuralValidator:
    """
    Vérifie que la trame parsée respecte la grammaire ISO 8583.

    Règles :
      1.1  MTI présent et format correct (4 digits)
      1.2  Champs numériques (N) — digits uniquement
      1.3  Champs à longueur fixe — taille exacte
      1.4  DE 43 (AN) — jeu de caractères autorisé
      1.5  PAN — longueur entre 13 et 19 digits
      1.6  Processing Code — 6 digits, préfixe reconnu
      1.7  Bitmap cohérence — champs attendus vs présents
    """

    def validate(self, data: dict, result: ValidationResult) -> None:
        """Exécute toutes les règles structurelles."""
        self._check_mti_format(data, result)
        self._check_numeric_fields(data, result)
        self._check_fixed_lengths(data, result)
        self._check_de43_charset(data, result)
        self._check_pan_length(data, result)
        self._check_processing_code_format(data, result)

    # ── 1.1 MTI Format ──
    def _check_mti_format(self, data: dict, result: ValidationResult) -> None:
        mti = data.get("mti")
        if not mti:
            result.add_error(ValidationError(
                layer=ValidationLayer.STRUCTURAL,
                severity=Severity.ERROR,
                field="mti",
                rule="MTI_PRESENT",
                message="Le champ MTI est absent ou vide.",
            ))
            return

        if not isinstance(mti, str) or len(mti) != 4 or not mti.isdigit():
            result.add_error(ValidationError(
                layer=ValidationLayer.STRUCTURAL,
                severity=Severity.ERROR,
                field="mti",
                rule="MTI_FORMAT",
                message=f"MTI invalide : attendu 4 digits, reçu '{mti}'.",
                value=str(mti),
            ))

    # ── 1.2 Champs Numériques (N) ──
    def _check_numeric_fields(self, data: dict, result: ValidationResult) -> None:
        for field_key, field_label in NUMERIC_FIELDS.items():
            value = data.get(field_key)
            if value is None:
                continue  # Absent = OK (vérifié en couche 3 si critique)

            if not isinstance(value, str) or not value.isdigit():
                result.add_error(ValidationError(
                    layer=ValidationLayer.STRUCTURAL,
                    severity=Severity.ERROR,
                    field=field_key,
                    rule="NUMERIC_ONLY",
                    message=(
                        f"{field_label} doit être numérique. "
                        f"Reçu : '{value}'"
                    ),
                    value=str(value),
                ))

    # ── 1.3 Longueurs Fixes ──
    def _check_fixed_lengths(self, data: dict, result: ValidationResult) -> None:
        for field_key, expected_len in FIXED_LENGTH_FIELDS.items():
            value = data.get(field_key)
            if value is None:
                continue

            if not isinstance(value, str):
                continue  # Déjà attrapé par le check numérique

            if len(value) != expected_len:
                result.add_error(ValidationError(
                    layer=ValidationLayer.STRUCTURAL,
                    severity=Severity.ERROR,
                    field=field_key,
                    rule="FIXED_LENGTH",
                    message=(
                        f"Longueur incorrecte pour {field_key} : "
                        f"attendu {expected_len}, reçu {len(value)}."
                    ),
                    value=value,
                ))

    # ── 1.4 DE 43 — Charset AN ──
    def _check_de43_charset(self, data: dict, result: ValidationResult) -> None:
        de43 = data.get("card_acceptor_name_location")
        if de43 is None:
            return

        if not isinstance(de43, str):
            result.add_error(ValidationError(
                layer=ValidationLayer.STRUCTURAL,
                severity=Severity.ERROR,
                field="card_acceptor_name_location",
                rule="DE43_CHARSET",
                message="DE 43 doit être une chaîne alphanumerique.",
                value=str(de43),
            ))
            return

        if not DE43_PATTERN.match(de43):
            # Identifier les caractères invalides
            invalid_chars = set()
            for ch in de43:
                if not DE43_PATTERN.match(ch):
                    invalid_chars.add(repr(ch))
            result.add_error(ValidationError(
                layer=ValidationLayer.STRUCTURAL,
                severity=Severity.ERROR,
                field="card_acceptor_name_location",
                rule="DE43_CHARSET",
                message=(
                    f"DE 43 contient des caractères non autorisés : "
                    f"{', '.join(invalid_chars)}"
                ),
                value=de43[:50],
            ))

    # ── 1.5 PAN — Longueur ──
    def _check_pan_length(self, data: dict, result: ValidationResult) -> None:
        pan = data.get("pan")
        if pan is None:
            return

        if not isinstance(pan, str) or not pan.isdigit():
            return  # Déjà couvert par check numérique

        if not (PAN_MIN_LENGTH <= len(pan) <= PAN_MAX_LENGTH):
            result.add_error(ValidationError(
                layer=ValidationLayer.STRUCTURAL,
                severity=Severity.ERROR,
                field="pan",
                rule="PAN_LENGTH",
                message=(
                    f"Longueur PAN hors norme ISO 7812 : "
                    f"attendu {PAN_MIN_LENGTH}-{PAN_MAX_LENGTH}, "
                    f"reçu {len(pan)}."
                ),
                value=f"{pan[:4]}****",
            ))

    # ── 1.6 Processing Code Format ──
    def _check_processing_code_format(self, data: dict, result: ValidationResult) -> None:
        pc = data.get("processing_code")
        if pc is None:
            return
        if not isinstance(pc, str) or len(pc) != 6:
            return  # Déjà couvert

        prefix = pc[:2]
        if prefix not in VALID_PROCESSING_CODES_PREFIX:
            result.add_error(ValidationError(
                layer=ValidationLayer.STRUCTURAL,
                severity=Severity.WARNING,
                field="processing_code",
                rule="PROCESSING_CODE_PREFIX",
                message=(
                    f"Processing code prefix '{prefix}' non reconnu. "
                    f"Préfixes valides : {sorted(VALID_PROCESSING_CODES_PREFIX)}"
                ),
                value=pc,
            ))


# ===========================================================================
#  COUCHE 2 — VALIDITÉ MONÉTIQUE (MÉTIER)
# ===========================================================================

class BusinessValidator:
    """
    Vérifie que la transaction a un sens logique pour Al Barid Bank.

    Règles :
      2.1  Algorithme de Luhn sur le PAN
      2.2  Code devise (DE 49) valide ISO 4217
      2.3  MTI supporté par le système
      2.4  MCC dans la plage ISO valide
      2.5  Montant positif et non aberrant (≤ 500 000 MAD)
      2.6  Date d'expiration carte non dépassée
      2.7  Cohérence Processing Code / MTI
    """

    def validate(self, data: dict, result: ValidationResult) -> None:
        """Exécute toutes les règles métier."""
        self._check_luhn(data, result)
        self._check_currency_code(data, result)
        self._check_mti_supported(data, result)
        self._check_mcc_range(data, result)
        self._check_amount_range(data, result)
        self._check_expiration_date(data, result)
        self._check_processing_code_mti_coherence(data, result)

    # ── 2.1 Algorithme de Luhn ──
    @staticmethod
    def _luhn_check(pan: str) -> bool:
        """
        Implémente l'algorithme de Luhn (ISO/IEC 7812-1).
        Retourne True si le PAN est valide.
        """
        if not pan.isdigit():
            return False

        digits = [int(d) for d in pan]
        # On parcourt de droite à gauche
        checksum = 0
        for i, d in enumerate(reversed(digits)):
            if i % 2 == 1:  # Position paire (en partant de la droite)
                d *= 2
                if d > 9:
                    d -= 9
            checksum += d
        return checksum % 10 == 0

    def _check_luhn(self, data: dict, result: ValidationResult) -> None:
        pan = data.get("pan")
        if pan is None or not isinstance(pan, str) or not pan.isdigit():
            return  # Déjà couvert en couche 1

        if not self._luhn_check(pan):
            result.add_error(ValidationError(
                layer=ValidationLayer.BUSINESS,
                severity=Severity.ERROR,
                field="pan",
                rule="LUHN_CHECK",
                message=(
                    "Le PAN échoue au test de Luhn (check digit invalide). "
                    "Possibilité de PAN corrompu ou forgé."
                ),
                value=f"{pan[:6]}****{pan[-4:]}",
            ))

    # ── 2.2 Code Devise ──
    def _check_currency_code(self, data: dict, result: ValidationResult) -> None:
        currency = data.get("currency_code_transaction")
        if currency is None:
            return

        if currency not in VALID_CURRENCY_CODES:
            result.add_error(ValidationError(
                layer=ValidationLayer.BUSINESS,
                severity=Severity.ERROR,
                field="currency_code_transaction",
                rule="CURRENCY_VALID",
                message=(
                    f"Code devise '{currency}' non reconnu ISO 4217. "
                    f"Devises autorisées : {sorted(VALID_CURRENCY_CODES)}"
                ),
                value=currency,
            ))

    # ── 2.3 MTI Supporté ──
    def _check_mti_supported(self, data: dict, result: ValidationResult) -> None:
        mti = data.get("mti")
        if mti is None:
            return  # Déjà couvert en couche 1

        if mti not in SUPPORTED_MTI:
            result.add_error(ValidationError(
                layer=ValidationLayer.BUSINESS,
                severity=Severity.ERROR,
                field="mti",
                rule="MTI_SUPPORTED",
                message=(
                    f"MTI '{mti}' non supporté par le système Al Barid Bank."
                ),
                value=mti,
            ))

    # ── 2.4 MCC Range ──
    def _check_mcc_range(self, data: dict, result: ValidationResult) -> None:
        mcc = data.get("mcc")
        if mcc is None:
            return

        if not isinstance(mcc, str) or not mcc.isdigit():
            return  # Couvert en couche 1

        mcc_int = int(mcc)
        if not (MCC_RANGE[0] <= mcc_int <= MCC_RANGE[1]):
            result.add_error(ValidationError(
                layer=ValidationLayer.BUSINESS,
                severity=Severity.ERROR,
                field="mcc",
                rule="MCC_RANGE",
                message=(
                    f"MCC '{mcc}' hors plage ISO ({MCC_RANGE[0]}-{MCC_RANGE[1]})."
                ),
                value=mcc,
            ))

    # ── 2.5 Montant Range ──
    def _check_amount_range(self, data: dict, result: ValidationResult) -> None:
        amount = data.get("amount_transaction")
        if amount is None:
            return

        if not isinstance(amount, str) or not amount.isdigit():
            return

        amount_int = int(amount)

        # Montant zéro (sauf balance inquiry)
        proc_code = data.get("processing_code", "")
        is_inquiry = proc_code[:2] in ("30", "31")

        if amount_int == 0 and not is_inquiry:
            result.add_error(ValidationError(
                layer=ValidationLayer.BUSINESS,
                severity=Severity.ERROR,
                field="amount_transaction",
                rule="AMOUNT_ZERO",
                message=(
                    "Montant = 0 pour une transaction non-inquiry. "
                    "Seules les consultations de solde (30/31) acceptent un montant nul."
                ),
                value=amount,
            ))

        # Montant aberrant (> 500 000 MAD)
        if amount_int > MAX_AMOUNT_CENTIMES:
            result.add_error(ValidationError(
                layer=ValidationLayer.BUSINESS,
                severity=Severity.ERROR,
                field="amount_transaction",
                rule="AMOUNT_MAX",
                message=(
                    f"Montant aberrant : {amount_int / 100:.2f} MAD "
                    f"dépasse le plafond ({MAX_AMOUNT_CENTIMES / 100:.2f} MAD)."
                ),
                value=amount,
            ))

    # ── 2.6 Date Expiration Carte ──
    def _check_expiration_date(self, data: dict, result: ValidationResult) -> None:
        exp = data.get("date_expiration")
        if exp is None:
            return

        if not isinstance(exp, str) or len(exp) != 4 or not exp.isdigit():
            return  # Format YYMM

        try:
            yy = int(exp[:2])
            mm = int(exp[2:4])

            if mm < 1 or mm > 12:
                result.add_error(ValidationError(
                    layer=ValidationLayer.BUSINESS,
                    severity=Severity.ERROR,
                    field="date_expiration",
                    rule="EXPIRY_MONTH",
                    message=f"Mois d'expiration invalide : {mm}.",
                    value=exp,
                ))
                return

            # Convertir YYMM → date de fin de mois
            year = 2000 + yy
            # Dernier jour du mois d'expiration
            if mm == 12:
                exp_date = datetime(year + 1, 1, 1) - timedelta(days=1)
            else:
                exp_date = datetime(year, mm + 1, 1) - timedelta(days=1)

            now = datetime.now()
            if exp_date < now:
                result.add_error(ValidationError(
                    layer=ValidationLayer.BUSINESS,
                    severity=Severity.ERROR,
                    field="date_expiration",
                    rule="CARD_EXPIRED",
                    message=(
                        f"Carte expirée : {mm:02d}/{year}. "
                        f"Transaction sur carte périmée."
                    ),
                    value=exp,
                ))

        except (ValueError, OverflowError):
            result.add_error(ValidationError(
                layer=ValidationLayer.BUSINESS,
                severity=Severity.ERROR,
                field="date_expiration",
                rule="EXPIRY_PARSE",
                message=f"Impossible de parser la date d'expiration '{exp}'.",
                value=exp,
            ))

    # ── 2.7 Cohérence Processing Code / MTI ──
    def _check_processing_code_mti_coherence(
        self, data: dict, result: ValidationResult
    ) -> None:
        """
        Vérifie que le type de transaction (Processing Code)
        est cohérent avec la classe du message (MTI).

        Exemples d'incohérence :
        - Balance Inquiry (30xx) sur un MTI 0200 (financial transaction)
          → devrait être 0100 (authorization)
        - Reversal (MTI 04xx) avec processing code Purchase (00)
          est acceptable (on reverse un achat)
        """
        mti = data.get("mti")
        pc = data.get("processing_code")
        if not mti or not pc or len(mti) < 4 or len(pc) < 2:
            return

        mti_class = mti[1]  # 2e digit = message class
        pc_prefix = pc[:2]

        # Network management (0800/1800/2800) ne devrait pas avoir
        # de processing code financier
        if mti_class == "8" and pc_prefix in ("00", "01", "09", "20"):
            result.add_error(ValidationError(
                layer=ValidationLayer.BUSINESS,
                severity=Severity.WARNING,
                field="processing_code",
                rule="PC_MTI_COHERENCE",
                message=(
                    f"Processing code financier ({pc_prefix}) "
                    f"incohérent avec MTI Network Management ({mti})."
                ),
                value=f"MTI={mti}, PC={pc}",
            ))


# ===========================================================================
#  COUCHE 3 — INTÉGRITÉ ML (DATA QUALITY)
# ===========================================================================

class MLQualityValidator:
    """
    Vérifie que la trame contient les données nécessaires au Feature
    Engineering et au scoring ML. Garbage In, Garbage Out.

    Règles :
      3.1  Présence des features critiques pour les 3 types d'anomalies
      3.2  Horodatage cohérent (pas dans le futur, pas trop ancien)
      3.3  PAN masquable (assez long pour extraire le BIN)
      3.4  DE 43 parsable (ville + pays extractibles)
      3.5  Terminal ID non vide (profiling terminal)
      3.6  Montant convertible en numérique
      3.7  BIN connu (Warning si BIN inconnu — pas bloquant)
      3.8  Cohérence canal enrichi (GAB/TPE/ECOM) avec POS entry mode
    """

    def validate(self, data: dict, result: ValidationResult) -> None:
        """Exécute toutes les règles de qualité ML."""
        self._check_critical_fields(data, result)
        self._check_timestamp_coherence(data, result)
        self._check_pan_bin_extractable(data, result)
        self._check_de43_parsable(data, result)
        self._check_terminal_id(data, result)
        self._check_amount_numeric(data, result)
        self._check_known_bin(data, result)
        self._check_channel_pos_coherence(data, result)

    # ── 3.1 Features Critiques ──
    def _check_critical_fields(self, data: dict, result: ValidationResult) -> None:
        for field_key, usage in ML_CRITICAL_FIELDS.items():
            value = data.get(field_key)
            if value is None or (isinstance(value, str) and value.strip() == ""):
                result.add_error(ValidationError(
                    layer=ValidationLayer.ML_QUALITY,
                    severity=Severity.ERROR,
                    field=field_key,
                    rule="ML_FIELD_REQUIRED",
                    message=(
                        f"Champ critique absent pour le ML : {field_key}. "
                        f"Utilisé pour : {usage}."
                    ),
                ))

    # ── 3.2 Horodatage Cohérent ──
    def _check_timestamp_coherence(self, data: dict, result: ValidationResult) -> None:
        dt_str = data.get("transmission_datetime")
        if dt_str is None or not isinstance(dt_str, str) or len(dt_str) != 10:
            return  # Déjà couvert par le check critique

        try:
            # Format MMDDHHmmss — ISO 8583 DE 7
            month = int(dt_str[0:2])
            day = int(dt_str[2:4])
            hour = int(dt_str[4:6])
            minute = int(dt_str[6:8])
            second = int(dt_str[8:10])

            # Valider les composantes
            if not (1 <= month <= 12):
                result.add_error(ValidationError(
                    layer=ValidationLayer.ML_QUALITY,
                    severity=Severity.ERROR,
                    field="transmission_datetime",
                    rule="TIMESTAMP_MONTH",
                    message=f"Mois invalide dans DE 7 : {month}.",
                    value=dt_str,
                ))
                return

            if not (1 <= day <= 31):
                result.add_error(ValidationError(
                    layer=ValidationLayer.ML_QUALITY,
                    severity=Severity.ERROR,
                    field="transmission_datetime",
                    rule="TIMESTAMP_DAY",
                    message=f"Jour invalide dans DE 7 : {day}.",
                    value=dt_str,
                ))
                return

            if not (0 <= hour <= 23 and 0 <= minute <= 59 and 0 <= second <= 59):
                result.add_error(ValidationError(
                    layer=ValidationLayer.ML_QUALITY,
                    severity=Severity.ERROR,
                    field="transmission_datetime",
                    rule="TIMESTAMP_TIME",
                    message=f"Heure invalide dans DE 7 : {hour}:{minute}:{second}.",
                    value=dt_str,
                ))
                return

            # Reconstruire la datetime (année courante par défaut, DE 7 n'a pas d'année)
            now = datetime.now()
            try:
                tx_datetime = datetime(now.year, month, day, hour, minute, second)
            except ValueError:
                result.add_error(ValidationError(
                    layer=ValidationLayer.ML_QUALITY,
                    severity=Severity.ERROR,
                    field="transmission_datetime",
                    rule="TIMESTAMP_INVALID_DATE",
                    message=f"Date invalide : mois={month}, jour={day}.",
                    value=dt_str,
                ))
                return

            # Futur ?
            future_limit = now + timedelta(hours=TIMESTAMP_FUTURE_TOLERANCE_HOURS)
            if tx_datetime > future_limit:
                result.add_error(ValidationError(
                    layer=ValidationLayer.ML_QUALITY,
                    severity=Severity.ERROR,
                    field="transmission_datetime",
                    rule="TIMESTAMP_FUTURE",
                    message=(
                        f"Horodatage dans le futur : {tx_datetime.isoformat()} "
                        f"(tolérance : +{TIMESTAMP_FUTURE_TOLERANCE_HOURS}h)."
                    ),
                    value=dt_str,
                ))

            # Trop ancien ?
            past_limit = now - timedelta(hours=TIMESTAMP_PAST_TOLERANCE_HOURS)
            if tx_datetime < past_limit:
                result.add_error(ValidationError(
                    layer=ValidationLayer.ML_QUALITY,
                    severity=Severity.WARNING,
                    field="transmission_datetime",
                    rule="TIMESTAMP_STALE",
                    message=(
                        f"Horodatage trop ancien : {tx_datetime.isoformat()} "
                        f"(tolérance : -{TIMESTAMP_PAST_TOLERANCE_HOURS}h). "
                        f"Possible retard de transmission."
                    ),
                    value=dt_str,
                ))

        except (ValueError, TypeError) as exc:
            result.add_error(ValidationError(
                layer=ValidationLayer.ML_QUALITY,
                severity=Severity.ERROR,
                field="transmission_datetime",
                rule="TIMESTAMP_PARSE",
                message=f"Impossible de parser DE 7 : {exc}",
                value=dt_str,
            ))

    # ── 3.3 PAN — BIN Extractible ──
    def _check_pan_bin_extractable(self, data: dict, result: ValidationResult) -> None:
        pan = data.get("pan")
        if pan is None or not isinstance(pan, str):
            return

        if len(pan) < 6:
            result.add_error(ValidationError(
                layer=ValidationLayer.ML_QUALITY,
                severity=Severity.ERROR,
                field="pan",
                rule="PAN_BIN_EXTRACT",
                message=(
                    "PAN trop court pour extraire le BIN (6 premiers digits). "
                    f"Longueur reçue : {len(pan)}."
                ),
                value=f"{pan[:4]}...",
            ))

    # ── 3.4 DE 43 Parsable (ville + pays) ──
    def _check_de43_parsable(self, data: dict, result: ValidationResult) -> None:
        de43 = data.get("card_acceptor_name_location")
        if de43 is None:
            return  # Couvert par check critique

        if not isinstance(de43, str):
            return

        # Format standard : 40 caractères
        # Positions 26-38 = City, 39-40 = Country
        if len(de43) < 40:
            result.add_error(ValidationError(
                layer=ValidationLayer.ML_QUALITY,
                severity=Severity.WARNING,
                field="card_acceptor_name_location",
                rule="DE43_LENGTH",
                message=(
                    f"DE 43 trop court ({len(de43)} chars, attendu 40). "
                    f"Extraction ville/pays peut échouer."
                ),
                value=de43[:50],
            ))
            return

        country = de43[38:40].strip()
        city = de43[25:38].strip()

        if not country or len(country) != 2:
            result.add_error(ValidationError(
                layer=ValidationLayer.ML_QUALITY,
                severity=Severity.WARNING,
                field="card_acceptor_name_location",
                rule="DE43_COUNTRY",
                message=f"Code pays non extractible de DE 43 (positions 39-40).",
                value=de43[35:],
            ))

        if not city:
            result.add_error(ValidationError(
                layer=ValidationLayer.ML_QUALITY,
                severity=Severity.WARNING,
                field="card_acceptor_name_location",
                rule="DE43_CITY",
                message="Ville non extractible de DE 43 (positions 26-38).",
                value=de43[25:40],
            ))

    # ── 3.5 Terminal ID ──
    def _check_terminal_id(self, data: dict, result: ValidationResult) -> None:
        tid = data.get("terminal_id")
        if tid is None:
            return  # Couvert par check critique

        if isinstance(tid, str) and tid.strip() == "":
            result.add_error(ValidationError(
                layer=ValidationLayer.ML_QUALITY,
                severity=Severity.ERROR,
                field="terminal_id",
                rule="TERMINAL_ID_EMPTY",
                message="Terminal ID vide — impossible de profiler le terminal.",
            ))

    # ── 3.6 Montant Convertible ──
    def _check_amount_numeric(self, data: dict, result: ValidationResult) -> None:
        amount = data.get("amount_transaction")
        if amount is None:
            return

        try:
            val = int(amount)
            if val < 0:
                result.add_error(ValidationError(
                    layer=ValidationLayer.ML_QUALITY,
                    severity=Severity.ERROR,
                    field="amount_transaction",
                    rule="AMOUNT_NEGATIVE",
                    message=f"Montant négatif : {val}.",
                    value=amount,
                ))
        except (ValueError, TypeError):
            result.add_error(ValidationError(
                layer=ValidationLayer.ML_QUALITY,
                severity=Severity.ERROR,
                field="amount_transaction",
                rule="AMOUNT_NUMERIC",
                message=f"Montant non convertible en entier : '{amount}'.",
                value=str(amount),
            ))

    # ── 3.7 BIN Connu (Warning) ──
    def _check_known_bin(self, data: dict, result: ValidationResult) -> None:
        pan = data.get("pan")
        if pan is None or not isinstance(pan, str) or len(pan) < 6:
            return

        bin_prefix = pan[:6]
        if bin_prefix not in KNOWN_BIN_PREFIXES:
            result.add_error(ValidationError(
                layer=ValidationLayer.ML_QUALITY,
                severity=Severity.WARNING,
                field="pan",
                rule="BIN_UNKNOWN",
                message=(
                    f"BIN '{bin_prefix}' non référencé chez Al Barid Bank. "
                    f"Transaction possiblement inter-bancaire ou étrangère."
                ),
                value=bin_prefix,
            ))

    # ── 3.8 Cohérence Canal / POS Entry Mode ──
    def _check_channel_pos_coherence(
        self, data: dict, result: ValidationResult
    ) -> None:
        """
        Vérifie la cohérence entre le canal déduit (GAB/TPE/ECOM)
        et le POS Entry Mode (DE 22).

        DE 22 valeurs courantes :
          - 051 = Chip (contact)          → GAB / TPE
          - 071 = Contactless chip        → TPE
          - 021 = Magnetic stripe         → GAB / TPE
          - 812 = E-commerce (CNP)        → ECOM
          - 010 = Manual key entry        → ECOM / TPE (fallback)
        """
        terminal_id = data.get("terminal_id", "")
        pos_entry = data.get("pos_entry_mode", "")

        if not terminal_id or not pos_entry:
            return

        # Déterminer le canal
        if terminal_id.startswith("GAB"):
            channel = "GAB"
        elif terminal_id.startswith("TPE"):
            channel = "TPE"
        elif terminal_id.startswith("ECOM"):
            channel = "ECOM"
        else:
            return  # Canal inconnu, pas de vérification

        # Vérifier la cohérence
        if channel == "ECOM" and pos_entry.startswith("05"):
            result.add_error(ValidationError(
                layer=ValidationLayer.ML_QUALITY,
                severity=Severity.WARNING,
                field="pos_entry_mode",
                rule="CHANNEL_POS_COHERENCE",
                message=(
                    f"Terminal ECOM avec POS Entry Mode chip ({pos_entry}). "
                    f"E-commerce devrait utiliser 812 (CNP) ou 010 (manual)."
                ),
                value=f"terminal={terminal_id}, POS={pos_entry}",
            ))

        if channel == "GAB" and pos_entry.startswith("81"):
            result.add_error(ValidationError(
                layer=ValidationLayer.ML_QUALITY,
                severity=Severity.WARNING,
                field="pos_entry_mode",
                rule="CHANNEL_POS_COHERENCE",
                message=(
                    f"Terminal GAB avec POS Entry Mode e-commerce ({pos_entry}). "
                    f"GAB devrait utiliser 051 (chip) ou 021 (magstripe)."
                ),
                value=f"terminal={terminal_id}, POS={pos_entry}",
            ))


# ===========================================================================
#  ORCHESTRATEUR — TransactionValidator
# ===========================================================================

class TransactionValidator:
    """
    Orchestrateur principal : exécute les 3 couches de validation
    en cascade sur chaque trame parsée.

    Cascade : si la Couche N échoue (ERROR), on n'exécute PAS la Couche N+1.
    Les WARNINGs ne bloquent pas la cascade.

    Usage :
        validator = TransactionValidator()
        result = validator.validate(parsed_dict)
    """

    def __init__(self):
        self.layer1 = StructuralValidator()
        self.layer2 = BusinessValidator()
        self.layer3 = MLQualityValidator()

        # Compteurs pour métriques
        self.metrics = {
            "total_validated": 0,
            "total_clean": 0,
            "total_quarantine": 0,
            "errors_by_layer": {
                "STRUCTURAL": 0,
                "BUSINESS": 0,
                "ML_QUALITY": 0,
            },
            "errors_by_rule": {},
            "warnings_total": 0,
        }

    def validate(self, data: dict) -> ValidationResult:
        """
        Valide une trame parsée (dict) à travers les 3 couches.

        Parameters
        ----------
        data : dict
            Le dictionnaire issu de parse_iso_to_dict() du Smart Parser.

        Returns
        -------
        ValidationResult
            Contient is_valid, errors, warnings, et métadonnées.
        """
        result = ValidationResult()
        result.validated_at = datetime.now().isoformat()
        self.metrics["total_validated"] += 1

        # ── Couche 1 : Conformité Structurelle ──
        self.layer1.validate(data, result)
        if not result.is_valid:
            result.layer_failed = ValidationLayer.STRUCTURAL.value
            self._update_metrics(result)
            logger.warning(
                "QUARANTINE [STRUCTURAL] MTI=%s | %d erreur(s) : %s",
                data.get("mti", "?"),
                len(result.errors),
                "; ".join(e.rule for e in result.errors),
            )
            return result
        result.layers_passed.append(ValidationLayer.STRUCTURAL.value)

        # ── Couche 2 : Validité Monétique ──
        self.layer2.validate(data, result)
        if not result.is_valid:
            result.layer_failed = ValidationLayer.BUSINESS.value
            self._update_metrics(result)
            logger.warning(
                "QUARANTINE [BUSINESS] MTI=%s | PAN=%s | %d erreur(s) : %s",
                data.get("mti", "?"),
                self._mask_pan(data.get("pan")),
                len(result.errors),
                "; ".join(e.rule for e in result.errors),
            )
            return result
        result.layers_passed.append(ValidationLayer.BUSINESS.value)

        # ── Couche 3 : Intégrité ML ──
        self.layer3.validate(data, result)
        if not result.is_valid:
            result.layer_failed = ValidationLayer.ML_QUALITY.value
            self._update_metrics(result)
            logger.warning(
                "QUARANTINE [ML_QUALITY] MTI=%s | PAN=%s | %d erreur(s) : %s",
                data.get("mti", "?"),
                self._mask_pan(data.get("pan")),
                len(result.errors),
                "; ".join(e.rule for e in result.errors),
            )
            return result
        result.layers_passed.append(ValidationLayer.ML_QUALITY.value)

        # ── Trame CLEAN ──
        self.metrics["total_clean"] += 1
        if result.warnings:
            self.metrics["warnings_total"] += len(result.warnings)
            logger.info(
                "CLEAN (avec %d warning(s)) MTI=%s | PAN=%s",
                len(result.warnings),
                data.get("mti", "?"),
                self._mask_pan(data.get("pan")),
            )
        else:
            logger.info(
                "CLEAN MTI=%s | PAN=%s",
                data.get("mti", "?"),
                self._mask_pan(data.get("pan")),
            )

        return result

    def _update_metrics(self, result: ValidationResult) -> None:
        """Met à jour les compteurs internes."""
        self.metrics["total_quarantine"] += 1
        if result.layer_failed:
            self.metrics["errors_by_layer"][result.layer_failed] += 1
        for error in result.errors:
            rule = error.rule
            self.metrics["errors_by_rule"][rule] = (
                self.metrics["errors_by_rule"].get(rule, 0) + 1
            )
        self.metrics["warnings_total"] += len(result.warnings)

    @staticmethod
    def _mask_pan(pan: Optional[str]) -> str:
        """Masque le PAN pour les logs (PCI DSS)."""
        if not pan or not isinstance(pan, str) or len(pan) < 8:
            return pan or "?"
        return f"{pan[:6]}****{pan[-4:]}"

    def get_metrics(self) -> dict:
        """Retourne les métriques de validation (pour Prometheus/Grafana)."""
        return {
            **self.metrics,
            "clean_rate": (
                self.metrics["total_clean"] / max(self.metrics["total_validated"], 1)
            ),
            "quarantine_rate": (
                self.metrics["total_quarantine"] / max(self.metrics["total_validated"], 1)
            ),
        }

    def print_metrics(self) -> None:
        """Affiche un rapport de métriques."""
        m = self.get_metrics()
        print()
        print("=" * 64)
        print("  RAPPORT VALIDATION — QUALITY CHECK")
        print("=" * 64)
        print(f"  Total validées     : {m['total_validated']}")
        print(f"  Clean              : {m['total_clean']} ({m['clean_rate']:.1%})")
        print(f"  Quarantine         : {m['total_quarantine']} ({m['quarantine_rate']:.1%})")
        print(f"  Warnings           : {m['warnings_total']}")
        print("-" * 64)
        print("  Rejets par couche :")
        for layer, count in m["errors_by_layer"].items():
            print(f"    {layer:<18s} : {count}")
        print("  Rejets par règle :")
        for rule, count in sorted(
            m["errors_by_rule"].items(), key=lambda x: -x[1]
        ):
            print(f"    {rule:<30s} : {count}")
        print("=" * 64)


# ===========================================================================
#  FONCTIONS UTILITAIRES POUR L'INTÉGRATION
# ===========================================================================

def enrich_with_validation(data: dict, result: ValidationResult) -> dict:
    """
    Enrichit le dict de la trame avec les métadonnées de validation.
    Utilisé avant l'écriture dans la Curated Zone ou la Quarantine.
    """
    enriched = data.copy()
    enriched.update(result.to_dict())
    return enriched




    # # ── Rapport final ──
    # validator.print_metrics()
