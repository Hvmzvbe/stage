"""
================================================================================
 Al Barid Bank — Plateforme MLOps Détection de Fraude
 Module : ISO 8583 Universal Parser (Bloc Ingestion Layer)
 Auteur : PFE Hamza — EMSI 5ème Année
 Version : 1.0.0
================================================================================

 Description :
   Script de décodage universel des trames ISO 8583 reçues via Kafka.
   Transforme chaque message binaire/ASCII en JSON structuré, prêt pour
   le Data Lakehouse (Raw Zone / Curated Zone) et le Feature Engineering.

 Flux Kafka :
   [iso8583_raw] → UniversalISO8583Parser → [iso8583_cleaned] | [quarantine_zone]

 Dépendances :
   pip install pyiso8583 confluent-kafka python-dotenv

================================================================================
"""

import json
import logging
import os
import sys
import time
from datetime import datetime, timezone
from typing import Any

import pyiso8583
import pyiso8583.specs
from confluent_kafka import Consumer, KafkaError, KafkaException, Producer
from dotenv import load_dotenv

# ─────────────────────────────────────────────────────────────────────────────
# CONFIGURATION LOGGING — Format structuré JSON-like pour intégration Grafana
# ─────────────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("/tmp/iso8583_parser.log", encoding="utf-8"),
    ],
)
logger = logging.getLogger("ISO8583Parser")

# ─────────────────────────────────────────────────────────────────────────────
# MAPPING DES DATA ELEMENTS ISO 8583
# Source : ISO 8583:1993 + extensions Al Barid Bank
# Facilite le Feature Engineering en ajoutant le nom sémantique de chaque champ
# ─────────────────────────────────────────────────────────────────────────────
DE_FIELD_MAPPING: dict[int, dict[str, str]] = {
    1:   {"name": "Secondary Bitmap",          "category": "bitmap"},
    2:   {"name": "Primary Account Number",    "category": "card",       "iso_ref": "PAN"},
    3:   {"name": "Processing Code",           "category": "transaction", "iso_ref": "DE3"},
    4:   {"name": "Transaction Amount",        "category": "amount",      "iso_ref": "DE4"},
    5:   {"name": "Settlement Amount",         "category": "amount",      "iso_ref": "DE5"},
    6:   {"name": "Cardholder Billing Amount", "category": "amount",      "iso_ref": "DE6"},
    7:   {"name": "Transmission DateTime",     "category": "datetime",    "iso_ref": "DE7"},
    9:   {"name": "Conversion Rate Settlement","category": "fx",          "iso_ref": "DE9"},
    11:  {"name": "System Trace Audit Number", "category": "transaction", "iso_ref": "STAN"},
    12:  {"name": "Local Transaction Time",    "category": "datetime",    "iso_ref": "DE12"},
    13:  {"name": "Local Transaction Date",    "category": "datetime",    "iso_ref": "DE13"},
    14:  {"name": "Expiration Date",           "category": "card",        "iso_ref": "DE14"},
    15:  {"name": "Settlement Date",           "category": "datetime",    "iso_ref": "DE15"},
    18:  {"name": "Merchant Category Code",    "category": "merchant",    "iso_ref": "MCC"},
    19:  {"name": "Acquiring Country Code",    "category": "geography",   "iso_ref": "DE19"},
    22:  {"name": "POS Entry Mode",            "category": "terminal",    "iso_ref": "DE22"},
    23:  {"name": "Card Sequence Number",      "category": "card",        "iso_ref": "DE23"},
    25:  {"name": "POS Condition Code",        "category": "terminal",    "iso_ref": "DE25"},
    26:  {"name": "POS PIN Capture Code",      "category": "terminal",    "iso_ref": "DE26"},
    32:  {"name": "Acquiring Institution ID",  "category": "institution", "iso_ref": "DE32"},
    33:  {"name": "Forwarding Institution ID", "category": "institution", "iso_ref": "DE33"},
    35:  {"name": "Track 2 Data",              "category": "card",        "iso_ref": "DE35"},
    37:  {"name": "Retrieval Reference Number","category": "transaction", "iso_ref": "RRN"},
    38:  {"name": "Authorization ID Response", "category": "transaction", "iso_ref": "DE38"},
    39:  {"name": "Response Code",             "category": "response",    "iso_ref": "DE39"},
    41:  {"name": "Card Acceptor Terminal ID", "category": "terminal",    "iso_ref": "TID"},
    42:  {"name": "Card Acceptor ID Code",     "category": "merchant",    "iso_ref": "MID"},
    43:  {"name": "Card Acceptor Name/Location","category": "geography",  "iso_ref": "DE43"},
    44:  {"name": "Additional Response Data",  "category": "response",    "iso_ref": "DE44"},
    45:  {"name": "Track 1 Data",              "category": "card",        "iso_ref": "DE45"},
    48:  {"name": "Additional Data (Private)", "category": "private",     "iso_ref": "DE48"},
    49:  {"name": "Transaction Currency Code", "category": "fx",          "iso_ref": "DE49"},
    50:  {"name": "Settlement Currency Code",  "category": "fx",          "iso_ref": "DE50"},
    51:  {"name": "Cardholder Billing Currency","category": "fx",         "iso_ref": "DE51"},
    52:  {"name": "Personal ID Number (PIN)",  "category": "security",    "iso_ref": "DE52"},
    54:  {"name": "Additional Amounts",        "category": "amount",      "iso_ref": "DE54"},
    55:  {"name": "EMV Chip Data",             "category": "emv",         "iso_ref": "DE55"},
    60:  {"name": "Reserved (National)",       "category": "private",     "iso_ref": "DE60"},
    61:  {"name": "Reserved (National)",       "category": "private",     "iso_ref": "DE61"},
    63:  {"name": "Reserved (Private)",        "category": "private",     "iso_ref": "DE63"},
    70:  {"name": "Network Management Info",   "category": "network",     "iso_ref": "DE70"},
    90:  {"name": "Original Data Elements",    "category": "transaction", "iso_ref": "DE90"},
    95:  {"name": "Replacement Amounts",       "category": "amount",      "iso_ref": "DE95"},
    100: {"name": "Receiving Institution ID",  "category": "institution", "iso_ref": "DE100"},
    102: {"name": "Account ID 1",              "category": "account",     "iso_ref": "DE102"},
    103: {"name": "Account ID 2",              "category": "account",     "iso_ref": "DE103"},
    127: {"name": "Reserved (Private)",        "category": "private",     "iso_ref": "DE127"},
    128: {"name": "MAC (Secondary)",           "category": "security",    "iso_ref": "DE128"},
}

# ─────────────────────────────────────────────────────────────────────────────
# CLASSIFICATION MTI — Message Type Indicator
# Permet d'enrichir le JSON avec le type de transaction dès l'ingestion
# ─────────────────────────────────────────────────────────────────────────────
MTI_DESCRIPTIONS: dict[str, str] = {
    "0100": "Authorization Request",
    "0110": "Authorization Response",
    "0120": "Authorization Advice",
    "0121": "Authorization Advice Repeat",
    "0130": "Authorization Advice Response",
    "0200": "Financial Transaction Request",
    "0210": "Financial Transaction Response",
    "0220": "Financial Transaction Advice",
    "0221": "Financial Transaction Advice Repeat",
    "0230": "Financial Transaction Advice Response",
    "0400": "Reversal Request",
    "0410": "Reversal Response",
    "0420": "Reversal Advice",
    "0421": "Reversal Advice Repeat",
    "0430": "Reversal Advice Response",
    "0800": "Network Management Request",
    "0810": "Network Management Response",
    "0820": "Network Management Advice",
}


# =============================================================================
# CLASSE PRINCIPALE : UniversalISO8583Parser
# =============================================================================
class UniversalISO8583Parser:
    """
    Parser universel pour les trames ISO 8583 (ASCII/BCD).

    Responsabilités :
    - Décoder dynamiquement toute trame ISO 8583 (DE 1 à 128)
    - Enrichir le JSON avec les métadonnées sémantiques (nom du champ, catégorie)
    - Router vers iso8583_cleaned (succès) ou quarantine_zone (erreur)
    - Maintenir des métriques de processing pour Prometheus/Grafana
    """

    def __init__(self, spec: dict | None = None):
        """
        Initialise le parser avec la spécification ISO 8583.

        Args:
            spec: Dictionnaire de spécification pyiso8583.
                  Défaut : ASCII spec (compatible réseau monétique marocain).
        """
        self.spec = spec or pyiso8583.specs.default_ascii
        self._metrics = {
            "total_processed": 0,
            "total_success": 0,
            "total_quarantine": 0,
            "total_errors": 0,
            "start_time": time.time(),
        }
        logger.info("UniversalISO8583Parser initialisé avec spec: %s",
                    type(self.spec).__name__)

    # ─────────────────────────────────────────────────────────────────────────
    # MÉTHODE PRINCIPALE : parse_frame
    # ─────────────────────────────────────────────────────────────────────────
    def parse_frame(self, raw_frame: bytes) -> dict[str, Any]:
        """
        Décode une trame ISO 8583 brute en JSON structuré.

        Pipeline interne :
          1. Décodage pyiso8583 (MTI + Bitmap + tous les DEs présents)
          2. Enrichissement sémantique (noms, catégories, flags de fraude)
          3. Ajout de métadonnées de traitement (timestamp, version)

        Args:
            raw_frame: La trame ISO 8583 brute en bytes.

        Returns:
            Dictionnaire Python complet (sérialisable en JSON).

        Raises:
            ValueError: Si la trame est vide ou incohérente.
            pyiso8583.DecodeError: Si le décodage échoue.
        """
        if not raw_frame:
            raise ValueError("Trame vide reçue — message ignoré")

        # ── 1. DÉCODAGE PYISO8583 ──────────────────────────────────────────
        # pyiso8583.decode retourne (doc, raw_doc)
        # doc = dictionnaire des champs décodés
        # raw_doc = les valeurs brutes avant interprétation
        doc, raw_doc = pyiso8583.decode(raw_frame, spec=self.spec)

        # ── 2. EXTRACTION DU MTI ──────────────────────────────────────────
        mti = doc.get("t", "UNKNOWN")
        mti_description = MTI_DESCRIPTIONS.get(mti, f"Unknown MTI: {mti}")

        # ── 3. CONSTRUCTION DU JSON ENRICHI ──────────────────────────────
        parsed_message: dict[str, Any] = {
            # Métadonnées de traitement
            "_metadata": {
                "parser_version": "1.0.0",
                "parsed_at": datetime.now(timezone.utc).isoformat(),
                "source_topic": "iso8583_raw",
                "raw_length_bytes": len(raw_frame),
                "spec_used": "ASCII",
            },
            # Informations MTI
            "MTI": mti,
            "MTI_description": mti_description,
            "transaction_type": self._classify_transaction(mti),
            # Bitmap info (pour debug et audit)
            "bitmap_info": {
                "primary_bitmap_hex": raw_doc.get("p", b"").hex(),
                "secondary_bitmap_present": "1" in doc,
            },
        }

        # ── 4. EXTRACTION DE TOUS LES DATA ELEMENTS PRÉSENTS ─────────────
        # pyiso8583 peuple le doc avec les clés "2" à "128" pour chaque
        # DE présent dans le bitmap.
        de_fields: dict[str, Any] = {}
        active_de_numbers: list[int] = []

        for de_num_str, value in doc.items():
            # Ignorer les clés internes de pyiso8583 (t=MTI, p=primary bitmap)
            if de_num_str in ("t", "p"):
                continue

            try:
                de_num = int(de_num_str)
            except (ValueError, TypeError):
                continue

            active_de_numbers.append(de_num)
            de_key = f"DE_{de_num:03d}"

            # Récupérer les métadonnées du champ depuis notre mapping
            field_meta = DE_FIELD_MAPPING.get(de_num, {})
            field_name = field_meta.get("name", f"Field {de_num}")
            field_category = field_meta.get("category", "unknown")

            # Masquer les données sensibles (PCI-DSS compliance)
            display_value = self._mask_sensitive_field(de_num, str(value))

            de_fields[de_key] = {
                "value": display_value,
                "field_name": field_name,
                "category": field_category,
                "de_number": de_num,
            }

        parsed_message["data_elements"] = de_fields
        parsed_message["active_de_count"] = len(active_de_numbers)
        parsed_message["active_de_list"] = sorted(active_de_numbers)

        # ── 5. EXTRACTION DES CHAMPS CRITIQUES POUR LE FEATURE ENGINEERING ─
        # Accès direct aux champs les plus importants pour la fraude
        parsed_message["fraud_features"] = self._extract_fraud_features(doc)

        return parsed_message

    # ─────────────────────────────────────────────────────────────────────────
    # EXTRACTION DES FEATURES ANTI-FRAUDE (accès rapide)
    # ─────────────────────────────────────────────────────────────────────────
    def _extract_fraud_features(self, doc: dict) -> dict[str, Any]:
        """
        Extrait et normalise les champs critiques pour la détection de fraude.
        Ces champs correspondent directement aux règles métier définies dans
        la spécification du projet.

        Champs ISO 8583 utilisés :
          - DE 4  → Montant (Amount)
          - DE 7  → Date/Heure de transmission (pour Impossible Travel)
          - DE 12 → Heure locale (pour Incohérence Heure/Commerce)
          - DE 13 → Date locale
          - DE 18 → MCC (Merchant Category Code — Incohérence commerce)
          - DE 22 → POS Entry Mode (Card Present vs Not Present)
          - DE 39 → Response Code (Card Testing — détection des refus)
          - DE 41 → Terminal ID
          - DE 43 → Localisation du terminal (Géographie / Impossible Travel)
          - DE 49 → Devise (Currency Code)
        """
        features: dict[str, Any] = {}

        # Montant (DE 4) — converti en float (les 2 derniers chiffres = centimes)
        if "4" in doc:
            try:
                amount_str = str(doc["4"]).zfill(12)
                features["amount_mad"] = float(amount_str) / 100.0
                # Flag Card Testing : micro-montants ≤ 50 MAD
                features["is_micro_amount"] = features["amount_mad"] <= 50.0
            except (ValueError, TypeError):
                features["amount_mad"] = None
                features["is_micro_amount"] = False

        # Date/Heure de transmission (DE 7) — format MMDDhhmmss
        if "7" in doc:
            features["transmission_datetime_raw"] = str(doc["7"])
            features["transmission_datetime_iso"] = self._parse_iso8583_datetime(
                str(doc["7"])
            )

        # Heure locale (DE 12) — format hhmmss → pour détection heure anormale
        if "12" in doc:
            try:
                time_str = str(doc["12"]).zfill(6)
                hour = int(time_str[:2])
                features["local_hour"] = hour
                # Flag : transaction entre 23h et 5h du matin
                features["is_odd_hour"] = hour >= 23 or hour <= 5
            except (ValueError, TypeError):
                features["local_hour"] = None
                features["is_odd_hour"] = False

        # Date locale (DE 13) — format MMDD
        if "13" in doc:
            features["local_date_raw"] = str(doc["13"])

        # MCC — Merchant Category Code (DE 18)
        if "18" in doc:
            mcc = str(doc["18"])
            features["mcc"] = mcc
            features["merchant_category"] = self._classify_mcc(mcc)
            # Flag : commerces à risque élevé (bijouteries, casinos, crypto)
            features["is_high_risk_merchant"] = mcc in self._get_high_risk_mcc_list()

        # POS Entry Mode (DE 22)
        if "22" in doc:
            pos_mode = str(doc["22"])
            features["pos_entry_mode"] = pos_mode
            # Distinction Card Present / Card Not Present (CNP = fraude plus facile)
            features["is_card_not_present"] = pos_mode[:2] in ("01", "10", "81", "82")

        # Response Code (DE 39)
        if "39" in doc:
            response_code = str(doc["39"])
            features["response_code"] = response_code
            # Flag : transaction refusée (Card Testing : série de refus)
            features["is_declined"] = response_code not in ("00", "000", "0000", "10")

        # Terminal ID (DE 41) — pour le profiling terminal
        if "41" in doc:
            features["terminal_id"] = str(doc["41"]).strip()

        # Localisation terminal (DE 43) — format: NomCommerce    VilleCode
        if "43" in doc:
            location_raw = str(doc["43"])
            features["terminal_location_raw"] = location_raw
            parsed_location = self._parse_de43_location(location_raw)
            features.update(parsed_location)

        # PAN masqué (DE 2) — pour le profiling client
        if "2" in doc:
            pan = str(doc["2"])
            features["pan_masked"] = self._mask_pan(pan)
            features["pan_bin"] = pan[:6] if len(pan) >= 6 else pan  # BIN = 6 premiers

        # Currency Code (DE 49)
        if "49" in doc:
            features["currency_code"] = str(doc["49"])
            # Flag : devise étrangère pour un client habituel MAD
            features["is_foreign_currency"] = str(doc["49"]) != "504"  # 504 = MAD

        # STAN (DE 11) — System Trace Audit Number
        if "11" in doc:
            features["stan"] = str(doc["11"])

        # RRN (DE 37) — Retrieval Reference Number
        if "37" in doc:
            features["rrn"] = str(doc["37"])

        return features

    # ─────────────────────────────────────────────────────────────────────────
    # MÉTHODES UTILITAIRES
    # ─────────────────────────────────────────────────────────────────────────

    def _classify_transaction(self, mti: str) -> str:
        """Classifie le type de transaction à partir du MTI."""
        mti_map = {
            "01": "Authorization",
            "02": "Financial",
            "04": "Reversal",
            "08": "NetworkManagement",
        }
        return mti_map.get(mti[:2], "Unknown")

    def _parse_iso8583_datetime(self, dt_str: str) -> str | None:
        """
        Convertit une date ISO 8583 (MMDDhhmmss) en ISO 8601.
        Ajoute l'année courante car ISO 8583 ne la stocke pas dans DE 7.
        """
        try:
            current_year = datetime.now().year
            month = int(dt_str[0:2])
            day = int(dt_str[2:4])
            hour = int(dt_str[4:6])
            minute = int(dt_str[6:8])
            second = int(dt_str[8:10])
            dt = datetime(current_year, month, day, hour, minute, second, tzinfo=timezone.utc)
            return dt.isoformat()
        except (ValueError, IndexError, TypeError):
            return None

    def _parse_de43_location(self, location_raw: str) -> dict[str, str]:
        """
        Parse le champ DE 43 (Card Acceptor Name/Location).
        Format standard : <NomCommerce (22)><Ville (13)><CodePays (2)>
        """
        result = {}
        try:
            # Format fixe 40 caractères selon ISO 8583
            location_raw = location_raw.ljust(40)
            result["merchant_name"] = location_raw[:22].strip()
            result["merchant_city"] = location_raw[22:35].strip()
            result["merchant_country"] = location_raw[35:37].strip()
        except Exception:  # noqa: BLE001
            result["merchant_name"] = location_raw.strip()
        return result

    def _classify_mcc(self, mcc: str) -> str:
        """Retourne la catégorie humaine d'un MCC."""
        mcc_categories = {
            "5411": "Supermarché", "5812": "Restaurant", "5411": "Épicerie",
            "5912": "Pharmacie", "5999": "Commerce divers", "6010": "Retrait bancaire",
            "6011": "Distributeur ATM", "6012": "Institution financière",
            "7011": "Hôtel / Hébergement", "7372": "Informatique / IT",
            "5094": "Bijouterie / Pierres précieuses", "7995": "Jeux / Casino",
            "6051": "Crypto / Non-financial institutions", "5912": "Pharmacie",
            "4111": "Transport en commun", "4121": "Taxi", "4814": "Téléphonie",
            "5310": "Discount Store", "5651": "Vêtements", "5732": "Électronique",
        }
        return mcc_categories.get(mcc, f"Code MCC {mcc}")

    def _get_high_risk_mcc_list(self) -> set[str]:
        """Retourne la liste des MCC à risque élevé de fraude."""
        return {
            "5094",  # Bijouteries / Pierres précieuses
            "7995",  # Jeux de hasard / Casinos
            "6051",  # Crypto / Non-financial institutions
            "6012",  # Money services
            "5912",  # Pharmacies (abus fréquents)
            "4829",  # Wire transfers
            "6540",  # Prepaid cards
        }

    def _mask_sensitive_field(self, de_num: int, value: str) -> str:
        """
        Masque les données sensibles selon PCI-DSS.
        DE 2 (PAN), DE 35 (Track 2), DE 45 (Track 1), DE 52 (PIN) sont masqués.
        """
        sensitive_fields = {2, 35, 45, 52}
        if de_num in sensitive_fields:
            return self._mask_pan(value)
        return value

    @staticmethod
    def _mask_pan(pan: str) -> str:
        """Masque un PAN selon PCI-DSS : conserve les 6 premiers et 4 derniers chiffres."""
        pan = pan.strip()
        if len(pan) >= 13:
            return f"{pan[:6]}{'*' * (len(pan) - 10)}{pan[-4:]}"
        return "*" * len(pan)

    def get_metrics(self) -> dict[str, Any]:
        """Retourne les métriques de performance pour Prometheus."""
        elapsed = time.time() - self._metrics["start_time"]
        tps = self._metrics["total_success"] / elapsed if elapsed > 0 else 0
        return {
            **self._metrics,
            "uptime_seconds": round(elapsed, 2),
            "transactions_per_second": round(tps, 2),
            "success_rate_pct": round(
                self._metrics["total_success"]
                / max(self._metrics["total_processed"], 1)
                * 100, 2
            ),
        }


# =============================================================================
# KAFKA WORKER — Boucle de consommation temps réel
# =============================================================================
class KafkaISO8583Worker:
    """
    Worker Kafka pour la consommation continue du topic iso8583_raw.

    Gère le cycle complet :
    1. Consommation depuis iso8583_raw
    2. Décodage via UniversalISO8583Parser
    3. Publication vers iso8583_cleaned ou quarantine_zone
    4. Commit du offset (at-least-once delivery)
    """

    TOPIC_RAW = "iso8583_raw"
    TOPIC_CLEANED = "iso8583_cleaned"
    TOPIC_QUARANTINE = "quarantine_zone"

    def __init__(self):
        load_dotenv()

        kafka_bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
        kafka_group = os.getenv("KAFKA_CONSUMER_GROUP", "iso8583-parser-group")

        # Configuration Consumer
        consumer_config = {
            "bootstrap.servers": kafka_bootstrap,
            "group.id": kafka_group,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,        # Commit manuel pour fiabilité
            "session.timeout.ms": 30000,
            "max.poll.interval.ms": 300000,
            "fetch.max.bytes": 52428800,        # 50 MB max par fetch
        }

        # Configuration Producer
        producer_config = {
            "bootstrap.servers": kafka_bootstrap,
            "acks": "all",                       # Durabilité maximale
            "retries": 5,
            "retry.backoff.ms": 500,
            "compression.type": "snappy",        # Compression pour les gros volumes
            "linger.ms": 5,                      # Micro-batching pour throughput
        }

        self.consumer = Consumer(consumer_config)
        self.producer = Producer(producer_config)
        self.parser = UniversalISO8583Parser()

        logger.info(
            "KafkaISO8583Worker initialisé | Bootstrap: %s | Group: %s",
            kafka_bootstrap, kafka_group
        )

    def _delivery_callback(self, err, msg):
        """Callback de livraison pour le producer Kafka (logging des erreurs)."""
        if err is not None:
            logger.error("Échec livraison message | Topic: %s | Erreur: %s",
                         msg.topic(), err)
        else:
            logger.debug("Message livré | Topic: %s | Partition: %d | Offset: %d",
                         msg.topic(), msg.partition(), msg.offset())

    def _publish(self, topic: str, payload: dict, key: str | None = None):
        """Sérialise et publie un dictionnaire JSON vers un topic Kafka."""
        try:
            message_bytes = json.dumps(payload, ensure_ascii=False, default=str).encode("utf-8")
            self.producer.produce(
                topic=topic,
                value=message_bytes,
                key=key.encode("utf-8") if key else None,
                callback=self._delivery_callback,
            )
            self.producer.poll(0)  # Déclenche les callbacks en attente
        except Exception as e:
            logger.critical("Impossible de publier sur %s : %s", topic, e)
            raise

    def run(self):
        """
        Boucle principale de consommation.
        Traite les messages en continu jusqu'à interruption (Ctrl+C ou SIGTERM).
        """
        self.consumer.subscribe([self.TOPIC_RAW])
        logger.info("Démarrage écoute topic: %s", self.TOPIC_RAW)

        try:
            while True:
                # Poll avec timeout de 1 seconde (non-bloquant)
                msg = self.consumer.poll(timeout=1.0)

                if msg is None:
                    # Aucun message — log des métriques toutes les ~60s
                    continue

                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        # Fin de partition — normal en l'absence de nouveaux messages
                        logger.debug("Fin de partition atteinte : %s [%d] @ %d",
                                     msg.topic(), msg.partition(), msg.offset())
                    else:
                        raise KafkaException(msg.error())
                    continue

                # ── TRAITEMENT DU MESSAGE ──────────────────────────────────
                self.parser._metrics["total_processed"] += 1
                raw_value = msg.value()
                message_key = msg.key().decode("utf-8") if msg.key() else None

                try:
                    # ── PARSING ISO 8583 ──────────────────────────────────
                    parsed = self.parser.parse_frame(raw_value)

                    # Enrichissement avec les métadonnées Kafka
                    parsed["_metadata"]["kafka_partition"] = msg.partition()
                    parsed["_metadata"]["kafka_offset"] = msg.offset()
                    parsed["_metadata"]["kafka_timestamp"] = msg.timestamp()[1]

                    # Publication vers le topic cleaned
                    partition_key = parsed.get("fraud_features", {}).get("pan_masked", message_key)
                    self._publish(self.TOPIC_CLEANED, parsed, key=partition_key)

                    self.parser._metrics["total_success"] += 1
                    logger.info(
                        "✓ Parsed | MTI: %s | DEs actifs: %d | Montant: %s MAD | Partition: %d | Offset: %d",
                        parsed.get("MTI"),
                        parsed.get("active_de_count", 0),
                        parsed.get("fraud_features", {}).get("amount_mad", "N/A"),
                        msg.partition(),
                        msg.offset(),
                    )

                except (pyiso8583.DecodeError, ValueError) as e:
                    # ── QUARANTINE : Trame invalide ────────────────────────
                    self.parser._metrics["total_quarantine"] += 1
                    quarantine_payload = {
                        "_metadata": {
                            "quarantine_reason": str(e),
                            "quarantine_at": datetime.now(timezone.utc).isoformat(),
                            "kafka_partition": msg.partition(),
                            "kafka_offset": msg.offset(),
                            "raw_hex": raw_value.hex() if raw_value else "",
                            "raw_length_bytes": len(raw_value) if raw_value else 0,
                        },
                        "error_type": type(e).__name__,
                        "error_message": str(e),
                    }
                    self._publish(self.TOPIC_QUARANTINE, quarantine_payload)
                    logger.warning(
                        "⚠ QUARANTINE | Raison: %s | Offset: %d | Raw (hex, 50B): %s",
                        e, msg.offset(), raw_value[:50].hex() if raw_value else ""
                    )

                except Exception as e:
                    # ── ERREUR INATTENDUE ──────────────────────────────────
                    self.parser._metrics["total_errors"] += 1
                    logger.error(
                        "✗ Erreur critique sur offset %d : %s",
                        msg.offset(), e, exc_info=True
                    )

                finally:
                    # Commit manuel — garantit "at-least-once" delivery
                    self.consumer.commit(asynchronous=False)

                # Log métriques toutes les 1000 transactions
                if self.parser._metrics["total_processed"] % 1000 == 0:
                    metrics = self.parser.get_metrics()
                    logger.info("📊 Métriques | %s", json.dumps(metrics))

        except KeyboardInterrupt:
            logger.info("Arrêt demandé par l'utilisateur (Ctrl+C)")
        except Exception as e:
            logger.critical("Erreur fatale dans la boucle principale : %s", e, exc_info=True)
            raise
        finally:
            self._shutdown()

    def _shutdown(self):
        """Fermeture propre des connexions Kafka."""
        logger.info("Fermeture des connexions Kafka...")
        try:
            self.producer.flush(timeout=10)  # Vider le buffer producer
            self.consumer.close()
            logger.info("Connexions fermées proprement.")
        except Exception as e:
            logger.error("Erreur lors de la fermeture : %s", e)

        # Afficher les métriques finales
        final_metrics = self.parser.get_metrics()
        logger.info("📊 Métriques finales : %s", json.dumps(final_metrics, indent=2))


# =============================================================================
# POINT D'ENTRÉE
# =============================================================================
if __name__ == "__main__":
    logger.info("=" * 70)
    logger.info("  Al Barid Bank — ISO 8583 Universal Parser")
    logger.info("  Plateforme MLOps Détection de Fraude — v1.0.0")
    logger.info("=" * 70)

    worker = KafkaISO8583Worker()
    worker.run()
