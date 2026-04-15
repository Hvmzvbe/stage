"""
===========================================================================
Module : kafka_consumer.py
Projet : Plateforme MLOps - Détection de Fraude (Al Barid Bank)

---------------------------------------------------------------------------
Rôle   : Consumer Kafka — Parser ISO 8583 en temps réel.

         Flux :
           raw_stream (bytes ISO 8583)
               |
           Smart Parser (auto-détection spec87/93/03)
               |
               |--- cleaned_transactions (topic Kafka - JSON)
               |--- parsed_transactions.log (fichier local - JSON)
               |--- parsing_errors (topic Kafka - erreurs)

Usage  :
    python kafka_consumer.py
    python kafka_consumer.py --bootstrap localhost:9092
    python kafka_consumer.py --log-file mon_fichier.log

Requires : pip install confluent-kafka pyiso8583
===========================================================================
"""

import argparse
import json
import logging
import os
import signal
from datetime import datetime

from confluent_kafka import Consumer, Producer, KafkaError, KafkaException

# Import du Smart Parser
from iso8583_smart_parser import (
    parse_iso_to_dict,
)
import iso8583

# ---------------------------------------------------------------------------
# Logging console
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(name)s | %(message)s",
)
logger = logging.getLogger("kafka_consumer")

# ---------------------------------------------------------------------------
# Configuration par défaut
# ---------------------------------------------------------------------------
CONSUMER_CONFIG = {
    "bootstrap.servers": "127.0.0.1:9092",
    "group.id": "iso8583-parser-group",
    "auto.offset.reset": "earliest",
    "enable.auto.commit": True,
    "session.timeout.ms": 30000,
}

PRODUCER_CONFIG = {
    "bootstrap.servers": "127.0.0.1:9092",
    "client.id": "iso8583-parser-producer",
}

TOPIC_RAW     = "raw_stream"
TOPIC_CLEANED = "cleaned_transactions"
TOPIC_ERRORS  = "parsing_errors"

DEFAULT_LOG_FILE = "parsed_transactions.log"

# Compteurs
metrics = {
    "total_received": 0,
    "total_parsed": 0,
    "total_errors": 0,
    "by_spec": {},
    "by_channel": {},
    "total_amount_mad": 0.0,
    "started_at": None,
}


# ===========================================================================
#  FILE LOGGER — Écriture JSON dans un fichier log
# ===========================================================================

class TransactionFileLogger:
    """
    Écrit chaque transaction parsée (JSON) dans un fichier log.
    Format JSONL (un JSON par ligne) — idéal pour Spark/Pandas.
    """

    def __init__(self, filepath: str):
        self.filepath = os.path.abspath(filepath)
        self._file = open(self.filepath, "a", encoding="utf-8")
        logger.info("FILE LOGGER ouvert : %s", self.filepath)

    def write(self, data: dict):
        """Écrit une transaction comme une ligne JSON."""
        line = json.dumps(data, ensure_ascii=False, default=str)
        self._file.write(line + "\n")
        self._file.flush()

    def close(self):
        """Ferme le fichier."""
        if self._file and not self._file.closed:
            self._file.close()
            logger.info("FILE LOGGER fermé : %s", self.filepath)

    @property
    def line_count(self):
        """Nombre de lignes dans le fichier."""
        try:
            with open(self.filepath, "r", encoding="utf-8") as f:
                return sum(1 for _ in f)
        except FileNotFoundError:
            return 0


# ===========================================================================
#  ENRICHISSEMENT
# ===========================================================================

def enrich_parsed_data(parsed: dict) -> dict:
    """
    Enrichit le JSON parsé avec des champs dérivés :
    _parsed_at, _amount_mad, _is_atm_withdrawal, _is_ecommerce,
    _channel, _city, _country, _is_international
    """
    enriched = parsed.copy()

    enriched["_parsed_at"] = datetime.now().isoformat()

    raw_amount = parsed.get("amount_transaction")
    if raw_amount and raw_amount.isdigit():
        enriched["_amount_mad"] = int(raw_amount) / 100.0
    else:
        enriched["_amount_mad"] = None

    proc_code = parsed.get("processing_code", "")
    enriched["_is_atm_withdrawal"] = proc_code[:2] == "01"

    pos_condition = parsed.get("pos_condition_code", "")
    pos_entry = parsed.get("pos_entry_mode", "")
    enriched["_is_ecommerce"] = pos_condition == "08" or pos_entry.startswith("81")

    terminal_id = parsed.get("terminal_id", "")
    if terminal_id.startswith("GAB"):
        enriched["_channel"] = "GAB"
    elif terminal_id.startswith("TPE"):
        enriched["_channel"] = "TPE"
    elif terminal_id.startswith("ECOM"):
        enriched["_channel"] = "ECOM"
    else:
        enriched["_channel"] = "OTHER"

    location = parsed.get("card_acceptor_name_location", "")
    if len(location) >= 40:
        enriched["_city"] = location[25:38].strip()
        enriched["_country"] = location[38:40].strip()
    elif len(location) > 2:
        enriched["_city"] = "UNKNOWN"
        enriched["_country"] = location[-2:].strip()
    else:
        enriched["_city"] = None
        enriched["_country"] = None

    enriched["_is_international"] = (
        enriched["_country"] is not None and enriched["_country"] != "MA"
    )

    return enriched


# ===========================================================================
#  TRAITEMENT D'UN MESSAGE
# ===========================================================================

def process_message(
    raw_value: bytes,
    output_producer: Producer,
    file_logger: TransactionFileLogger,
) -> bool:
    """
    1. Détecte le spec
    2. Décode la trame (128 DEs)
    3. Enrichit
    4. Publie sur cleaned_transactions
    5. Écrit dans le fichier log
    """
    metrics["total_received"] += 1

    try:
        # Smart parsing
        parsed = parse_iso_to_dict(raw_value)

        # Enrichissement
        enriched = enrich_parsed_data(parsed)

        # Sérialisation
        json_output = json.dumps(enriched, ensure_ascii=False, default=str)

        # → Kafka cleaned_transactions
        key = parsed.get("pan", "UNKNOWN")
        output_producer.produce(
            topic=TOPIC_CLEANED,
            key=key.encode("utf-8"),
            value=json_output.encode("utf-8"),
        )
        output_producer.poll(0)

        # → Fichier log
        file_logger.write(enriched)

        # Métriques
        metrics["total_parsed"] += 1
        spec = parsed.get("_spec_detected", "unknown")
        metrics["by_spec"][spec] = metrics["by_spec"].get(spec, 0) + 1
        channel = enriched.get("_channel", "OTHER")
        metrics["by_channel"][channel] = metrics["by_channel"].get(channel, 0) + 1
        amount = enriched.get("_amount_mad", 0) or 0
        metrics["total_amount_mad"] += amount

        # Log console
        pan = parsed.get("pan", "?")
        pan_masked = f"{pan[:4]}****{pan[-4:]}" if pan and len(pan) > 8 else pan
        logger.info(
            "PARSED [#%d] %s | %s | %s | %.2f MAD | %s | %s",
            metrics["total_parsed"],
            spec,
            parsed.get("mti", "?"),
            pan_masked,
            amount,
            enriched.get("_channel", "?"),
            enriched.get("_city", "?"),
        )

        return True

    except iso8583.DecodeError as exc:
        metrics["total_errors"] += 1
        error_payload = {
            "error": str(exc),
            "raw_hex": raw_value.hex()[:200],
            "timestamp": datetime.now().isoformat(),
            "raw_length": len(raw_value),
        }
        output_producer.produce(
            topic=TOPIC_ERRORS,
            value=json.dumps(error_payload).encode("utf-8"),
        )
        logger.error("ERREUR PARSING [#%d] : %s", metrics["total_errors"], exc)
        return False

    except Exception as exc:
        metrics["total_errors"] += 1
        logger.error("ERREUR INATTENDUE : %s", exc, exc_info=True)
        return False


# ===========================================================================
#  RAPPORT FINAL
# ===========================================================================

def print_metrics(file_logger: TransactionFileLogger):
    """Affiche le rapport final."""
    elapsed = (datetime.now() - metrics["started_at"]).total_seconds()
    tps = metrics["total_parsed"] / max(elapsed, 1)

    print()
    print("=" * 64)
    print("  RAPPORT FINAL — CONSUMER ISO 8583")
    print("=" * 64)
    print(f"  Duree              : {elapsed:.1f} secondes")
    print(f"  Recus              : {metrics['total_received']}")
    print(f"  Parses OK          : {metrics['total_parsed']}")
    print(f"  Erreurs            : {metrics['total_errors']}")
    print(f"  Debit              : {tps:.1f} trames/sec")
    print(f"  Montant total      : {metrics['total_amount_mad']:.2f} MAD")
    print(f"  Fichier log        : {file_logger.filepath}")
    print(f"  Lignes dans le log : {file_logger.line_count}")
    print("-" * 64)
    print("  Par spec :")
    for spec, cnt in metrics["by_spec"].items():
        print(f"    {spec:<22s} : {cnt}")
    print("  Par canal :")
    for ch, cnt in metrics["by_channel"].items():
        print(f"    {ch:<22s} : {cnt}")
    print("=" * 64)


# ===========================================================================
#  BOUCLE PRINCIPALE
# ===========================================================================

def run_consumer(
    consumer_config: dict = None,
    producer_config: dict = None,
    topic_in: str = TOPIC_RAW,
    log_file: str = DEFAULT_LOG_FILE,
):
    """
    Boucle principale du Consumer :
    - Écoute raw_stream
    - Parse chaque trame ISO 8583
    - Publie sur cleaned_transactions
    - Écrit dans le fichier log
    """
    c_config = consumer_config or CONSUMER_CONFIG
    p_config = producer_config or PRODUCER_CONFIG

    consumer = Consumer(c_config)
    producer = Producer(p_config)
    file_logger = TransactionFileLogger(log_file)

    consumer.subscribe([topic_in])

    logger.info("=" * 60)
    logger.info("  CONSUMER ISO 8583 DEMARRE")
    logger.info("  Topic IN       : %s", topic_in)
    logger.info("  Topic OUT      : %s", TOPIC_CLEANED)
    logger.info("  Topic ERRORS   : %s", TOPIC_ERRORS)
    logger.info("  Fichier LOG    : %s", file_logger.filepath)
    logger.info("  Group ID       : %s", c_config["group.id"])
    logger.info("  Bootstrap      : %s", c_config["bootstrap.servers"])
    logger.info("=" * 60)
    logger.info("En attente de trames ISO 8583 sur [%s]...", topic_in)

    metrics["started_at"] = datetime.now()

    # Arrêt propre avec Ctrl+C
    running = True

    def signal_handler(sig, frame):
        nonlocal running
        running = False
        logger.info("Signal d'arret recu (Ctrl+C)")

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    try:
        while running:
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    raise KafkaException(msg.error())

            process_message(msg.value(), producer, file_logger)

    finally:
        producer.flush(timeout=5)
        consumer.close()
        file_logger.close()
        print_metrics(file_logger)
        logger.info("Consumer arrete proprement.")


# ===========================================================================
#  CLI
# ===========================================================================

# if __name__ == "__main__":
#     parser = argparse.ArgumentParser(
#         description="Consumer Kafka — ISO 8583 Smart Parser + File Logger"
#     )
#     parser.add_argument(
#         "--bootstrap", default="127.0.0.1:9092",
#         help="Kafka bootstrap servers (default: 127.0.0.1:9092)"
#     )
#     parser.add_argument(
#         "--group-id", default="iso8583-parser-group",
#         help="Consumer group ID"
#     )
#     parser.add_argument(
#         "--topic-in", default=TOPIC_RAW,
#         help=f"Topic entree (default: {TOPIC_RAW})"
#     )
#     parser.add_argument(
#         "--log-file", default=DEFAULT_LOG_FILE,
#         help=f"Fichier de sortie JSON (default: {DEFAULT_LOG_FILE})"
#     )
#
#     args = parser.parse_args()
#
#     c_config = {
#         "bootstrap.servers": args.bootstrap,
#         "group.id": args.group_id,
#         "auto.offset.reset": "earliest",
#         "enable.auto.commit": True,
#     }
#     p_config = {
#         "bootstrap.servers": args.bootstrap,
#         "client.id": "iso8583-parser-producer",
#     }
#
#     run_consumer(
#         consumer_config=c_config,
#         producer_config=p_config,
#         topic_in=args.topic_in,
#         log_file=args.log_file,
#     )