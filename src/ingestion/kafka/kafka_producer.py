"""
===========================================================================
Module : kafka_producer.py
Projet : Plateforme MLOps - Détection de Fraude (Al Barid Bank)
---------------------------------------------------------------------------
Rôle   : Producer Kafka — Simule le Switch Monétique.
         Génère des trames ISO 8583 brutes et les publie sur le topic
         "raw_stream" (Ingestion Layer).

Usage  :
    python kafka_producer.py                    # Mode continu (1 trame/sec)
    python kafka_producer.py --count 10         # 10 trames puis stop
    python kafka_producer.py --burst            # Rafale rapide (stress test)

Requires : pip install confluent-kafka pyiso8583
===========================================================================
"""

import argparse
#import json
import logging
import random
import time
from datetime import datetime

from confluent_kafka import Producer

import iso8583
from iso8583.specs import default as spec87_binary

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(name)s | %(message)s",
)
logger = logging.getLogger("kafka_producer")

# ---------------------------------------------------------------------------
# Configuration Kafka
# ---------------------------------------------------------------------------
KAFKA_CONFIG = {
    "bootstrap.servers": "localhost:9092",
    "client.id": "switch-monetique-simulator",
}

TOPIC_RAW = "raw_stream"

# ---------------------------------------------------------------------------
# Données de simulation (scénarios Al Barid Bank)
# ---------------------------------------------------------------------------
SCENARIOS = [
    {
        "label": "Retrait GAB Rabat",
        "mti": "0200", "processing_code": "010000",
        "mcc": "6011", "pos_entry_mode": "051", "pos_condition": "00",
        "terminal_id": "GAB10042", "merchant_id": "BARIDBANK000042",
        "location": "GAB BARID BANK MEDINA    RABAT        MA",
        "amount_range": (20000, 200000),  # 200 - 2000 MAD (en centimes)
        "has_pin": True,
    },
    {
        "label": "Retrait GAB Casablanca",
        "mti": "0200", "processing_code": "010000",
        "mcc": "6011", "pos_entry_mode": "051", "pos_condition": "00",
        "terminal_id": "GAB10088", "merchant_id": "BARIDBANK000088",
        "location": "GAB BARID BANK MAARIF    CASABLANCA   MA",
        "amount_range": (20000, 500000),
        "has_pin": True,
    },
    {
        "label": "Retrait GAB Oujda",
        "mti": "0200", "processing_code": "010000",
        "mcc": "6011", "pos_entry_mode": "051", "pos_condition": "00",
        "terminal_id": "GAB20087", "merchant_id": "BARIDBANK000087",
        "location": "GAB BARID BANK OUJDA     OUJDA        MA",
        "amount_range": (10000, 300000),
        "has_pin": True,
    },
    {
        "label": "Achat TPE Marjane",
        "mti": "0200", "processing_code": "000000",
        "mcc": "5411", "pos_entry_mode": "051", "pos_condition": "00",
        "terminal_id": "TPE30100", "merchant_id": "MARJANE00000100",
        "location": "MARJANE HAY RIAD         RABAT        MA",
        "amount_range": (5000, 300000),
        "has_pin": True,
    },
    {
        "label": "Achat E-commerce Jumia",
        "mti": "0200", "processing_code": "000000",
        "mcc": "5999", "pos_entry_mode": "812", "pos_condition": "08",
        "terminal_id": "ECOM0001", "merchant_id": "JUMIA0000000001",
        "location": "JUMIA MAROC SARL         CASABLANCA   MA",
        "amount_range": (10000, 500000),
        "has_pin": False,
    },
    {
        "label": "Achat E-commerce Amazon",
        "mti": "0200", "processing_code": "000000",
        "mcc": "5999", "pos_entry_mode": "812", "pos_condition": "08",
        "terminal_id": "ECOM0002", "merchant_id": "AMAZON000000002",
        "location": "AMAZON EU SARL           LUXEMBOURG   LU",
        "amount_range": (5000, 1000000),
        "has_pin": False,
    },
    # --- Scénarios suspects (pour tester la détection de fraude) ---
    {
        "label": "SUSPECT - Bijouterie 3h du matin",
        "mti": "0200", "processing_code": "000000",
        "mcc": "5944", "pos_entry_mode": "051", "pos_condition": "00",
        "terminal_id": "TPE99001", "merchant_id": "BIJOUX000099001",
        "location": "BIJOUTERIE ATLAS         CASABLANCA   MA",
        "amount_range": (500000, 2000000),  # Gros montants
        "has_pin": True,
        "force_hour": 3,  # 3h du matin
    },
    {
        "label": "SUSPECT - Micro-montant (card testing)",
        "mti": "0200", "processing_code": "000000",
        "mcc": "5999", "pos_entry_mode": "812", "pos_condition": "08",
        "terminal_id": "ECOM0099", "merchant_id": "TESTSHOP0000099",
        "location": "UNKNOWN SHOP             UNKNOWN      XX",
        "amount_range": (100, 5000),  # 1 - 50 MAD (micro-montants)
        "has_pin": False,
    },
]

# Pool de PANs simulés
PAN_POOL = [
    "4147331234567890",
    "4147339988776655",
    "4147335555666677",
    "4147330000111122",
    "4147338888999900",
    "4147337777888899",
]


def generate_iso_frame(scenario: dict, pan: str = None) -> bytearray:
    """
    Génère une trame ISO 8583 brute à partir d'un scénario.

    Parameters
    ----------
    scenario : dict
        Dictionnaire décrivant le type de transaction.
    pan : str, optional
        PAN spécifique, sinon aléatoire.

    Returns
    -------
    bytearray
        Trame ISO 8583 encodée (prête pour Kafka).
    """
    now = datetime.now()

    # Heure forcée pour scénarios suspects
    if "force_hour" in scenario:
        hour = scenario["force_hour"]
        time_str = f"{hour:02d}{random.randint(0,59):02d}{random.randint(0,59):02d}"
    else:
        time_str = now.strftime("%H%M%S")

    date_str = now.strftime("%m%d")
    pan = pan or random.choice(PAN_POOL)
    amount = random.randint(*scenario["amount_range"])
    stan = f"{random.randint(1, 999999):06d}"

    doc = {
        "t":  scenario["mti"],
        "p":  "",
        "2":  pan,
        "3":  scenario["processing_code"],
        "4":  f"{amount:012d}",
        "7":  f"{date_str}{time_str}",
        "11": stan,
        "12": time_str,
        "13": date_str,
        "14": "2812",
        "18": scenario["mcc"],
        "22": scenario["pos_entry_mode"],
        "25": scenario["pos_condition"],
        "32": "007350",
        "37": f"{date_str}{time_str}{stan[:2]}",
        "38": f"{random.randint(100000, 999999)}",
        "39": "00",
        "41": scenario["terminal_id"],
        "42": scenario["merchant_id"],
        "43": scenario["location"],
        "49": "504",
    }

    if scenario["has_pin"]:
        doc["52"] = f"{random.randint(0, 0xFFFFFFFFFFFFFFFF):016X}"

    raw, _ = iso8583.encode(doc, spec87_binary)
    return raw


def delivery_callback(err, msg):
    """Callback appelé après chaque envoi Kafka."""
    if err:
        logger.error("ECHEC livraison : %s", err)
    else:
        logger.info(
            "Livré → topic=%s | partition=%s | offset=%s | size=%d bytes",
            msg.topic(), msg.partition(), msg.offset(), len(msg.value()),
        )


def run_producer(
    kafka_config: dict = None,
    topic: str = TOPIC_RAW,
    count: int = 0,
    delay: float = 1.0,
    burst: bool = False,
):
    """
    Boucle principale du Producer.

    Parameters
    ----------
    kafka_config : dict
        Configuration Kafka (bootstrap.servers, etc.).
    topic : str
        Topic Kafka cible.
    count : int
        Nombre de trames à envoyer (0 = infini).
    delay : float
        Délai entre chaque trame (secondes).
    burst : bool
        Si True, envoie sans délai (stress test).
    """
    config = kafka_config or KAFKA_CONFIG
    producer = Producer(config)

    logger.info("Producer démarré — topic=%s | bootstrap=%s",
                topic, config["bootstrap.servers"])

    sent = 0
    try:
        while True:
            # Choix aléatoire du scénario (pondéré : plus de transactions normales)
            weights = [20, 20, 15, 15, 10, 10, 5, 5]
            scenario = random.choices(SCENARIOS, weights=weights, k=1)[0]

            # Génération de la trame
            raw = generate_iso_frame(scenario)
            sent += 1

            logger.info(
                "[#%d] %s | %d bytes",
                sent, scenario["label"], len(raw),
            )

            # Publication sur Kafka
            producer.produce(
                topic=topic,
                key=None,
                value=bytes(raw),
                callback=delivery_callback,
            )
            producer.poll(0)

            # Condition d'arrêt
            if count > 0 and sent >= count:
                logger.info("Limite atteinte (%d trames). Arrêt.", count)
                break

            # Délai
            if not burst:
                time.sleep(delay)

    except KeyboardInterrupt:
        logger.info("Arrêt demandé par l'utilisateur (Ctrl+C)")
    finally:
        remaining = producer.flush(timeout=5)
        logger.info("Producer arrêté. Messages en attente : %d", remaining)



if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Producer Kafka — Switch Monétique Simulator"
    )
    parser.add_argument(
        "--bootstrap", default="localhost:9092",
        help="Kafka bootstrap servers (default: localhost:9092)"
    )
    parser.add_argument(
        "--topic", default=TOPIC_RAW,
        help=f"Topic Kafka (default: {TOPIC_RAW})"
    )
    parser.add_argument(
        "--count", type=int, default=0,
        help="Nombre de trames à envoyer (0 = infini)"
    )
    parser.add_argument(
        "--delay", type=float, default=1.0,
        help="Délai entre chaque trame en secondes (default: 1.0)"
    )
    parser.add_argument(
        "--burst", action="store_true",
        help="Mode rafale (pas de délai)"
    )

    args = parser.parse_args()

    config = {
        "bootstrap.servers": args.bootstrap,
        "client.id": "switch-monetique-simulator",
    }

    run_producer(
        kafka_config=config,
        topic=args.topic,
        count=args.count,
        delay=args.delay,
        burst=args.burst,
    )
