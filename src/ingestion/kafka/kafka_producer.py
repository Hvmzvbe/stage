"""
===========================================================================
Module : kafka_producer.py
Projet : Plateforme MLOps - Détection de Fraude (Al Barid Bank)
Auteur : Hamza (PFE EMSI 2025-2026)
---------------------------------------------------------------------------
Rôle   : Producer Kafka — Diffuse des trames ISO 8583 (hex) sur raw_stream.

         Définis tes trames dans le tableau FRAMES_HEX ci-dessous,
         le Producer les envoie une par une sur le topic Kafka.

Usage  :
    python kafka_producer.py
    python kafka_producer.py --delay 2
    python kafka_producer.py --loop

Requires : pip install confluent-kafka
===========================================================================
"""

import argparse
import logging
import time

from confluent_kafka import Producer

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
    "bootstrap.servers": "127.0.0.1:9092",
    "client.id": "switch-monetique-simulator",
}

TOPIC_RAW = "raw_stream"


# ===========================================================================
#  TABLEAU DE TRAMES ISO 8583 (HEX)
#  ─────────────────────────────────
#  Ajoute / modifie / supprime des trames ici.
#  Chaque entrée :
#    - "label" : description (pour le log)
#    - "hex"  : trame brute en hexadécimal
# ===========================================================================

FRAMES_HEX = [

    # ── 1. Retrait GAB Rabat Médina — 100 MAD — spec87 binary ──
    {
        "label": "Retrait GAB Rabat 100 MAD (spec87)",
        "hex": "30323030723c44810ee09000313634313437333331323334353637383930303130303030303030303030303130303030303431333133303030303030303030313133303030303034313332383132363031313035313030303630303733353030343133313330303030303131323334353630304741423130303432424152494442414e4b3030303034324741422042415249442042414e4b204d4544494e4120202020524142415420202020202020204d41353034aabbccdd11223344",
    },

    # ── 2. Retrait GAB Oujda — 200 MAD — spec87 + secondary bitmap ──
    {
        "label": "Retrait GAB Oujda 200 MAD (secondary bitmap)",
        "hex": "30323030f23c46c12ee09000040000000400000031363431343733333132333435363738393030313030303030303030303030323030303030343133313533303030303030303432313533303030303431333239303336303131303531303031303031323036303037333530333334313437333331323334353637383930443239303331303130303030303030303030343133313533303030343237383930313230304741423230303837424152494442414e4b3030303038374741422042415249442042414e4b204f554a444120202020204f554a444120202020202020204d41353034aabb001122334455333031313330303130303132333435363738",
    },

    # ── 3. Achat Jumia E-commerce — 750 MAD — spec87 binary ──
    {
        "label": "Achat Jumia 750 MAD (e-commerce)",
        "hex": "30323030723c44812ee0800031363431343733333132333435363738393030303030303030303030303030373530303030343133323131353330303030373737323131353330303431333238313235393939383132303830363030373335303333343134373333313233343536373839304432383132313031303030303030303030303431333231313533303737393938383737303045434f4d303030314a554d4941303030303030303030314a554d4941204d41524f43205341524c20202020202020202043415341424c414e43412020204d41353034",
    },

    # ── 4. Retrait GAB Rabat — 1500 MAD — spec93 (MTI 1200) ──
    {
        "label": "Retrait GAB Rabat 1500 MAD (spec93)",
        "hex": "31323030723c46c12ee09000313634313437333339393838373736363535303130303030303030303030313530303030303431333134343530303030313233343134343530303034313332393036363031313035313030313030303031323036303037333530333334313437333339393838373736363535443239303631303130303030303030303030343133313434353030333434343535363630304741423630303132424152494442414e4b3030303031324741422042415249442042414e4b20484159204e4148444120524142415420202020202020204d4135303411223344aabbccdd",
    },

    # ── 5. Retrait GAB Fès — 300 MAD — spec03 (MTI 2200) ──
    {
        "label": "Retrait GAB Fes 300 MAD (spec03)",
        "hex": "32323030723c44810ee08000313634313437333338383838393939393030303130303030303030303030303330303030303431333139303030303030303330303139303030303034313333303031363031313035313030303630303733353030343133313930303033303033333334343430304741423430303333424152494442414e4b3030303033334741422042415249442042414e4b2046455320202020202020464553202020202020202020204d41353034",
    },

]


# ===========================================================================
#  PRODUCER
# ===========================================================================

def delivery_callback(err, msg):
    """Callback après chaque envoi Kafka."""
    if err:
        logger.error("ECHEC livraison : %s", err)
    else:
        logger.info(
            "  -> Livre : topic=%s | partition=%d | offset=%d | %d bytes",
            msg.topic(), msg.partition(), msg.offset(), len(msg.value()),
        )


def run_producer(
    kafka_config: dict = None,
    topic: str = TOPIC_RAW,
    delay: float = 1.0,
    loop: bool = False,
):
    """
    Envoie les trames du tableau FRAMES_HEX sur le topic Kafka.
    """
    config = kafka_config or KAFKA_CONFIG
    producer = Producer(config)

    logger.info("=" * 60)
    logger.info("  PRODUCER ISO 8583 DEMARRE")
    logger.info("  Topic        : %s", topic)
    logger.info("  Bootstrap    : %s", config["bootstrap.servers"])
    logger.info("  Trames       : %d", len(FRAMES_HEX))
    logger.info("  Delai        : %.1f sec", delay)
    logger.info("  Mode boucle  : %s", "OUI" if loop else "NON")
    logger.info("=" * 60)

    sent = 0
    round_num = 0

    try:
        while True:
            round_num += 1
            if loop:
                logger.info("--- ROUND %d ---", round_num)

            for i, frame in enumerate(FRAMES_HEX):
                label = frame["label"]
                hex_data = frame["hex"].replace(" ", "").replace("\n", "")

                # Conversion hex → bytes
                try:
                    raw_bytes = bytes.fromhex(hex_data)
                except ValueError as exc:
                    logger.error(
                        "ERREUR HEX invalide [trame #%d] %s : %s",
                        i + 1, label, exc,
                    )
                    continue

                sent += 1
                logger.info(
                    "ENVOI [#%d] %s | %d bytes",
                    sent, label, len(raw_bytes),
                )

                producer.produce(
                    topic=topic,
                    key=None,
                    value=raw_bytes,
                    callback=delivery_callback,
                )
                producer.poll(0)

                time.sleep(delay)

            if not loop:
                break

    except KeyboardInterrupt:
        logger.info("Arret demande (Ctrl+C)")

    finally:
        remaining = producer.flush(timeout=5)
        logger.info("=" * 60)
        logger.info("  PRODUCER ARRETE")
        logger.info("  Trames envoyees : %d", sent)
        logger.info("  Messages restants : %d", remaining)
        logger.info("=" * 60)


# ===========================================================================
#  CLI
# ===========================================================================

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Producer Kafka — Diffuse des trames ISO 8583 (hex)"
    )
    parser.add_argument(
        "--bootstrap", default="127.0.0.1:9092",
        help="Kafka bootstrap servers (default: 127.0.0.1:9092)"
    )
    parser.add_argument(
        "--topic", default=TOPIC_RAW,
        help=f"Topic Kafka (default: {TOPIC_RAW})"
    )
    parser.add_argument(
        "--delay", type=float, default=1.0,
        help="Delai entre chaque trame en secondes (default: 1.0)"
    )
    parser.add_argument(
        "--loop", action="store_true",
        help="Boucle infinie sur le tableau de trames"
    )

    args = parser.parse_args()

    config = {
        "bootstrap.servers": args.bootstrap,
        "client.id": "switch-monetique-simulator",
    }

    run_producer(
        kafka_config=config,
        topic=args.topic,
        delay=args.delay,
        loop=args.loop,
    )