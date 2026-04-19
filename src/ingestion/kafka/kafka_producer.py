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
    "bootstrap.servers": "localhost:9092",
    "client.id": "switch-monetique",
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

    # ── T1: VALIDE — Retrait GAB Casablanca 500 MAD ──
    # PAN 4147330000000007 (Luhn ✓) | MTI 0200 | PC 010000 (retrait)
    # Amount 500.00 MAD | Terminal GAB20001 | MCC 6011 | Exp 12/2028
    # Résultat attendu : ✅ CLEAN → Curated Zone
    {
        "label": "VALIDE — Retrait GAB Casablanca 500 MAD",
        "hex": "30323030723c448108e0800031363431343733333030303030303030303730313030303030303030303030353030303030343139313433303030303030303031313433303030303431393238313236303131303531303030363036303037333530303431393030303030314741423230303031424152494442414e4b3030303030314741422042415249442042414e4b2043415341202020202043415341424c414e4341202020204d41353034",
    },

    # ── T2: VALIDE — Achat TPE Marrakech 1250 MAD ──
    # PAN 4147339900000009 (Luhn ✓) | MTI 0200 | PC 000000 (achat)
    # Amount 1250.00 MAD | Terminal TPE30055 | MCC 5411 (grocery) | Exp 08/2029
    # POS entry 071 (contactless chip)
    # Résultat attendu : ✅ CLEAN → Curated Zone
    {
        "label": "VALIDE — Achat TPE Marrakech 1250 MAD",
        "hex": "30323030723c448108e08000313634313437333339393030303030303039303030303030303030303030313235303030303431393135303030303030303030323135303030303034313932393038353431313037313030303630363030373335303034313930303030303254504533303035354341525245464d41524b30303035354341525245464f5552204d41524b4554204755454c495a204d415252414b45434820202020204d41353034",
    },

    # ── T3: REJECT COUCHE 1 — Processing code non-numérique ──
    # DE 3 = "01AB00" → contient des lettres dans un champ numérique
    # Règle déclenchée : NUMERIC_ONLY (couche STRUCTURAL)
    # Résultat attendu : ❌ QUARANTINE dès la couche 1
    {
        "label": "REJECT COUCHE 1 — Processing code non-numerique (01AB00)",
        "hex": "30323030723c448108e0800031363431343733333030303030303030303730314142303030303030303030333030303030343139313630303030303030303033313630303030303431393238313236303131303531303030363036303037333530303431393030303030334741423230303031424152494442414e4b3030303030314741422042415249442042414e4b204d4544494e4120202052414241542020202020202020204d41353034",
    },

    # ── T4: REJECT COUCHE 2 — PAN échoue Luhn + carte expirée ──
    # PAN 4147330000009999 (Luhn ✗) → LUHN_CHECK
    # DE 14 = "2312" → expirée décembre 2023 → CARD_EXPIRED
    # Passe la couche 1, échoue en couche 2 (BUSINESS)
    # Résultat attendu : ❌ QUARANTINE en couche 2 (double erreur)
    {
        "label": "REJECT COUCHE 2 — PAN echoue Luhn + carte expiree 12/2023",
        "hex": "30323030723c448108e0800031363431343733333030303030303939393930313030303030303030303032303030303030343139313730303030303030303034313730303030303431393233313236303131303531303030363036303037333530303431393030303030344741423430303130424152494442414e4b3030303031304741422042415249442042414e4b20414744414c2020202052414241542020202020202020204d41353034",
    },

    # ── T5: REJECT COUCHE 3 — Champs ML critiques absents ──
    # PAN valide, structure OK, business OK
    # MAIS : pas de DE 18 (MCC), pas de DE 41 (terminal_id),
    #        pas de DE 43 (card_acceptor_name_location)
    # Règle déclenchée : ML_FIELD_REQUIRED (couche ML_QUALITY)
    # Résultat attendu : ❌ QUARANTINE en couche 3
    {
        "label": "REJECT COUCHE 3 — Champs ML critiques absents (terminal, MCC, DE43)",
        "hex": "30323030723c00810800800031363431343733333030303030303030303730313030303030303030303030373530303030343139313830303030303030303035313830303030303431393238313230303036303630303733353030343139303030303035353034",
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