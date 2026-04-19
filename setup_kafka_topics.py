"""
===========================================================================
Module : setup_kafka_topics.py
Projet : Plateforme MLOps - Détection de Fraude (Al Barid Bank)
Auteur : Hamza (PFE EMSI 2025-2026)

Rôle   : Crée et valide les topics Kafka nécessaires pour l'ingestion.

Usage  :
    python setup_kafka_topics.py
    python setup_kafka_topics.py --bootstrap 10.0.0.5:9092
    python setup_kafka_topics.py --check-only
    python setup_kafka_topics.py --describe
===========================================================================
"""

import argparse
import logging
import sys
from typing import List, Dict

try:
    from confluent_kafka.admin import AdminClient, NewTopic
    from confluent_kafka import KafkaError
except ImportError:
    print("ERROR: confluent-kafka not installed")
    print("Install with: pip install confluent-kafka")
    sys.exit(1)

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(name)s | %(message)s",
)
logger = logging.getLogger("kafka_setup")

# ---------------------------------------------------------------------------
# Configuration des Topics
# ---------------------------------------------------------------------------

TOPICS_CONFIG: Dict[str, Dict] = {
    "raw_stream": {
        "partitions": 3,
        "replication_factor": 1,
        "config": {
            "retention.ms": str(24 * 60 * 60 * 1000),  # 24h
            "compression.type": "snappy",
            "cleanup.policy": "delete",
        },
        "description": "Flux brut ISO 8583 depuis le Switch",
    },
    
    "cleaned_transactions": {
        "partitions": 3,
        "replication_factor": 1,
        "config": {
            "retention.ms": str(7 * 24 * 60 * 60 * 1000),  # 7 jours
            "compression.type": "snappy",
            "cleanup.policy": "delete",
        },
        "description": "Transactions parsées, enrichies, CLEAN",
    },
    
    "quarantine_transactions": {
        "partitions": 2,
        "replication_factor": 1,
        "config": {
            "retention.ms": str(30 * 24 * 60 * 60 * 1000),  # 30 jours
            "compression.type": "snappy",
            "cleanup.policy": "delete",
        },
        "description": "Transactions rejetées par le Validator (debug)",
    },
    
    "parsing_errors": {
        "partitions": 1,
        "replication_factor": 1,
        "config": {
            "retention.ms": str(7 * 24 * 60 * 60 * 1000),  # 7 jours
            "cleanup.policy": "delete",
        },
        "description": "Erreurs de parsing ISO 8583",
    },
}

# ---------------------------------------------------------------------------
# Fonctions
# ---------------------------------------------------------------------------

def create_admin_client(bootstrap_servers: str) -> AdminClient:
    """Crée un client admin Kafka."""
    config = {"bootstrap.servers": bootstrap_servers}
    return AdminClient(config)

def list_existing_topics(admin: AdminClient) -> List[str]:
    """Liste tous les topics existants."""
    try:
        metadata = admin.list_topics(timeout=10)
        return list(metadata.topics.keys())
    except Exception as exc:
        logger.error("Erreur lors de la lecture des topics : %s", exc)
        return []

def create_topics(admin: AdminClient, bootstrap_servers: str) -> bool:
    """Crée les topics manquants."""
    existing = list_existing_topics(admin)
    logger.info("Topics existants : %s", ", ".join(existing) if existing else "aucun")
    
    topics_to_create = []
    
    for topic_name, config in TOPICS_CONFIG.items():
        if topic_name in existing:
            logger.info("✓ Topic '%s' existe déjà", topic_name)
            continue
        
        logger.info("Création du topic '%s'...", topic_name)
        
        new_topic = NewTopic(
            topic=topic_name,
            num_partitions=config["partitions"],
            replication_factor=config["replication_factor"],
            config=config["config"],
        )
        topics_to_create.append(new_topic)
    
    if not topics_to_create:
        logger.info("✓ Tous les topics existent déjà !")
        return True
    
    # Créer les topics
    try:
        result_dict = admin.create_topics(topics_to_create, validate_only=False)
        
        for topic, future in result_dict.items():
            try:
                future.result(timeout=30)
                logger.info("✓ Topic '%s' créé avec succès", topic)
            except Exception as exc:
                logger.error("✗ Erreur création '%s' : %s", topic, exc)
                return False
        
        return True
    except Exception as exc:
        logger.error("Erreur lors de la création des topics : %s", exc)
        return False

def describe_topics(admin: AdminClient) -> bool:
    """Affiche les détails des topics (partitions, rétention, etc)."""
    logger.info("=" * 70)
    logger.info("  CONFIGURATION DES TOPICS")
    logger.info("=" * 70)
    
    try:
        metadata = admin.list_topics(timeout=10)
        
        for topic_name in sorted(TOPICS_CONFIG.keys()):
            topic_metadata = metadata.topics.get(topic_name)
            
            if not topic_metadata:
                logger.warning("  Topic '%s' NON TROUVÉ", topic_name)
                continue
            
            partitions = topic_metadata.partitions
            logger.info(f"\n  ✓ {topic_name}")
            logger.info(f"    Description   : {TOPICS_CONFIG[topic_name]['description']}")
            logger.info(f"    Partitions     : {len(partitions)}")
            logger.info(f"    Replication    : {TOPICS_CONFIG[topic_name]['replication_factor']}")
            
            for partition_id, partition in partitions.items():
                logger.info(
                    f"      Partition {partition_id} — Leader: {partition.leader}, "
                    f"Replicas: {partition.replicas}, ISR: {partition.isrs}"
                )
        
        return True
    except Exception as exc:
        logger.error("Erreur lors de la description : %s", exc)
        return False

def validate_connectivity(bootstrap_servers: str) -> bool:
    """Teste la connexion à Kafka."""
    logger.info("Test de connexion à Kafka (%s)...", bootstrap_servers)
    try:
        admin = create_admin_client(bootstrap_servers)
        metadata = admin.list_topics(timeout=10)
        logger.info("✓ Connexion OK — %d topics existants", len(metadata.topics))
        return True
    except Exception as exc:
        logger.error("✗ Erreur de connexion : %s", exc)
        logger.error("Vérifie que Kafka est lancé : docker-compose up -d")
        return False

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Setup des topics Kafka pour Al Barid Bank",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Exemples :
  python setup_kafka_topics.py                           # Setup complet
  python setup_kafka_topics.py --bootstrap 10.0.0.5:9092 # Kafka distant
  python setup_kafka_topics.py --check-only              # Vérif sans créer
  python setup_kafka_topics.py --describe                # Affiche config
        """,
    )
    parser.add_argument(
        "--bootstrap",
        default="127.0.0.1:9092",
        help="Kafka bootstrap servers (default: 127.0.0.1:9092)",
    )
    parser.add_argument(
        "--check-only",
        action="store_true",
        help="Vérifie la connexion sans créer les topics",
    )
    parser.add_argument(
        "--describe",
        action="store_true",
        help="Affiche les détails des topics",
    )
    
    args = parser.parse_args()
    bootstrap = args.bootstrap
    
    logger.info("=" * 70)
    logger.info("  KAFKA TOPIC SETUP — Al Barid Bank")
    logger.info("=" * 70)
    logger.info("  Bootstrap  : %s", bootstrap)
    logger.info("  Topics     : %d à configurer", len(TOPICS_CONFIG))
    logger.info("=" * 70)
    
    # 1. Test de connexion
    if not validate_connectivity(bootstrap):
        sys.exit(1)
    
    admin = create_admin_client(bootstrap)
    
    # 2. Check-only mode
    if args.check_only:
        existing = list_existing_topics(admin)
        print()
        print(f"Topics actuels : {', '.join(existing) if existing else 'aucun'}")
        return
    
    # 3. Création des topics
    if not create_topics(admin, bootstrap):
        logger.error("✗ Erreur lors de la création des topics")
        sys.exit(1)
    
    # 4. Describe
    if args.describe or True:  # Toujours afficher après création
        print()
        describe_topics(admin)
    
    logger.info("\n✓ Setup Kafka terminé avec succès !")

if __name__ == "__main__":
    main()
