"""
===========================================================================
Module : main.py
Projet : Plateforme MLOps - Détection de Fraude (Al Barid Bank)
Auteur : Hamza (PFE EMSI 2025-2026)
---------------------------------------------------------------------------
Rôle   : ORCHESTRATEUR DE L'INGESTION LAYER — Point d'entrée unique.

         Ce fichier est le CŒUR du pipeline. Il câble tous les composants
         pour qu'un seul lancement fasse tourner le flux complet :

           Kafka raw_stream
                │
           ┌────▼─────┐
           │ PARSER    │  iso8583_smart_parser.parse_iso_to_dict()
           └────┬──────┘
                │  (Parse Error? → topic parsing_errors)
           ┌────▼──────┐
           │ RAW ZONE   │  Stockage brut IMMUABLE (toujours, même si erreur)
           └────┬──────┘
                │
           ┌────▼──────────┐
           │ VALIDATOR     │  transaction_validator.validate() — 3 couches
           └────┬──────────┘
                │
           ┌────▼────┐           ┌──────────────┐
           │ CLEAN?  │── Non ──▶ │ QUARANTINE    │ JSONL + Kafka topic
           └────┬────┘           └──────────────┘
                │ Oui
           ┌────▼──────────┐
           │ ENRICHMENT    │  enrich_parsed_data()
           └────┬──────────┘
                │
           ┌────▼──────────────────────────┐
           │ CURATED ZONE                  │ Parquet (Spark) + Kafka topic
           └───────────────────────────────┘

Usage  :
    python main.py                              # Mode Kafka (production)
    python main.py --loop-test                  # Mode test local sans Kafka
    python main.py --bootstrap 10.0.0.5:9092    # Kafka distant
    python main.py --data-dir /hdfs/fraud/data  # Répertoire HDFS

Requires : pip install confluent-kafka pyiso8583 pyarrow
===========================================================================
"""

import argparse
import json
import logging
import signal
import uuid
from datetime import datetime
from pathlib import Path
from typing import Optional

# ── Confluent Kafka ──
from confluent_kafka import Consumer, Producer, KafkaError, KafkaException

# ── Imports internes (MÊME DOSSIER) ──
from iso8583_smart_parser import parse_iso_to_dict
from kafka.kafka_consumer import enrich_parsed_data
from kafka.kafka_producer import FRAMES_HEX
from transaction_validator import (
    TransactionValidator,
    ValidationResult,
    enrich_with_validation,
)
import iso8583

# ── PyArrow pour écriture Parquet ──
try:
    import pyarrow as pa
    import pyarrow.parquet as pq
    PARQUET_AVAILABLE = True
except ImportError:
    PARQUET_AVAILABLE = False


# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(name)s | %(message)s",
)
logger = logging.getLogger("main")


# ===========================================================================
#  CONFIGURATION CENTRALISÉE
# ===========================================================================

class Config:
    """Configuration unique de tout le pipeline d'ingestion."""

    def __init__(
        self,
        bootstrap: str = "127.0.0.1:9092",
        group_id: str = "ingestion-pipeline-group",
        data_dir: str = "./data",
        parquet_batch: int = 50,
    ):
        self.bootstrap = bootstrap
        self.group_id = group_id
        self.parquet_batch = parquet_batch

        # ── Topics Kafka ──
        self.TOPIC_RAW = "raw_stream"
        self.TOPIC_CLEANED = "cleaned_transactions"
        self.TOPIC_QUARANTINE = "quarantine_transactions"
        self.TOPIC_ERRORS = "parsing_errors"

        # ── Répertoires Data Lakehouse ──
        base = Path(data_dir)
        self.raw_zone_dir = base / "raw_zone"
        self.curated_zone_dir = base / "curated_zone"
        self.quarantine_zone_dir = base / "quarantine_zone"

        # Créer les dossiers
        for d in (self.raw_zone_dir, self.curated_zone_dir, self.quarantine_zone_dir):
            d.mkdir(parents=True, exist_ok=True)

    @property
    def consumer_config(self) -> dict:
        return {
            "bootstrap.servers": self.bootstrap,
            "group.id": self.group_id,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": True,
            "session.timeout.ms": 30000,
        }

    @property
    def producer_config(self) -> dict:
        return {
            "bootstrap.servers": self.bootstrap,
            "client.id": "ingestion-pipeline-producer",
        }


# ===========================================================================
#  WRITERS — Écriture dans les 3 Zones du Data Lakehouse
# ===========================================================================

class RawZoneWriter:
    """
    Raw Zone — Stockage IMMUABLE de chaque trame brute (hex).
    Écrit AVANT tout traitement : même si le parsing échoue,
    on garde la trame pour investigation.
    """

    def __init__(self, directory: Path):
        self.filepath = directory / "raw_frames.jsonl"
        self._f = open(self.filepath, "a", encoding="utf-8")
        self.count = 0

    def write(self, raw_bytes: bytes, extra: dict = None):
        record = {
            "_raw_id": str(uuid.uuid4()),
            "_raw_hex": raw_bytes.hex(),
            "_raw_len": len(raw_bytes),
            "_ingested_at": datetime.now().isoformat(),
        }
        if extra:
            record.update(extra)
        self._f.write(json.dumps(record, ensure_ascii=False) + "\n")
        self._f.flush()
        self.count += 1

    def close(self):
        if not self._f.closed:
            self._f.close()


class QuarantineWriter:
    """
    Quarantine Zone — Trames rejetées par le Validator.
    Contient le motif de rejet pour investigation par l'équipe support.
    """

    def __init__(self, directory: Path):
        self.filepath = directory / "quarantine.jsonl"
        self._f = open(self.filepath, "a", encoding="utf-8")
        self.count = 0

    def write(self, parsed: dict, result: ValidationResult):
        record = {
            **parsed,
            **result.to_dict(),
            "_quarantined_at": datetime.now().isoformat(),
        }
        self._f.write(json.dumps(record, ensure_ascii=False, default=str) + "\n")
        self._f.flush()
        self.count += 1

    def close(self):
        if not self._f.closed:
            self._f.close()


class CuratedWriter:
    """
    Curated Zone — Trames CLEAN, enrichies, prêtes pour le ML.
    Double écriture :
      - Parquet sur disque (pour Spark / Feature Engineering)
      - Kafka topic cleaned_transactions (pour l'Online API scoring)
    """

    def __init__(self, directory: Path, producer: Optional[Producer], topic: str, batch_size: int):
        self.directory = directory
        self.producer = producer
        self.topic = topic
        self.batch_size = batch_size
        self._buffer: list[dict] = []
        self.count = 0
        self.parquet_files: list[str] = []

    def write(self, record: dict):
        """Publie sur Kafka immédiatement + bufferise pour Parquet."""
        # ── Kafka (temps réel) ──
        if self.producer:
            key = record.get("pan", "UNKNOWN")
            self.producer.produce(
                topic=self.topic,
                key=(key if isinstance(key, str) else "UNKNOWN").encode("utf-8"),
                value=json.dumps(record, ensure_ascii=False, default=str).encode("utf-8"),
            )
            self.producer.poll(0)

        # ── Buffer Parquet ──
        self._buffer.append(self._flatten(record))
        self.count += 1

        if len(self._buffer) >= self.batch_size:
            self._flush_parquet()

    @staticmethod
    def _flatten(data: dict) -> dict:
        """Aplatit les dict/list imbriqués en JSON string pour Parquet."""
        flat = {}
        for k, v in data.items():
            if isinstance(v, (dict, list, tuple)):
                flat[k] = json.dumps(v, ensure_ascii=False, default=str)
            else:
                flat[k] = v
        return flat

    def _flush_parquet(self):
        if not self._buffer:
            return

        ts = datetime.now().strftime("%Y%m%d_%H%M%S")

        if PARQUET_AVAILABLE:
            path = self.directory / f"transactions_{ts}.parquet"
            try:
                table = pa.Table.from_pylist(self._buffer)
                pq.write_table(table, str(path), compression="snappy")
                self.parquet_files.append(str(path))
                logger.info("PARQUET FLUSH : %d lignes → %s", len(self._buffer), path.name)
            except Exception as exc:
                logger.error("ERREUR Parquet : %s — fallback JSONL", exc)
                self._flush_jsonl(ts)
        else:
            self._flush_jsonl(ts)

        self._buffer.clear()

    def _flush_jsonl(self, ts: str):
        path = self.directory / f"transactions_{ts}.jsonl"
        with open(path, "a", encoding="utf-8") as f:
            for rec in self._buffer:
                f.write(json.dumps(rec, ensure_ascii=False, default=str) + "\n")
        logger.info("JSONL FLUSH : %d lignes → %s", len(self._buffer), path.name)

    def flush_remaining(self):
        """Flush ce qui reste dans le buffer (appelé à l'arrêt)."""
        if self._buffer:
            self._flush_parquet()


# ===========================================================================
#  MÉTRIQUES DU PIPELINE
# ===========================================================================

class Metrics:
    """Compteurs centralisés — prêts pour Prometheus/Grafana."""

    def __init__(self):
        self.started_at: Optional[datetime] = None
        self.received = 0
        self.parsed_ok = 0
        self.parse_errors = 0
        self.clean = 0
        self.quarantine = 0
        self.warnings = 0
        self.total_mad = 0.0
        self.by_spec: dict[str, int] = {}
        self.by_channel: dict[str, int] = {}
        self.by_rule: dict[str, int] = {}

    def print_report(self, raw: RawZoneWriter, curated: CuratedWriter, quar: QuarantineWriter):
        elapsed = (datetime.now() - self.started_at).total_seconds() if self.started_at else 0
        tps = self.parsed_ok / max(elapsed, 1)
        total_validated = self.clean + self.quarantine

        print()
        print("=" * 70)
        print("  RAPPORT FINAL — INGESTION PIPELINE")
        print("=" * 70)
        print(f"  Durée               : {elapsed:.1f} sec ({tps:.1f} trames/sec)")
        print()
        print(f"  Reçues (Kafka)      : {self.received}")
        print(f"  Parsées OK          : {self.parsed_ok}")
        print(f"  Erreurs parsing     : {self.parse_errors}")
        print()
        print(f"  Validées CLEAN      : {self.clean}")
        print(f"  Envoyées QUARANTINE : {self.quarantine}")
        clean_pct = (self.clean / max(total_validated, 1)) * 100
        print(f"  Taux de conformité  : {clean_pct:.1f}%")
        print(f"  Warnings            : {self.warnings}")
        print(f"  Montant total       : {self.total_mad:,.2f} MAD")
        print()
        print("  ── STOCKAGE ──")
        print(f"  Raw Zone            : {raw.count} trames → {raw.filepath}")
        print(f"  Curated Zone        : {curated.count} trames")
        for pf in curated.parquet_files:
            print(f"    → {pf}")
        print(f"  Quarantine Zone     : {quar.count} trames → {quar.filepath}")
        print()
        if self.by_spec:
            print("  ── PAR SPEC ISO ──")
            for s, c in sorted(self.by_spec.items()):
                print(f"    {s:<22s} : {c}")
        if self.by_channel:
            print("  ── PAR CANAL ──")
            for ch, c in sorted(self.by_channel.items()):
                print(f"    {ch:<22s} : {c}")
        if self.by_rule:
            print("  ── REJETS PAR RÈGLE ──")
            for r, c in sorted(self.by_rule.items(), key=lambda x: -x[1]):
                print(f"    {r:<30s} : {c}")
        print("=" * 70)


# ===========================================================================
#  CŒUR DU PIPELINE — Traitement d'un message
# ===========================================================================

def process_frame(
    raw_bytes: bytes,
    validator: TransactionValidator,
    raw_w: RawZoneWriter,
    curated_w: CuratedWriter,
    quarantine_w: QuarantineWriter,
    error_producer: Optional[Producer],
    config: Config,
    metrics: Metrics,
) -> bool:
    """
    Pipeline complet pour UNE trame brute ISO 8583.

    Étapes :
      1. Raw Zone     ← copie immuable (TOUJOURS, avant tout)
      2. Smart Parser ← décodage ISO 8583
      3. Validator    ← quality check 3 couches
      4. Routing      ← Curated Zone (enrichi) ou Quarantine Zone
    """
    metrics.received += 1
    frame_num = metrics.received

    # ══════════════════════════════════════════
    #  1. RAW ZONE — copie brute IMMUABLE
    # ══════════════════════════════════════════
    raw_w.write(raw_bytes, {"_frame_num": frame_num})

    # ══════════════════════════════════════════
    #  2. PARSER ISO 8583
    # ══════════════════════════════════════════
    try:
        parsed = parse_iso_to_dict(raw_bytes)
        spec = parsed.get("_spec_detected", "unknown")
        metrics.parsed_ok += 1
        metrics.by_spec[spec] = metrics.by_spec.get(spec, 0) + 1

    except (iso8583.DecodeError, ValueError) as exc:
        metrics.parse_errors += 1
        logger.error("PARSE ERROR [#%d] %s", frame_num, exc)

        if error_producer:
            error_producer.produce(
                topic=config.TOPIC_ERRORS,
                value=json.dumps({
                    "error": str(exc),
                    "raw_hex": raw_bytes.hex()[:200],
                    "raw_len": len(raw_bytes),
                    "timestamp": datetime.now().isoformat(),
                }).encode("utf-8"),
            )
            error_producer.poll(0)
        return False

    except Exception as exc:
        metrics.parse_errors += 1
        logger.error("PARSE ERROR INATTENDUE [#%d] %s", frame_num, exc, exc_info=True)
        return False

    # ══════════════════════════════════════════
    #  3. VALIDATOR — Quality Check 3 couches
    # ══════════════════════════════════════════
    result = validator.validate(parsed)

    if result.warnings:
        metrics.warnings += len(result.warnings)

    # ══════════════════════════════════════════
    #  4. ROUTING — CLEAN vs QUARANTINE
    # ══════════════════════════════════════════

    pan = parsed.get("pan", "?")
    pan_masked = f"{pan[:6]}****{pan[-4:]}" if pan and len(pan) > 8 else pan or "?"

    if not result.is_valid:
        # ── QUARANTINE ──
        metrics.quarantine += 1
        for e in result.errors:
            metrics.by_rule[e.rule] = metrics.by_rule.get(e.rule, 0) + 1

        quarantine_w.write(parsed, result)

        if error_producer:
            error_producer.produce(
                topic=config.TOPIC_QUARANTINE,
                key=pan.encode("utf-8") if isinstance(pan, str) else b"UNKNOWN",
                value=json.dumps(
                    enrich_with_validation(parsed, result),
                    ensure_ascii=False, default=str,
                ).encode("utf-8"),
            )
            error_producer.poll(0)

        logger.warning(
            "QUARANTINE [#%d] %s | %s | %s | %s",
            frame_num, result.layer_failed,
            parsed.get("mti", "?"), pan_masked,
            "; ".join(e.rule for e in result.errors),
        )
        return False

    # ── CLEAN → Enrichissement + Curated Zone ──
    enriched = enrich_parsed_data(parsed)
    final = {**enriched, **result.to_dict()}

    curated_w.write(final)

    channel = enriched.get("_channel", "OTHER")
    amount = enriched.get("_amount_mad", 0.0) or 0.0
    metrics.clean += 1
    metrics.total_mad += amount
    metrics.by_channel[channel] = metrics.by_channel.get(channel, 0) + 1

    warn_tag = f" ({len(result.warnings)}w)" if result.warnings else ""
    logger.info(
        "CLEAN [#%d] %s | %s | %s | %.2f MAD | %s | %s%s",
        frame_num, spec, parsed.get("mti", "?"),
        pan_masked, amount, channel,
        enriched.get("_city", "?"), warn_tag,
    )
    return True


# ===========================================================================
#  MODE 1 — KAFKA (Production)
# ===========================================================================

def run_kafka(config: Config):
    """
    Boucle Kafka complète.
    Lance : python main.py
    Prérequis : Kafka lancé + producer qui envoie sur raw_stream.
    """
    consumer = Consumer(config.consumer_config)
    producer = Producer(config.producer_config)
    error_producer = Producer(config.producer_config)
    validator = TransactionValidator()
    metrics = Metrics()

    raw_w = RawZoneWriter(config.raw_zone_dir)
    curated_w = CuratedWriter(config.curated_zone_dir, producer, config.TOPIC_CLEANED, config.parquet_batch)
    quarantine_w = QuarantineWriter(config.quarantine_zone_dir)

    consumer.subscribe([config.TOPIC_RAW])
    metrics.started_at = datetime.now()

    logger.info("=" * 70)
    logger.info("  INGESTION PIPELINE — AL BARID BANK (Kafka)")
    logger.info("=" * 70)
    logger.info("  Bootstrap        : %s", config.bootstrap)
    logger.info("  Group ID         : %s", config.group_id)
    logger.info("  Topic IN         : %s", config.TOPIC_RAW)
    logger.info("  Topic CLEAN      : %s", config.TOPIC_CLEANED)
    logger.info("  Topic QUARANTINE : %s", config.TOPIC_QUARANTINE)
    logger.info("  Topic ERRORS     : %s", config.TOPIC_ERRORS)
    logger.info("  Raw Zone         : %s", config.raw_zone_dir)
    logger.info("  Curated Zone     : %s", config.curated_zone_dir)
    logger.info("  Quarantine Zone  : %s", config.quarantine_zone_dir)
    logger.info("  Parquet batch    : %d", config.parquet_batch)
    logger.info("  PyArrow          : %s", "OUI" if PARQUET_AVAILABLE else "NON (fallback JSONL)")
    logger.info("=" * 70)
    logger.info("En attente de trames sur [%s]...", config.TOPIC_RAW)

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

            process_frame(
                msg.value(), validator, raw_w, curated_w,
                quarantine_w, error_producer, config, metrics,
            )

    finally:
        curated_w.flush_remaining()
        producer.flush(timeout=10)
        error_producer.flush(timeout=10)
        consumer.close()
        raw_w.close()
        quarantine_w.close()

        metrics.print_report(raw_w, curated_w, quarantine_w)
        validator.print_metrics()
        logger.info("Pipeline arrêté proprement.")


# ===========================================================================
#  MODE 2 — TEST LOCAL (sans Kafka)
# ===========================================================================

def run_local_test(config: Config):
    """
    Simule le pipeline complet avec les trames du kafka_producer.py.
    Lance : python main.py --loop-test
    Aucun serveur Kafka requis.
    """
    validator = TransactionValidator()
    metrics = Metrics()
    metrics.started_at = datetime.now()

    raw_w = RawZoneWriter(config.raw_zone_dir)
    curated_w = CuratedWriter(config.curated_zone_dir, None, config.TOPIC_CLEANED, config.parquet_batch)
    quarantine_w = QuarantineWriter(config.quarantine_zone_dir)

    logger.info("=" * 70)
    logger.info("  INGESTION PIPELINE — MODE TEST LOCAL (sans Kafka)")
    logger.info("  Trames à traiter : %d (depuis kafka_producer.FRAMES_HEX)", len(FRAMES_HEX))
    logger.info("  Data dir         : %s", config.raw_zone_dir.parent)
    logger.info("  PyArrow          : %s", "OUI" if PARQUET_AVAILABLE else "NON (fallback JSONL)")
    logger.info("=" * 70)

    for i, frame in enumerate(FRAMES_HEX):
        label = frame["label"]
        hex_data = frame["hex"].replace(" ", "").replace("\n", "")

        try:
            raw_bytes = bytes.fromhex(hex_data)
        except ValueError as exc:
            logger.error("HEX invalide [#%d] %s : %s", i + 1, label, exc)
            continue

        logger.info("━" * 55)
        logger.info("TRAME [#%d] %s (%d bytes)", i + 1, label, len(raw_bytes))

        process_frame(
            raw_bytes, validator, raw_w, curated_w,
            quarantine_w, None, config, metrics,
        )

    # Flush
    curated_w.flush_remaining()
    raw_w.close()
    quarantine_w.close()

    metrics.print_report(raw_w, curated_w, quarantine_w)
    validator.print_metrics()


# ===========================================================================
#  CLI — POINT D'ENTRÉE
# ===========================================================================

if __name__ == "__main__":
    ap = argparse.ArgumentParser(
        description="Ingestion Pipeline — Al Barid Bank Fraud Detection",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Exemples :
  python main.py                                  # Kafka (production)
  python main.py --loop-test                      # Test local sans Kafka
  python main.py --bootstrap 10.0.0.5:9092        # Kafka distant
  python main.py --data-dir /hdfs/fraud/data      # HDFS
        """,
    )
    ap.add_argument("--bootstrap", default="127.0.0.1:9092", help="Kafka bootstrap (default: 127.0.0.1:9092)")
    ap.add_argument("--group-id", default="ingestion-pipeline-group", help="Consumer group ID")
    ap.add_argument("--data-dir", default="./data", help="Répertoire Data Lakehouse (default: ./data)")
    ap.add_argument("--parquet-batch", type=int, default=50, help="Batch size avant flush Parquet (default: 50)")
    ap.add_argument("--loop-test", action="store_true", help="Mode test local (sans Kafka)")

    args = ap.parse_args()

    cfg = Config(
        bootstrap=args.bootstrap,
        group_id=args.group_id,
        data_dir=args.data_dir,
        parquet_batch=args.parquet_batch,
    )

    if args.loop_test:
        run_local_test(cfg)
    else:
        run_kafka(cfg)
