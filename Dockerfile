# ===========================================================================
# Dockerfile — Ingestion Pipeline (Al Barid Bank)
# Auteur : Hamza (PFE EMSI 2025-2026)
# ---------------------------------------------------------------------------
# Build  : docker build -t barid-ingestion:1.0 .
# Run    : docker run barid-ingestion:1.0
# ===========================================================================

FROM python:3.11-slim

LABEL maintainer="Hamza — PFE EMSI 2025-2026"
LABEL project="Al Barid Bank — Détection de Fraude MLOps"
LABEL version="1.0"

# ── Pas de bytecode, logs en temps réel ──
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

# ── Dépendances système (librdkafka pour confluent-kafka) ──
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    librdkafka-dev \
    && rm -rf /var/lib/apt/lists/*

# ── Dépendances Python ──
COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt

# ── Code source ──
COPY src/ingestion/ /app/
COPY src/ingestion/kafka/ /app/kafka/

# ── Dossier data (volume mount en prod) ──
RUN mkdir -p /app/data/raw_zone /app/data/curated_zone /app/data/quarantine_zone /app/data/notification_logs

# ── Healthcheck ──
HEALTHCHECK --interval=30s --timeout=10s --retries=3 \
    CMD python -c "import confluent_kafka; print('OK')" || exit 1

# ── Point d'entrée : l'orchestrateur ──
ENTRYPOINT ["python", "main.py"]
CMD ["--bootstrap", "kafka:9092", "--data-dir", "/app/data", "--parquet-batch", "50"]
