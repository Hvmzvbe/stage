# ============================================================
# Dockerfile — ISO 8583 Parser Microservice
# Al Barid Bank — Plateforme MLOps Détection de Fraude
# ============================================================
# Build : docker build -t barid-iso8583-parser:1.0.0 .
# Run   : docker run --env-file .env barid-iso8583-parser:1.0.0
# ============================================================

FROM python:3.11-slim

# Métadonnées
LABEL maintainer="PFE Hamza — EMSI"
LABEL version="1.0.0"
LABEL description="ISO 8583 Universal Parser pour flux Kafka"

# Variables d'environnement
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONFAULTHANDLER=1

# Répertoire de travail
WORKDIR /app

# Installation des dépendances système (librdkafka pour confluent-kafka)
RUN apt-get update && apt-get install -y --no-install-recommends \
    librdkafka-dev \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Copie et installation des dépendances Python
COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt

# Copie du code source
COPY iso8583_parser.py .

# Utilisateur non-root (sécurité)
RUN useradd --create-home --shell /bin/bash parser_user
USER parser_user

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \
    CMD python -c "from iso8583_parser import UniversalISO8583Parser; p = UniversalISO8583Parser(); print('OK')" || exit 1

# Lancement
CMD ["python", "-u", "iso8583_parser.py"]
