# ═══════════════════════════════════════════════════════════════════════
# MAKEFILE — Al Barid Bank MLOps Platform
# ═══════════════════════════════════════════════════════════════════════
#
# Usage:
#   make help              # Affiche l'aide
#   make kafka-up          # Lance Kafka + Zookeeper
#   make kafka-down        # Arrête Kafka
#   make kafka-logs        # Voir les logs
#   make topics-create     # Crée les topics
#   make pipeline-run      # Lance le pipeline complet
#   make clean             # Nettoie tout
#
# ═══════════════════════════════════════════════════════════════════════

.PHONY: help kafka-up kafka-down kafka-status kafka-logs topics-create \
        topics-list topics-delete pipeline-run pipeline-test kafka-cli clean

# ───────────────────────────────────────────────────────────────────────
# COLORS & OUTPUT
# ───────────────────────────────────────────────────────────────────────
RED=\033[0;31m
GREEN=\033[0;32m
YELLOW=\033[1;33m
BLUE=\033[0;34m
NC=\033[0m # No Color

# ───────────────────────────────────────────────────────────────────────
# VARIABLES
# ───────────────────────────────────────────────────────────────────────
DOCKER_COMPOSE=docker-compose
KAFKA_CONTAINER=kafka-broker-al-barid
ZOOKEEPER_CONTAINER=zookeeper-al-barid
BOOTSTRAP_SERVER=127.0.0.1:9092
KAFKA_UI_URL=http://localhost:8080

# ═══════════════════════════════════════════════════════════════════════
# HELP
# ═══════════════════════════════════════════════════════════════════════
help:
	@echo "$(BLUE)═══════════════════════════════════════════════════════════════$(NC)"
	@echo "$(BLUE)  Al Barid Bank MLOps — Kafka Management$(NC)"
	@echo "$(BLUE)═══════════════════════════════════════════════════════════════$(NC)"
	@echo ""
	@echo "$(YELLOW)KAFKA INFRASTRUCTURE$(NC)"
	@echo "  make kafka-up              Start Kafka + Zookeeper"
	@echo "  make kafka-down            Stop all services"
	@echo "  make kafka-status          Check service status"
	@echo "  make kafka-logs            View Kafka logs (streaming)"
	@echo "  make kafka-cli             Access Kafka CLI"
	@echo ""
	@echo "$(YELLOW)TOPICS MANAGEMENT$(NC)"
	@echo "  make topics-create         Create all topics"
	@echo "  make topics-list           List all topics"
	@echo "  make topics-delete         Delete all topics"
	@echo "  make topics-describe       Show topic details"
	@echo ""
	@echo "$(YELLOW)PIPELINE$(NC)"
	@echo "  make pipeline-run          Run full ingestion pipeline"
	@echo "  make pipeline-test         Run local test (no Kafka)"
	@echo "  make producer-run          Start test producer"
	@echo ""
	@echo "$(YELLOW)KAFKA UI$(NC)"
	@echo "  make kafka-ui-open         Open Kafka UI in browser"
	@echo ""
	@echo "$(YELLOW)MAINTENANCE$(NC)"
	@echo "  make clean                 Remove all containers & volumes"
	@echo "  make reset                 Full reset (⚠️  deletes data)"
	@echo ""

# ═══════════════════════════════════════════════════════════════════════
# KAFKA INFRASTRUCTURE
# ═══════════════════════════════════════════════════════════════════════

kafka-up:
	@echo "$(GREEN)🚀 Lancement de Kafka + Zookeeper...$(NC)"
	$(DOCKER_COMPOSE) up -d
	@echo "$(GREEN)✓ Services lancés$(NC)"
	@sleep 5
	@make kafka-status

kafka-down:
	@echo "$(RED)⏹️  Arrêt des services...$(NC)"
	$(DOCKER_COMPOSE) down
	@echo "$(GREEN)✓ Services arrêtés$(NC)"

kafka-status:
	@echo "$(BLUE)📊 Status des containers:$(NC)"
	@$(DOCKER_COMPOSE) ps

kafka-logs:
	@echo "$(BLUE)📋 Logs Kafka (Ctrl+C pour arrêter):$(NC)"
	@$(DOCKER_COMPOSE) logs kafka -f

zookeeper-logs:
	@echo "$(BLUE)📋 Logs Zookeeper (Ctrl+C pour arrêter):$(NC)"
	@$(DOCKER_COMPOSE) logs zookeeper -f

kafka-cli:
	@echo "$(BLUE)🖥️  Accès au Kafka CLI...$(NC)"
	@docker exec -it $(KAFKA_CONTAINER) bash

# ═══════════════════════════════════════════════════════════════════════
# TOPICS MANAGEMENT
# ═══════════════════════════════════════════════════════════════════════

topics-create:
	@echo "$(GREEN)📌 Création des topics...$(NC)"
	@python setup_kafka_topics.py
	@echo "$(GREEN)✓ Topics créés$(NC)"

topics-list:
	@echo "$(BLUE)📋 Topics existants:$(NC)"
	@docker exec $(KAFKA_CONTAINER) kafka-topics \
		--list --bootstrap-server localhost:9092

topics-describe:
	@echo "$(BLUE)📊 Configuration des topics:$(NC)"
	@python setup_kafka_topics.py --describe

topics-delete:
	@echo "$(RED)⚠️  Suppression de TOUS les topics...$(NC)"
	@docker exec $(KAFKA_CONTAINER) kafka-topics \
		--delete --bootstrap-server localhost:9092 \
		--topic raw_stream --topic cleaned_transactions \
		--topic quarantine_transactions --topic parsing_errors 2>/dev/null || true
	@echo "$(GREEN)✓ Topics supprimés$(NC)"

# ═══════════════════════════════════════════════════════════════════════
# PIPELINE OPERATIONS
# ═══════════════════════════════════════════════════════════════════════

pipeline-run:
	@echo "$(GREEN)🔄 Lancement du pipeline complet...$(NC)"
	@cd src/ingestion && python main.py

pipeline-test:
	@echo "$(GREEN)🧪 Lancement du test local (sans Kafka)...$(NC)"
	@cd src/ingestion && python main.py --loop-test

producer-run:
	@echo "$(GREEN)📤 Lancement du Producer (test)...$(NC)"
	@cd src/ingestion && python kafka/kafka_producer.py --delay 1 --loop

# ═══════════════════════════════════════════════════════════════════════
# KAFKA UI
# ═══════════════════════════════════════════════════════════════════════

kafka-ui-open:
	@echo "$(BLUE)🌐 Ouverture de Kafka UI...$(NC)"
	@command -v xdg-open >/dev/null 2>&1 && xdg-open $(KAFKA_UI_URL) || \
	 command -v open >/dev/null 2>&1 && open $(KAFKA_UI_URL) || \
	 echo "Ouvre manuellement : $(KAFKA_UI_URL)"

kafka-ui-logs:
	@echo "$(BLUE)📋 Logs Kafka UI:$(NC)"
	@$(DOCKER_COMPOSE) logs kafka-ui -f

# ═══════════════════════════════════════════════════════════════════════
# TESTING
# ═══════════════════════════════════════════════════════════════════════

test-connection:
	@echo "$(GREEN)🧪 Test de connexion Kafka...$(NC)"
	@python test_kafka_connection.py $(BOOTSTRAP_SERVER)

# ═══════════════════════════════════════════════════════════════════════
# MAINTENANCE
# ═══════════════════════════════════════════════════════════════════════

clean:
	@echo "$(RED)🧹 Nettoyage des containers...$(NC)"
	@$(DOCKER_COMPOSE) down
	@echo "$(GREEN)✓ Nettoyage terminé$(NC)"

reset:
	@echo "$(RED)⚠️  RESET COMPLET — suppression de tous les volumes!$(NC)"
	@read -p "Êtes-vous sûr ? (y/N) " -n 1 -r; \
	echo; \
	if [[ $$REPLY =~ ^[Yy]$$ ]]; then \
		$(DOCKER_COMPOSE) down -v; \
		echo "$(GREEN)✓ Reset terminé$(NC)"; \
	else \
		echo "$(YELLOW)Annulé$(NC)"; \
	fi

# ═══════════════════════════════════════════════════════════════════════
# QUICK START
# ═══════════════════════════════════════════════════════════════════════

quickstart: kafka-up topics-create test-connection
	@echo ""
	@echo "$(GREEN)✨ QUICKSTART TERMINÉ ✨$(NC)"
	@echo ""
	@echo "Prochaines étapes :"
	@echo "  1. Ouvre Kafka UI   : make kafka-ui-open"
	@echo "  2. Lance Producer   : make producer-run (terminal 1)"
	@echo "  3. Lance Pipeline   : make pipeline-run (terminal 2)"
	@echo ""
