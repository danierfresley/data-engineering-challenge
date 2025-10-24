.PHONY: up down build clean logs test init quick-start

DOCKER_COMPOSE = docker-compose
AIRFLOW_UI = http://localhost:8080

# Main commands
up:
	$(DOCKER_COMPOSE) up -d

down:
	$(DOCKER_COMPOSE) down

build:
	$(DOCKER_COMPOSE) build --no-cache

clean:
	$(DOCKER_COMPOSE) down -v --remove-orphans
	docker system prune -f
	rm -rf logs data

logs:
	$(DOCKER_COMPOSE) logs -f

init:
	@echo "🚀 Initializing environment..."
	chmod +x scripts/init.sh
	./scripts/init.sh

test-dags:
	@echo "📋 Listing DAGs..."
	docker exec de-airflow-webserver airflow dags list

shell-airflow:
	docker exec -it de-airflow-webserver bash

shell-postgres:
	docker exec -it de-postgres psql -U airflow -d airflow

# Quick start - versión mejorada
quick-start:
	@echo "🚀 Quick starting environment..."
	
	# Crear directorios necesarios
	@mkdir -p data logs/airflow logs/etl postgres/backups
	
	# Crear .env si no existe
	@if [ ! -f .env ]; then \
		echo "📝 Creating .env file..."; \
		cat > .env << 'EOF' ;\
AIRFLOW_UID=50000 ;\
FERNET_KEY=46BKJoQYlPPOexq0OhDZnIlNepKFf87WFwLbfzqDDho= ;\
WEBSERVER_SECRET_KEY=airflow-webserver-secret-key-2024 ;\
 ;\
POSTGRES_USER=airflow ;\
POSTGRES_PASSWORD=airflow ;\
POSTGRES_DB=airflow ;\
 ;\
DATA_PATH=./data ;\
LOG_PATH=./logs ;\
ENVIRONMENT=development ;\
EOF \
		echo "✅ .env file created"; \
	fi
	
	@echo "🐳 Building images..."
	@$(DOCKER_COMPOSE) build --no-cache
	
	@echo "📊 Starting PostgreSQL..."
	@$(DOCKER_COMPOSE) up -d postgres
	
	@echo "⏳ Waiting for PostgreSQL..."
	@sleep 15
	
	@echo "🔧 Initializing Airflow..."
	@$(DOCKER_COMPOSE) run --rm airflow-init
	
	@echo "🌐 Starting Airflow services..."
	@$(DOCKER_COMPOSE) up -d
	
	@echo "✅ Quick start completed!"
	@echo "🌐 Airflow available at: $(AIRFLOW_UI)"
	@echo "👤 Username: admin, Password: admin"

status:
	@echo "📊 Services status:"
	@$(DOCKER_COMPOSE) ps

check-airflow:
	@echo "🔍 Checking Airflow..."
	@docker exec de-airflow-webserver airflow version

check-dags:
	@echo "📋 Checking DAGs..."
	@docker exec de-airflow-webserver airflow dags list