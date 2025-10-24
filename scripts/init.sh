#!/bin/bash
# scripts/init.sh
set -e

echo "🚀 Initializing Data Engineer Test Environment..."

# Create necessary directories
mkdir -p data logs/airflow logs/etl postgres/backups

# Check if .env exists
if [ ! -f .env ]; then
    echo "📝 Creating .env file..."
    cat > .env << 'EOF'
# Airflow Configuration
AIRFLOW_UID=50000
FERNET_KEY=46BKJoQYlPPOexq0OhDZnIlNepKFf87WFwLbfzqDDho=
WEBSERVER_SECRET_KEY=airflow-webserver-secret-key-2024

# Database Configuration
POSTGRES_USER=airflow
POSTGRES_PASSWORD=airflow
POSTGRES_DB=airflow

# Paths
DATA_PATH=./data
LOG_PATH=./logs
ENVIRONMENT=development
EOF
    echo "✅ .env file created"
fi

# Load environment variables
export $(grep -v '^#' .env | xargs)

echo "🐳 Building Docker images..."
docker-compose build --no-cache

echo "📊 Starting PostgreSQL..."
docker-compose up -d postgres

echo "⏳ Waiting for PostgreSQL to be ready..."
for i in {1..30}; do
    if docker-compose exec -T postgres pg_isready -U airflow > /dev/null 2>&1; then
        echo "✅ PostgreSQL is ready!"
        break
    fi
    if [ $i -eq 30 ]; then
        echo "❌ PostgreSQL failed to start within 60 seconds"
        exit 1
    fi
    echo "⏰ Waiting for PostgreSQL... ($i/30)"
    sleep 2
done

echo "🔧 Initializing Airflow..."
if docker-compose run --rm airflow-init; then
    echo "✅ Airflow initialized successfully"
else
    echo "❌ Airflow initialization failed"
    echo "Trying alternative initialization method..."
    docker-compose run --rm airflow-init || true
fi

echo "🌐 Starting Airflow services..."
docker-compose up -d airflow-webserver airflow-scheduler

echo "✅ Environment initialization completed!"
echo ""
echo "📊 Services:"
echo "   Airflow UI: http://localhost:8080"
echo "   PostgreSQL: localhost:5432"
echo "   Default credentials: admin / admin"
echo ""
echo "🔍 Checking services status..."
docker-compose ps

echo ""
echo "⏳ Waiting for Airflow to be ready..."
sleep 30

echo "✅ All services should be ready!"
echo "🌐 Access Airflow at: http://localhost:8080"