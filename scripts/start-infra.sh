#!/usr/bin/env bash
# scripts/start-infra.sh
# Starts local Docker Compose dependencies (Postgres, Redis, Kafka, Prometheus, Grafana)
# and waits for all health checks to become healthy.

set -euo pipefail

echo "=========================================================="
echo "FlashSale Engine - Starting Local Infrastructure"
echo "=========================================================="

if ! command -v docker &> /dev/null; then
    echo "Error: Docker is not installed or not in PATH." >&2
    exit 1
fi

if [ ! -f ".env" ] && [ -f ".env.example" ]; then
    echo "Creating .env from .env.example..."
    cp .env.example .env
fi

echo "Starting Docker Compose services in background..."
docker compose up -d

echo ""
echo "Waiting for infrastructure health checks to report healthy..."

services=("flashsale-postgres" "flashsale-redis" "flashsale-kafka")
timeout_seconds=60

for service in "${services[@]}"; do
    printf "Checking %s ... " "$service"
    elapsed=0
    healthy=false
    while [ "$elapsed" -lt "$timeout_seconds" ]; do
        status=$(docker inspect --format='{{json .State.Health.Status}}' "$service" 2>/dev/null || echo "unknown")
        if [ "$status" = '"healthy"' ]; then
            healthy=true
            echo "HEALTHY"
            break
        fi
        sleep 2
        elapsed=$((elapsed + 2))
    done

    if [ "$healthy" = false ]; then
        echo "TIMEOUT or UNHEALTHY"
        echo "Warning: Service $service did not report healthy within $timeout_seconds seconds. Inspect with: docker logs $service"
    fi
done

echo ""
echo "Local Infrastructure Endpoints:"
echo "  * PostgreSQL:         localhost:5432 (db: flashsale_db, user: flashsale)"
echo "  * Redis:              localhost:6379"
echo "  * Apache Kafka:       localhost:9092"
echo "  * Prometheus:         http://localhost:9090"
echo "  * Grafana Dashboard:  http://localhost:3000 (admin / admin)"
echo "=========================================================="
