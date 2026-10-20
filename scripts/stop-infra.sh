#!/usr/bin/env bash
# scripts/stop-infra.sh
# Stops local Docker Compose infrastructure containers cleanly.

echo "Stopping FlashSale Engine local infrastructure..."
docker compose down
echo "Containers stopped."
