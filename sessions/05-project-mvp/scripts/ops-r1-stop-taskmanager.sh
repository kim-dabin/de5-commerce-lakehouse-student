#!/usr/bin/env bash
set -euo pipefail

COMPOSE_FILE="${COMPOSE_FILE:-docker-compose.lite.yml}"

docker compose -f "${COMPOSE_FILE}" stop flink-taskmanager

echo "Stopped flink-taskmanager."
echo "Check Flink UI and then recover with:"
echo "  ./scripts/ops-r1-start-taskmanager.sh"
