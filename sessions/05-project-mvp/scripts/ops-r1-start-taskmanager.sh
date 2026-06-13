#!/usr/bin/env bash
set -euo pipefail

COMPOSE_FILE="${COMPOSE_FILE:-docker-compose.lite.yml}"

docker compose -f "${COMPOSE_FILE}" up -d flink-taskmanager
docker compose -f "${COMPOSE_FILE}" ps flink-jobmanager flink-taskmanager

echo
echo "TaskManager is starting. Check Flink UI:"
echo "  http://localhost:8081"
