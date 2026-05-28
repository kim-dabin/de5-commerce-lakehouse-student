#!/usr/bin/env bash
set -euo pipefail

COMPOSE_FILE="${COMPOSE_FILE:-docker-compose.lite.yml}"

docker compose -f "${COMPOSE_FILE}" exec -T spark-client \
  /workspace/scripts/spark-iceberg-transform-inner.sh
