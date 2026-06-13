#!/usr/bin/env bash
set -euo pipefail

COMPOSE_FILE="${COMPOSE_FILE:-docker-compose.lite.yml}"
SQL_FILE="${1:?Usage: $0 <workspace-relative-sql-file>}"

docker compose -f "${COMPOSE_FILE}" exec -T spark-client \
  /workspace/scripts/spark-iceberg-sql-inner.sh "/workspace/${SQL_FILE}"
