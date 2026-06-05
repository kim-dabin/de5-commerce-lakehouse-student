#!/usr/bin/env bash
set -euo pipefail

COMPOSE_FILE="${COMPOSE_FILE:-docker-compose.lite.yml}"

docker compose -f "${COMPOSE_FILE}" exec -T starrocks-fe \
  mysql -h127.0.0.1 -P9030 -uroot < labs/07-realtime-olap/01-query-starrocks.sql
