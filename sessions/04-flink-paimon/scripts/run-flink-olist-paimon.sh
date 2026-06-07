#!/usr/bin/env bash
set -euo pipefail

COMPOSE_FILE="${COMPOSE_FILE:-docker-compose.lite.yml}"

docker compose -f "${COMPOSE_FILE}" exec -T flink-jobmanager \
  /bin/bash -lc 'mkdir -p /opt/flink/log && chown -R flink:flink /opt/flink/log'

docker compose -f "${COMPOSE_FILE}" exec -T --user flink flink-jobmanager \
  /opt/flink/bin/sql-client.sh \
  -f /workspace/labs/04-flink-paimon/11-insert-olist-bounded.sql
