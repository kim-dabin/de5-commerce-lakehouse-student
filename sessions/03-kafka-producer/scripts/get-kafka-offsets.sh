#!/usr/bin/env bash
set -euo pipefail

COMPOSE_FILE="${COMPOSE_FILE:-docker-compose.lite.yml}"
TOPIC="${KAFKA_TOPIC:-ux-events}"

docker compose -f "${COMPOSE_FILE}" exec -T kafka \
  /opt/kafka/bin/kafka-get-offsets.sh \
  --bootstrap-server kafka:19092 \
  --topic "${TOPIC}"
