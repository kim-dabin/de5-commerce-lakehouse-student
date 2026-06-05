#!/usr/bin/env bash
set -euo pipefail

COMPOSE_FILE="${COMPOSE_FILE:-docker-compose.lite.yml}"
TOPIC="${KAFKA_TOPIC:-ux-events}"

docker compose -f "${COMPOSE_FILE}" exec -T kafka \
  /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka:19092 \
  --delete \
  --if-exists \
  --topic "${TOPIC}"

for _ in $(seq 1 10); do
  if ! docker compose -f "${COMPOSE_FILE}" exec -T kafka \
    /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server kafka:19092 \
    --list | grep -Fx "${TOPIC}" >/dev/null; then
    break
  fi
  sleep 1
done

docker compose -f "${COMPOSE_FILE}" run --rm kafka-init
