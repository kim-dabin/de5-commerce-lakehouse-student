#!/usr/bin/env bash
set -euo pipefail

COMPOSE_FILE="${COMPOSE_FILE:-docker-compose.lite.yml}"
TOPIC="${KAFKA_TOPIC:-ux-events}"
MAX_MESSAGES="${MAX_MESSAGES:-5}"

args=(
  /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka:19092 \
  --topic "${TOPIC}" \
  --from-beginning \
  --max-messages "${MAX_MESSAGES}" \
  --timeout-ms 10000 \
  --property print.key=true \
  --property key.separator=" | "
)

if [[ -n "${GROUP_ID:-}" ]]; then
  args+=(--group "${GROUP_ID}")
fi

docker compose -f "${COMPOSE_FILE}" exec -T kafka "${args[@]}"
