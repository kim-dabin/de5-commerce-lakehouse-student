#!/usr/bin/env bash
set -euo pipefail

COMPOSE_FILE="${COMPOSE_FILE:-docker-compose.lite.yml}"

if [[ $# -gt 0 ]]; then
  TOPICS=("$@")
else
  TOPICS=(ux-events review-events order-status-events)
fi

for topic in "${TOPICS[@]}"; do
  echo "== Kafka end offsets: ${topic} =="
  docker compose -f "${COMPOSE_FILE}" exec -T kafka \
    /opt/kafka/bin/kafka-get-offsets.sh \
    --bootstrap-server kafka:19092 \
    --topic "${topic}"
done
