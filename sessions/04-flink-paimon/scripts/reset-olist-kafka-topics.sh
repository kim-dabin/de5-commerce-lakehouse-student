#!/usr/bin/env bash
set -euo pipefail

COMPOSE_FILE="${COMPOSE_FILE:-docker-compose.lite.yml}"
TOPICS=("${@:-ux-events review-events order-status-events}")

if [[ $# -eq 0 ]]; then
  TOPICS=(ux-events review-events order-status-events)
fi

for topic in "${TOPICS[@]}"; do
  echo "== reset topic: ${topic} =="
  KAFKA_TOPIC="${topic}" COMPOSE_FILE="${COMPOSE_FILE}" ./scripts/reset-kafka-topic.sh
done
