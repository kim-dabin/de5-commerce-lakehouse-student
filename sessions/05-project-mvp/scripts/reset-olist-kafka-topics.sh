#!/usr/bin/env bash
set -euo pipefail

COMPOSE_FILE="${COMPOSE_FILE:-docker-compose.lite.yml}"
TOPICS=("${@:-ux-events review-events order-status-events}")

if [[ $# -eq 0 ]]; then
  TOPICS=(ux-events review-events order-status-events)
fi

# RF=2 + min.insync.replicas=2 so the R3 drill can reproduce a real ISR failure:
# with both brokers up acks=all writes succeed; stop kafka2 and ISR drops to 1 < 2,
# so acks=all producers fail with NotEnoughReplicasException.
# Requires both `kafka` and `kafka2` to be up (docker compose up -d brings up both).
TOPIC_RF="${KAFKA_TOPIC_RF:-2}"
TOPIC_MIN_ISR="${KAFKA_TOPIC_MIN_ISR:-2}"

for topic in "${TOPICS[@]}"; do
  echo "== reset topic: ${topic} (RF=${TOPIC_RF}, min.insync.replicas=${TOPIC_MIN_ISR}) =="
  KAFKA_TOPIC="${topic}" \
  KAFKA_TOPIC_RF="${TOPIC_RF}" \
  KAFKA_TOPIC_MIN_ISR="${TOPIC_MIN_ISR}" \
  COMPOSE_FILE="${COMPOSE_FILE}" ./scripts/reset-kafka-topic.sh
done
