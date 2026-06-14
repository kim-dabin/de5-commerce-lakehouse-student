#!/usr/bin/env bash
set -euo pipefail

COMPOSE_FILE="${COMPOSE_FILE:-docker-compose.lite.yml}"
TOPIC="${TOPIC:-ux-events}"

docker compose -f "${COMPOSE_FILE}" exec -T kafka \
  /opt/kafka/bin/kafka-configs.sh \
  --bootstrap-server kafka:19092 \
  --alter \
  --entity-type topics \
  --entity-name "${TOPIC}" \
  --delete-config min.insync.replicas,max.message.bytes

echo
echo "Kafka topic config after recovery:"
docker compose -f "${COMPOSE_FILE}" exec -T kafka \
  /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka:19092 \
  --describe \
  --topic "${TOPIC}"
