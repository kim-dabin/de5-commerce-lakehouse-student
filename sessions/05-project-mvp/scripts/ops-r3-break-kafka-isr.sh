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
  --add-config min.insync.replicas=2

echo
echo "Injected Kafka ISR misconfiguration on topic '${TOPIC}'."
echo "A producer using acks=all should now fail on this single-broker cluster."
echo "Recover with:"
echo "  TOPIC=${TOPIC} ./scripts/ops-r3-fix-kafka-isr.sh"
