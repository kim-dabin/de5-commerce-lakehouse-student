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
  --add-config min.insync.replicas=2,max.message.bytes=64

echo
echo "Injected Kafka write-path misconfiguration on topic '${TOPIC}'."
echo "  - min.insync.replicas=2 models the real ISR misconfiguration case."
echo "  - max.message.bytes=64 is a deterministic local tripwire, so the Olist JSON producer fails visibly."
echo "Recover with:"
echo "  TOPIC=${TOPIC} ./scripts/ops-r3-fix-kafka-isr.sh"
