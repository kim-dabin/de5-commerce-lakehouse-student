#!/usr/bin/env bash
set -euo pipefail

COMPOSE_FILE="${COMPOSE_FILE:-docker-compose.lite.yml}"
BROKER="${BROKER:-kafka2}"
TOPIC="${TOPIC:-ux-events}"

# Real ISR failure injection.
# olist topics are created RF=2 with min.insync.replicas=2 (see reset-olist-kafka-topics.sh).
# Stopping the second broker drops the in-sync replica set from 2 to 1 (< 2), so acks=all
# producers are rejected with NotEnoughReplicasException. The controller/leader stays on
# `kafka`, so consumers (Flink) keep reading existing data: only the write path is broken.
docker compose -f "${COMPOSE_FILE}" stop "${BROKER}"

echo
echo "Stopped ${BROKER}. In-sync replicas for '${TOPIC}' should drop to 1 (< min.insync.replicas=2):"
docker compose -f "${COMPOSE_FILE}" exec -T kafka \
  /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka:19092 \
  --describe \
  --topic "${TOPIC}" || true

echo
echo "Now run the producer to see the acks=all write path fail (NotEnoughReplicasException):"
echo "  ./scripts/produce-olist-ux-events.sh --max-events 200"
echo
echo "Recover with:"
echo "  ./scripts/ops-r3-fix-kafka-isr.sh"
