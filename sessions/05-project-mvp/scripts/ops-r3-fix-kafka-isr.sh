#!/usr/bin/env bash
set -euo pipefail

COMPOSE_FILE="${COMPOSE_FILE:-docker-compose.lite.yml}"
BROKER="${BROKER:-kafka2}"
TOPIC="${TOPIC:-ux-events}"

# Recovery: bring the second broker back so it rejoins the ISR (1 -> 2),
# which restores the acks=all write path for min.insync.replicas=2 topics.
docker compose -f "${COMPOSE_FILE}" start "${BROKER}"

echo
echo "Started ${BROKER}. Waiting for it to rejoin the ISR for '${TOPIC}'..."
for _ in $(seq 1 18); do
  ISR_LINE="$(docker compose -f "${COMPOSE_FILE}" exec -T kafka \
    /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server kafka:19092 \
    --describe \
    --topic "${TOPIC}" 2>/dev/null | grep -E 'Isr:' | head -1 || true)"
  # Both replicas back in sync -> the Isr list has two ids (a comma).
  if printf '%s' "${ISR_LINE}" | grep -qE 'Isr: *[0-9]+,[0-9]+'; then
    break
  fi
  sleep 2
done

echo
echo "Kafka topic state after recovery:"
docker compose -f "${COMPOSE_FILE}" exec -T kafka \
  /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka:19092 \
  --describe \
  --topic "${TOPIC}" || true

echo
echo "ISR restored. Re-run the producer to confirm the write path recovered:"
echo "  ./scripts/produce-olist-ux-events.sh --max-events 200"
