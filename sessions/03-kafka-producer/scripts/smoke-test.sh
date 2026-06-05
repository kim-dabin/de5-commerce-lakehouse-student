#!/usr/bin/env bash
set -euo pipefail

COMPOSE_FILE="${COMPOSE_FILE:-docker-compose.lite.yml}"
TOPIC="${KAFKA_TOPIC:-ux-events}"

echo "== Compose services =="
docker compose -f "${COMPOSE_FILE}" ps

echo
echo "== Kafka topic =="
docker compose -f "${COMPOSE_FILE}" exec -T kafka \
  /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka:19092 \
  --describe \
  --topic "${TOPIC}"

echo
echo "== Kafka UI =="
if command -v curl >/dev/null 2>&1; then
  curl -fsSI "http://localhost:${KAFKA_UI_HOST_PORT:-8088}" >/dev/null && \
    echo "Kafka UI is reachable: http://localhost:${KAFKA_UI_HOST_PORT:-8088}" || {
      echo "Kafka UI is not reachable yet."
      echo "Wait a few seconds and rerun ./scripts/smoke-test.sh if the container is still starting."
    }
else
  echo "curl not found; skip Kafka UI endpoint check"
fi

echo
echo "== Topic offsets =="
docker compose -f "${COMPOSE_FILE}" exec -T kafka \
  /opt/kafka/bin/kafka-get-offsets.sh \
  --bootstrap-server kafka:19092 \
  --topic "${TOPIC}"

echo
echo "Kafka smoke test completed."
