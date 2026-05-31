#!/usr/bin/env bash
set -euo pipefail

COMPOSE_FILE="${COMPOSE_FILE:-docker-compose.lite.yml}"
TOPIC="${KAFKA_TOPIC:-commerce-events}"

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
echo "== MinIO buckets =="
docker compose -f "${COMPOSE_FILE}" run --rm minio-init

echo
echo "== Iceberg REST catalog config =="
if command -v curl >/dev/null 2>&1; then
  curl -fsS http://localhost:8181/v1/config || {
    echo
    echo "Iceberg REST endpoint is not ready or does not expose /v1/config yet."
    echo "Wait a few seconds and rerun ./scripts/smoke-test.sh if the container is still starting."
  }
else
  echo "curl not found; skip REST endpoint check"
fi

echo
echo "Smoke test completed."
