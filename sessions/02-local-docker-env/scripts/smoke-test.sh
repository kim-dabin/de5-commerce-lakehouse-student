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
echo "== Flink cluster and jobs =="
if command -v curl >/dev/null 2>&1; then
  echo "-- Flink overview --"
  curl -fsS http://localhost:8081/overview || {
    echo
    echo "Flink REST endpoint is not ready yet."
    echo "Wait a few seconds and rerun ./scripts/smoke-test.sh if the container is still starting."
  }

  echo
  echo "-- Flink jobs --"
  curl -fsS http://localhost:8081/jobs || {
    echo
    echo "Flink jobs endpoint is not ready yet."
    echo "Wait a few seconds and rerun ./scripts/smoke-test.sh if the container is still starting."
  }

  echo
  echo "Note: 2차시에는 아직 Flink 애플리케이션 잡을 제출하지 않으므로 running job이 0개여도 정상입니다."
  echo "      여기서는 Flink 클러스터와 job 목록 API가 응답하는지만 확인합니다."
else
  echo "curl not found; skip Flink REST checks"
fi

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
echo "== StarRocks cluster =="
docker compose -f "${COMPOSE_FILE}" exec -T starrocks-fe \
  mysql -uroot -h starrocks-fe -P9030 \
  -e "SHOW FRONTENDS; SHOW COMPUTE NODES;" || {
    echo
    echo "StarRocks FE/CN is not ready yet."
    echo "Wait a few seconds and rerun ./scripts/smoke-test.sh if the container is still starting."
  }

echo
echo "Smoke test completed."
