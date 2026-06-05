#!/usr/bin/env bash
set -euo pipefail

COMPOSE_FILE="${COMPOSE_FILE:-docker-compose.lite.yml}"

docker compose -f "${COMPOSE_FILE}" up -d --build \
  kafka kafka-init kafka-ui \
  minio minio-init \
  iceberg-postgres iceberg-rest \
  flink-jobmanager flink-taskmanager \
  spark-client \
  starrocks-fe starrocks-cn

./scripts/reset-olist-kafka-topics.sh
./scripts/produce-olist-ux-events.sh
./scripts/produce-olist-review-events.sh
./scripts/produce-olist-order-events.sh

./scripts/reset-olist-paimon.sh
./scripts/run-flink-olist-paimon.sh
./scripts/query-olist-paimon.sh

./scripts/reset-iceberg-tables.sh
./scripts/run-spark-iceberg-transform.sh
./scripts/query-iceberg-tables.sh
./scripts/query-bi-metrics.sh

./scripts/reset-realtime-olap.sh
./scripts/query-realtime-olap.sh
./scripts/query-realtime-olap-metrics.sh

echo
echo "Olist BI pipeline completed."
echo "Start BI dashboard:"
echo "  ./scripts/start-streamlit-bi.sh"
echo "Dashboard URL: http://127.0.0.1:8501"
