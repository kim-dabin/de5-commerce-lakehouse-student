#!/usr/bin/env bash
set -euo pipefail

COMPOSE_FILE="${COMPOSE_FILE:-docker-compose.lite.yml}"
ICEBERG_VERSION="${ICEBERG_VERSION:-1.11.0}"
TABLE="${1:-iceberg_lake.analytics.olist_category_daily}"

docker compose -f "${COMPOSE_FILE}" exec -T spark-client \
  /opt/spark/bin/spark-submit \
  --packages "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:${ICEBERG_VERSION},org.apache.iceberg:iceberg-aws-bundle:${ICEBERG_VERSION}" \
  /workspace/labs/08-ops-recovery/find_iceberg_recovery_point.py \
  --table "${TABLE}"
