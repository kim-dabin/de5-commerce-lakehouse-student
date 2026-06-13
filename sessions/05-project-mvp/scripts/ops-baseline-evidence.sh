#!/usr/bin/env bash
set -euo pipefail

COMPOSE_FILE="${COMPOSE_FILE:-docker-compose.lite.yml}"

echo "== Docker services =="
docker compose -f "${COMPOSE_FILE}" ps

echo
echo "== Flink running jobs =="
docker compose -f "${COMPOSE_FILE}" exec -T flink-jobmanager \
  /opt/flink/bin/flink list -r || true

echo
echo "== Paimon counts =="
./scripts/query-olist-paimon.sh

echo
echo "== Iceberg mart counts =="
./scripts/query-iceberg-tables.sh || {
  echo
  echo "WARN: Iceberg mart query failed."
  echo "If this is the first run, start Airflow and trigger de5_olist_project_mvp_pipeline first."
}

echo
echo "== StarRocks/BI metrics =="
./scripts/query-bi-metrics.sh || {
  echo
  echo "WARN: StarRocks/BI metric query failed."
  echo "Check whether Iceberg marts exist and whether StarRocks external metadata needs refresh."
}
