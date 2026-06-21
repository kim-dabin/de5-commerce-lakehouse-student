#!/usr/bin/env bash
set -euo pipefail

COMPOSE_FILE="${COMPOSE_FILE:-docker-compose.lite.yml}"
DAG_ID="${DAG_ID:-de5_olist_project_mvp_pipeline}"

set +e
OUTPUT="$(
  docker compose -f "${COMPOSE_FILE}" --profile orchestration exec -T airflow-webserver \
    airflow dags trigger "${DAG_ID}" 2>&1
)"
STATUS=$?
set -e

if [[ ${STATUS} -eq 0 ]]; then
  printf '%s\n' "${OUTPUT}"
  exit 0
fi

if grep -q "not found in DagModel" <<<"${OUTPUT}"; then
  printf '%s\n' "${OUTPUT}" >&2
  echo "DAG is parsed but not yet serialized in Airflow metadata DB. Reserializing DAGs and retrying..." >&2
  docker compose -f "${COMPOSE_FILE}" --profile orchestration exec -T airflow-webserver \
    airflow dags reserialize -S /opt/airflow/dags >/dev/null
  docker compose -f "${COMPOSE_FILE}" --profile orchestration exec -T airflow-webserver \
    airflow dags trigger "${DAG_ID}"
  exit 0
fi

printf '%s\n' "${OUTPUT}" >&2
exit "${STATUS}"
