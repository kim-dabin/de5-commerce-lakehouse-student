#!/usr/bin/env bash
set -euo pipefail

COMPOSE_FILE="${COMPOSE_FILE:-docker-compose.lite.yml}"
DAG_ID="${DAG_ID:-de5_lite_lakehouse_pipeline}"

docker compose -f "${COMPOSE_FILE}" --profile orchestration exec -T airflow-webserver \
  airflow dags trigger "${DAG_ID}"
