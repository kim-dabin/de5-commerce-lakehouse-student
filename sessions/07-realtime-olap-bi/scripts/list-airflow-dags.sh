#!/usr/bin/env bash
set -euo pipefail

COMPOSE_FILE="${COMPOSE_FILE:-docker-compose.lite.yml}"

docker compose -f "${COMPOSE_FILE}" --profile orchestration exec -T airflow-webserver \
  airflow dags list
