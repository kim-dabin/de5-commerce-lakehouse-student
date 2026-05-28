#!/usr/bin/env bash
set -euo pipefail

COMPOSE_FILE="${COMPOSE_FILE:-docker-compose.lite.yml}"
force_recreate_arg=""
if [[ "${AIRFLOW_FORCE_RECREATE:-false}" == "true" ]]; then
  force_recreate_arg="--force-recreate"
fi

docker compose -f "${COMPOSE_FILE}" --profile orchestration up -d --build ${force_recreate_arg:+${force_recreate_arg}} airflow-postgres
docker compose -f "${COMPOSE_FILE}" --profile orchestration up --build ${force_recreate_arg:+${force_recreate_arg}} airflow-init
docker compose -f "${COMPOSE_FILE}" --profile orchestration up -d --build ${force_recreate_arg:+${force_recreate_arg}} airflow-webserver airflow-scheduler

echo "Airflow UI: http://localhost:${AIRFLOW_HOST_PORT:-8080}"
echo "Username: ${_AIRFLOW_WWW_USER_USERNAME:-admin}"
echo "Password: ${_AIRFLOW_WWW_USER_PASSWORD:-admin}"
