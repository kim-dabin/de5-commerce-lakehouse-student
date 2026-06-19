#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
COMPOSE_PROJECT_NAME="${COMPOSE_PROJECT_NAME:-de5-openmetadata}"
OPENMETADATA_VERSION="${OPENMETADATA_VERSION:-1.12.6}"
OPENMETADATA_NETWORK="${OPENMETADATA_NETWORK:-${COMPOSE_PROJECT_NAME}_app_net}"
OPENMETADATA_HOST="${OPENMETADATA_HOST:-http://openmetadata-server:8585/api}"
OPENMETADATA_ADMIN_EMAIL="${OPENMETADATA_ADMIN_EMAIL:-admin@open-metadata.org}"
OPENMETADATA_ADMIN_PASSWORD="${OPENMETADATA_ADMIN_PASSWORD:-admin}"

if ! docker network inspect "${OPENMETADATA_NETWORK}" >/dev/null 2>&1; then
  echo "ERROR: Docker network not found: ${OPENMETADATA_NETWORK}" >&2
  echo "Start OpenMetadata first:" >&2
  echo "  ${SCRIPT_DIR}/start-openmetadata.sh" >&2
  exit 1
fi

docker run --rm \
  --network "${OPENMETADATA_NETWORK}" \
  -e OPENMETADATA_HOST="${OPENMETADATA_HOST}" \
  -e OPENMETADATA_JWT_TOKEN="${OPENMETADATA_JWT_TOKEN:-}" \
  -e OPENMETADATA_ADMIN_EMAIL="${OPENMETADATA_ADMIN_EMAIL}" \
  -e OPENMETADATA_ADMIN_PASSWORD="${OPENMETADATA_ADMIN_PASSWORD}" \
  -e OPENMETADATA_SEED_LINEAGE="${OPENMETADATA_SEED_LINEAGE:-true}" \
  -e OPENMETADATA_SEED_AIRFLOW_PIPELINE="${OPENMETADATA_SEED_AIRFLOW_PIPELINE:-false}" \
  -e OPENMETADATA_RESET_AIRFLOW_PIPELINE="${OPENMETADATA_RESET_AIRFLOW_PIPELINE:-false}" \
  -e OPENMETADATA_KAFKA_BOOTSTRAP="${OPENMETADATA_KAFKA_BOOTSTRAP:-host.docker.internal:9092}" \
  -e PYTHONDONTWRITEBYTECODE=1 \
  -v "${SCRIPT_DIR}:/workspace/openmetadata:ro" \
  "docker.getcollate.io/openmetadata/ingestion:${OPENMETADATA_VERSION}" \
  python /workspace/openmetadata/seed_de5_lite_demo.py
