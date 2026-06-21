#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
COMPOSE_PROJECT_NAME="${COMPOSE_PROJECT_NAME:-de5-openmetadata}"
OPENMETADATA_VERSION="${OPENMETADATA_VERSION:-1.12.6}"
OPENMETADATA_NETWORK="${OPENMETADATA_NETWORK:-${COMPOSE_PROJECT_NAME}_app_net}"
OPENMETADATA_HOST="${OPENMETADATA_HOST:-}"
OPENMETADATA_ADMIN_EMAIL="${OPENMETADATA_ADMIN_EMAIL:-admin@open-metadata.org}"
OPENMETADATA_ADMIN_PASSWORD="${OPENMETADATA_ADMIN_PASSWORD:-admin}"
DQ_DEMO_INCLUDE_FAILURE="${DQ_DEMO_INCLUDE_FAILURE:-false}"

if ! docker network inspect "${OPENMETADATA_NETWORK}" >/dev/null 2>&1; then
  if docker network inspect "openmetadata_app_net" >/dev/null 2>&1; then
    OPENMETADATA_NETWORK="openmetadata_app_net"
  else
    echo "ERROR: Docker network not found: ${OPENMETADATA_NETWORK}" >&2
    echo "Start OpenMetadata first:" >&2
    echo "  ${SCRIPT_DIR}/start-openmetadata.sh" >&2
    exit 1
  fi
fi

OPENMETADATA_HOST="${OPENMETADATA_HOST:-http://openmetadata_server:8585/api}"

docker run --rm \
  --network "${OPENMETADATA_NETWORK}" \
  -e OPENMETADATA_HOST="${OPENMETADATA_HOST}" \
  -e OPENMETADATA_ADMIN_EMAIL="${OPENMETADATA_ADMIN_EMAIL}" \
  -e OPENMETADATA_ADMIN_PASSWORD="${OPENMETADATA_ADMIN_PASSWORD}" \
  -e DQ_DEMO_INCLUDE_FAILURE="${DQ_DEMO_INCLUDE_FAILURE}" \
  -e PYTHONDONTWRITEBYTECODE=1 \
  -v "${SCRIPT_DIR}:/workspace/openmetadata:ro" \
  "docker.getcollate.io/openmetadata/ingestion:${OPENMETADATA_VERSION}" \
  python /workspace/openmetadata/seed_openmetadata_dq_demo.py
