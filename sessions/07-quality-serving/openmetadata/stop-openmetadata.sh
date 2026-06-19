#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
COMPOSE_FILE="${SCRIPT_DIR}/docker-compose.openmetadata.yml"
COMPOSE_PROJECT_NAME="${COMPOSE_PROJECT_NAME:-de5-openmetadata}"

args=(down)
if [[ "${DELETE_OPENMETADATA_VOLUMES:-false}" == "true" ]]; then
  args+=(--volumes)
fi

docker compose -p "${COMPOSE_PROJECT_NAME}" -f "${COMPOSE_FILE}" "${args[@]}"
