#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
COMPOSE_FILE="${SCRIPT_DIR}/docker-compose.openmetadata.yml"
COMPOSE_PROJECT_NAME="${COMPOSE_PROJECT_NAME:-de5-openmetadata}"
OM_UI_HOST_PORT="${OM_UI_HOST_PORT:-8585}"
OM_ADMIN_HOST_PORT="${OM_ADMIN_HOST_PORT:-8586}"
WAIT_SECONDS="${WAIT_SECONDS:-300}"

fail() {
  echo "ERROR: $*" >&2
  exit 1
}

command -v docker >/dev/null 2>&1 || fail "Docker CLI is not installed."
docker info >/dev/null 2>&1 || fail "Docker daemon is not running. Start Docker Desktop first."

if curl -fsS "http://localhost:${OM_ADMIN_HOST_PORT}/healthcheck" >/dev/null 2>&1; then
  cat <<EOF
OpenMetadata is already running.

UI:
  http://localhost:${OM_UI_HOST_PORT}

Login:
  admin@open-metadata.org / admin

Next:
  ${SCRIPT_DIR}/seed-openmetadata-demo.sh
EOF
  exit 0
fi

echo "Starting OpenMetadata optional stack..."
docker compose -p "${COMPOSE_PROJECT_NAME}" -f "${COMPOSE_FILE}" up -d

echo "Waiting for OpenMetadata healthcheck..."
deadline=$((SECONDS + WAIT_SECONDS))
until curl -fsS "http://localhost:${OM_ADMIN_HOST_PORT}/healthcheck" >/dev/null 2>&1; do
  if (( SECONDS >= deadline )); then
    echo "OpenMetadata is still starting or unhealthy after ${WAIT_SECONDS}s."
    echo "Check logs with:"
    echo "  docker compose -p ${COMPOSE_PROJECT_NAME} -f ${COMPOSE_FILE} logs -f openmetadata-server"
    exit 1
  fi
  sleep 5
done

cat <<EOF
OpenMetadata is ready.

UI:
  http://localhost:${OM_UI_HOST_PORT}

Login:
  admin@open-metadata.org / admin

Next:
  ${SCRIPT_DIR}/seed-openmetadata-demo.sh
EOF
