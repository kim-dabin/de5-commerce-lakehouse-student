#!/usr/bin/env bash
set -euo pipefail

export PATH="/usr/local/bin:/opt/homebrew/bin:/Applications/Docker.app/Contents/Resources/bin:${PATH}"

COMPOSE_FILE="${COMPOSE_FILE:-docker-compose.lite.yml}"
COMPOSE_PROJECT_NAME="${COMPOSE_PROJECT_NAME:-de5-lite}"
RATE="${LIVE_UX_RATE_PER_SECOND:-3}"
REPEAT="${LIVE_UX_REPEAT:-1}"
MAX_EVENTS="${LIVE_UX_MAX_EVENTS:-0}"
RUNTIME_DIR="${RUNTIME_DIR:-.runtime}"
PID_FILE="${RUNTIME_DIR}/live-ux-events.pid"
LOG_FILE="${RUNTIME_DIR}/live-ux-events.log"

mkdir -p "${RUNTIME_DIR}"

if [[ -f "${PID_FILE}" ]]; then
  existing_pid="$(cat "${PID_FILE}")"
  if [[ -n "${existing_pid}" ]] && kill -0 "${existing_pid}" >/dev/null 2>&1; then
    echo "live ux producer is already running: pid=${existing_pid}"
    echo "log: ${LOG_FILE}"
    exit 0
  fi
fi

running_producers="$(
  docker ps -q \
    --filter "label=com.docker.compose.project=${COMPOSE_PROJECT_NAME}" \
    --filter "label=com.docker.compose.service=kafka-producer" || true
)"
if [[ -n "${running_producers}" ]]; then
  echo "another kafka-producer container is already running:"
  echo "${running_producers}"
  echo "stop it first: ./scripts/stop-live-ux-events.sh"
  exit 1
fi

echo "starting live ux producer..."
echo "rate=${RATE}/sec repeat=${REPEAT} max_events=${MAX_EVENTS}"
echo "log=${LOG_FILE}"

if [[ "${MAX_EVENTS}" != "0" ]]; then
  (
    ./scripts/produce-olist-ux-events.sh \
      --repeat "${REPEAT}" \
      --rate-per-second "${RATE}" \
      --max-events "${MAX_EVENTS}" \
      --quiet \
      "$@"
  ) >"${LOG_FILE}" 2>&1 &
else
  (
    ./scripts/produce-olist-ux-events.sh \
      --repeat "${REPEAT}" \
      --rate-per-second "${RATE}" \
      --quiet \
      "$@"
  ) >"${LOG_FILE}" 2>&1 &
fi

pid="$!"
echo "${pid}" > "${PID_FILE}"

echo "started pid=${pid}"
echo "watch: ./scripts/live-ux-events-status.sh"
echo "stop : ./scripts/stop-live-ux-events.sh"
