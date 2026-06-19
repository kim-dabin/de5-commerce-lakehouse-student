#!/usr/bin/env bash
set -euo pipefail

export PATH="/usr/local/bin:/opt/homebrew/bin:/Applications/Docker.app/Contents/Resources/bin:${PATH}"

COMPOSE_FILE="${COMPOSE_FILE:-docker-compose.lite.yml}"
COMPOSE_PROJECT_NAME="${COMPOSE_PROJECT_NAME:-de5-lite}"
RUNTIME_DIR="${RUNTIME_DIR:-.runtime}"
PID_FILE="${RUNTIME_DIR}/live-ux-events.pid"
LOG_FILE="${RUNTIME_DIR}/live-ux-events.log"

if [[ -f "${PID_FILE}" ]]; then
  pid="$(cat "${PID_FILE}")"
  if [[ -n "${pid}" ]] && kill -0 "${pid}" >/dev/null 2>&1; then
    echo "stopping live ux producer pid=${pid}"
    kill "${pid}" >/dev/null 2>&1 || true
  fi
  rm -f "${PID_FILE}"
fi

producer_containers="$(
  docker ps -q \
    --filter "label=com.docker.compose.project=${COMPOSE_PROJECT_NAME}" \
    --filter "label=com.docker.compose.service=kafka-producer" || true
)"
if [[ -n "${producer_containers}" ]]; then
  echo "stopping kafka-producer container(s):"
  echo "${producer_containers}"
  docker stop ${producer_containers} >/dev/null
fi

echo "stopped."
if [[ -f "${LOG_FILE}" ]]; then
  echo "last log lines:"
  tail -10 "${LOG_FILE}" || true
fi
