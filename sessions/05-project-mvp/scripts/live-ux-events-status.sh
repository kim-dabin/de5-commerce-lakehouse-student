#!/usr/bin/env bash
set -euo pipefail

export PATH="/usr/local/bin:/opt/homebrew/bin:/Applications/Docker.app/Contents/Resources/bin:${PATH}"

COMPOSE_FILE="${COMPOSE_FILE:-docker-compose.lite.yml}"
COMPOSE_PROJECT_NAME="${COMPOSE_PROJECT_NAME:-de5-lite}"
RUNTIME_DIR="${RUNTIME_DIR:-.runtime}"
PID_FILE="${RUNTIME_DIR}/live-ux-events.pid"
LOG_FILE="${RUNTIME_DIR}/live-ux-events.log"

echo "== live ux producer process =="
if [[ -f "${PID_FILE}" ]]; then
  pid="$(cat "${PID_FILE}")"
  if [[ -n "${pid}" ]] && kill -0 "${pid}" >/dev/null 2>&1; then
    echo "RUNNING pid=${pid}"
  else
    echo "NOT RUNNING (stale pid file: ${pid})"
  fi
else
  echo "NOT RUNNING"
fi

echo
echo "== kafka-producer container =="
docker ps \
  --filter "label=com.docker.compose.project=${COMPOSE_PROJECT_NAME}" \
  --filter "label=com.docker.compose.service=kafka-producer" \
  --format "table {{.Names}}\t{{.Status}}\t{{.RunningFor}}" || true

echo
echo "== ux-events end offsets =="
./scripts/ops-kafka-topic-offsets.sh ux-events || true

if [[ -f "${LOG_FILE}" ]]; then
  echo
  echo "== last log lines =="
  tail -10 "${LOG_FILE}" || true
fi
