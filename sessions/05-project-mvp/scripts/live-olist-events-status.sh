#!/usr/bin/env bash
set -euo pipefail

export PATH="/usr/local/bin:/opt/homebrew/bin:/Applications/Docker.app/Contents/Resources/bin:${PATH}"

COMPOSE_PROJECT_NAME="${COMPOSE_PROJECT_NAME:-de5-lite}"
RUNTIME_DIR="${RUNTIME_DIR:-.runtime}"
PID_FILE="${RUNTIME_DIR}/live-olist-events.pid"
LOG_FILE="${RUNTIME_DIR}/live-olist-events.log"

echo "== live olist producer process =="
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
echo "== Kafka end offsets =="
./scripts/ops-kafka-topic-offsets.sh ux-events review-events order-status-events || true

if [[ -f "${LOG_FILE}" ]]; then
  echo
  echo "== last log lines =="
  tail -12 "${LOG_FILE}" || true
fi
