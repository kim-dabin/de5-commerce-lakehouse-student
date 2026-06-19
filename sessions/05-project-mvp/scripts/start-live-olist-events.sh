#!/usr/bin/env bash
set -euo pipefail

export PATH="/usr/local/bin:/opt/homebrew/bin:/Applications/Docker.app/Contents/Resources/bin:${PATH}"

COMPOSE_FILE="${COMPOSE_FILE:-docker-compose.lite.yml}"
COMPOSE_PROJECT_NAME="${COMPOSE_PROJECT_NAME:-de5-lite}"
PRODUCER_IMAGE="${PRODUCER_IMAGE:-de5-producer:python3.12}"
RATE="${LIVE_OLIST_RATE_PER_SECOND:-6}"
MAX_EVENTS="${LIVE_OLIST_MAX_EVENTS:-0}"
RUNTIME_DIR="${RUNTIME_DIR:-.runtime}"
PID_FILE="${RUNTIME_DIR}/live-olist-events.pid"
LOG_FILE="${RUNTIME_DIR}/live-olist-events.log"

mkdir -p "${RUNTIME_DIR}"

if [[ -f "${PID_FILE}" ]]; then
  existing_pid="$(cat "${PID_FILE}")"
  if [[ -n "${existing_pid}" ]] && kill -0 "${existing_pid}" >/dev/null 2>&1; then
    echo "live olist producer is already running: pid=${existing_pid}"
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
  echo "stop it first: ./scripts/stop-live-olist-events.sh or ./scripts/stop-live-ux-events.sh"
  exit 1
fi

if ! docker image inspect "${PRODUCER_IMAGE}" >/dev/null 2>&1; then
  docker compose -f "${COMPOSE_FILE}" build kafka-producer
fi

echo "starting live olist producer..."
echo "topics=ux-events,review-events,order-status-events"
echo "rate=${RATE}/sec total max_events=${MAX_EVENTS}"
echo "log=${LOG_FILE}"

if [[ "${MAX_EVENTS}" != "0" ]]; then
  (
    docker compose -f "${COMPOSE_FILE}" --profile tools run --rm \
      --entrypoint python \
      kafka-producer \
      /workspace/labs/03-kafka-producer/live_olist_events.py \
      --rate-per-second "${RATE}" \
      --max-events "${MAX_EVENTS}"
  ) >"${LOG_FILE}" 2>&1 &
else
  (
    docker compose -f "${COMPOSE_FILE}" --profile tools run --rm \
      --entrypoint python \
      kafka-producer \
      /workspace/labs/03-kafka-producer/live_olist_events.py \
      --rate-per-second "${RATE}"
  ) >"${LOG_FILE}" 2>&1 &
fi

pid="$!"
echo "${pid}" > "${PID_FILE}"

echo "started pid=${pid}"
echo "watch: ./scripts/live-olist-events-status.sh"
echo "stop : ./scripts/stop-live-olist-events.sh"
