#!/usr/bin/env bash
set -euo pipefail

COMPOSE_FILE="${COMPOSE_FILE:-docker-compose.lite.yml}"
PRODUCER_IMAGE="${PRODUCER_IMAGE:-de5-producer:python3.12}"

if ! docker image inspect "${PRODUCER_IMAGE}" >/dev/null 2>&1; then
  docker compose -f "${COMPOSE_FILE}" build kafka-producer
fi

docker compose -f "${COMPOSE_FILE}" --profile tools run --rm kafka-producer "$@"
