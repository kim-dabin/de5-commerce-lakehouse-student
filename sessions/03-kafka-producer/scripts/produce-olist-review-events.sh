#!/usr/bin/env bash
set -euo pipefail

COMPOSE_FILE="${COMPOSE_FILE:-docker-compose.lite.yml}"
INPUT="${OLIST_REVIEW_INPUT:-data/sample/olist/review_events.jsonl}"

if [[ "${VERBOSE:-false}" == "true" ]]; then
  ./scripts/produce-kafka.sh \
    --topic review-events \
    --input "/workspace/${INPUT}" \
    --key-field review_id \
    "$@"
else
  ./scripts/produce-kafka.sh \
    --topic review-events \
    --input "/workspace/${INPUT}" \
    --key-field review_id \
    --quiet \
    "$@"
fi
