#!/usr/bin/env bash
set -euo pipefail

COMPOSE_FILE="${COMPOSE_FILE:-docker-compose.lite.yml}"
INPUT="${OLIST_REVIEW_INPUT:-data/sample/olist/review_events.jsonl}"
QUIET_FLAG=(--quiet)

if [[ "${VERBOSE:-false}" == "true" ]]; then
  QUIET_FLAG=()
fi

./scripts/produce-kafka.sh \
  --topic review-events \
  --input "/workspace/${INPUT}" \
  --key-field review_id \
  "${QUIET_FLAG[@]}" \
  "$@"
