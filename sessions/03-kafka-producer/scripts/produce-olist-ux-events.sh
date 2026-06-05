#!/usr/bin/env bash
set -euo pipefail

COMPOSE_FILE="${COMPOSE_FILE:-docker-compose.lite.yml}"
INPUT="${OLIST_UX_INPUT:-data/sample/olist/ux_events.jsonl}"
QUIET_FLAG=(--quiet)

if [[ "${VERBOSE:-false}" == "true" ]]; then
  QUIET_FLAG=()
fi

./scripts/produce-kafka.sh \
  --topic ux-events \
  --input "/workspace/${INPUT}" \
  --key-field session_id \
  "${QUIET_FLAG[@]}" \
  "$@"
