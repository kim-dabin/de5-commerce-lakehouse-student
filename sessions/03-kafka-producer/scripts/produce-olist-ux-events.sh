#!/usr/bin/env bash
set -euo pipefail

COMPOSE_FILE="${COMPOSE_FILE:-docker-compose.lite.yml}"
INPUT="${OLIST_UX_INPUT:-data/sample/olist/ux_events.jsonl}"

./scripts/produce-kafka.sh \
  --topic ux-events \
  --input "/workspace/${INPUT}" \
  --key-field session_id \
  --quiet \
  "$@"
