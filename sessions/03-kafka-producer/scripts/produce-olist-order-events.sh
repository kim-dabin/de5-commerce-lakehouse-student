#!/usr/bin/env bash
set -euo pipefail

COMPOSE_FILE="${COMPOSE_FILE:-docker-compose.lite.yml}"
INPUT="${OLIST_ORDER_INPUT:-data/sample/olist/order_status_events.jsonl}"
QUIET_FLAG=(--quiet)

if [[ "${VERBOSE:-false}" == "true" ]]; then
  QUIET_FLAG=()
fi

./scripts/produce-kafka.sh \
  --topic order-status-events \
  --input "/workspace/${INPUT}" \
  --key-field order_id \
  "${QUIET_FLAG[@]}" \
  "$@"
