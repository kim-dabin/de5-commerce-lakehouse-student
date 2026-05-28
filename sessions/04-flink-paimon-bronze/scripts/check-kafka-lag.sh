#!/usr/bin/env bash
set -euo pipefail

COMPOSE_FILE="${COMPOSE_FILE:-docker-compose.lite.yml}"
GROUP_ID="${GROUP_ID:-de5-debug-consumer}"

if ! docker compose -f "${COMPOSE_FILE}" exec -T kafka \
  /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server kafka:19092 \
  --describe \
  --group "${GROUP_ID}"; then
  echo "No committed offsets found for group '${GROUP_ID}'."
  echo "Create one with: GROUP_ID=${GROUP_ID} ./scripts/consume-kafka.sh"
fi
