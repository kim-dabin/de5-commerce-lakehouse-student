#!/usr/bin/env bash
set -euo pipefail

COMPOSE_FILE="${COMPOSE_FILE:-docker-compose.lite.yml}"
JOB_HINT="${1:-ingest-ux-events}"
SAVEPOINT_DIR="${SAVEPOINT_DIR:-file:///opt/flink/savepoints}"

LIST_OUTPUT="$(docker compose -f "${COMPOSE_FILE}" exec -T flink-jobmanager /opt/flink/bin/flink list -r)"
echo "${LIST_OUTPUT}"

JOB_ID="$(printf '%s\n' "${LIST_OUTPUT}" | awk -v hint="${JOB_HINT}" '$0 ~ hint {print $4; exit}')"

if [[ -z "${JOB_ID}" ]]; then
  echo "Could not find a RUNNING job matching: ${JOB_HINT}" >&2
  echo "Usage: $0 [ingest-ux-events|ingest-review-current|ingest-order-current]" >&2
  exit 1
fi

echo
echo "Stopping ${JOB_HINT} (${JOB_ID}) with savepoint..."
docker compose -f "${COMPOSE_FILE}" exec -T flink-jobmanager \
  /opt/flink/bin/flink stop --savepointPath "${SAVEPOINT_DIR}" "${JOB_ID}"

echo
echo "Copy the savepoint path from the output above, then restore with:"
echo "  ./scripts/ops-r2-restore-job-from-savepoint.sh ${JOB_HINT} <savepoint-path>"
