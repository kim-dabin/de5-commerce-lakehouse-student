#!/usr/bin/env bash
set -euo pipefail

COMPOSE_FILE="${COMPOSE_FILE:-docker-compose.lite.yml}"
JOB_HINT="${1:-ingest-ux-events}"
SAVEPOINT_DIR="${SAVEPOINT_DIR:-file:///opt/flink/savepoints}"
SAVEPOINT_PATH_FILE="${SAVEPOINT_PATH_FILE:-.ops-r2-last-savepoint}"

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
STOP_OUTPUT="$(docker compose -f "${COMPOSE_FILE}" exec -T flink-jobmanager \
  /opt/flink/bin/flink stop --savepointPath "${SAVEPOINT_DIR}" "${JOB_ID}")"
printf '%s\n' "${STOP_OUTPUT}"

SAVEPOINT_PATH="$(printf '%s\n' "${STOP_OUTPUT}" | grep -Eo '(file|s3)://?[^[:space:]]*savepoint[^[:space:]]*' | tail -n 1 || true)"

echo
if [[ -n "${SAVEPOINT_PATH}" ]]; then
  printf '%s\n' "${SAVEPOINT_PATH}" > "${SAVEPOINT_PATH_FILE}"
  echo "Saved savepoint path to ${SAVEPOINT_PATH_FILE}:"
  echo "  ${SAVEPOINT_PATH}"
  echo
  echo "Restore with:"
  echo "  ./scripts/ops-r2-restore-job-from-savepoint.sh ${JOB_HINT}"
else
  echo "WARN: Could not parse the savepoint path automatically." >&2
  echo "Copy the savepoint path from the output above, then restore with:" >&2
  echo "  ./scripts/ops-r2-restore-job-from-savepoint.sh ${JOB_HINT} <savepoint-path>" >&2
fi
