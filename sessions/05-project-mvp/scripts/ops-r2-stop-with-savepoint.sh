#!/usr/bin/env bash
set -euo pipefail

COMPOSE_FILE="${COMPOSE_FILE:-docker-compose.lite.yml}"
JOB_HINT="${1:-ingest-ux-events}"
SAVEPOINT_DIR="${SAVEPOINT_DIR:-file:///opt/flink/savepoints}"
SAVEPOINT_PATH_FILE="${SAVEPOINT_PATH_FILE:-.ops-r2-last-savepoint}"
FLINK_REST_URL="${FLINK_REST_URL:-http://localhost:8081}"

case "${JOB_HINT}" in
  ingest-ux-events|ux|append)
    JOB_PATTERN="ingest-ux-events|ux_events_bronze"
    ;;
  ingest-review-current|review|review_current)
    JOB_PATTERN="ingest-review-current|review_current"
    ;;
  ingest-order-current|order|order_current)
    JOB_PATTERN="ingest-order-current|order_current"
    ;;
  *)
    JOB_PATTERN="${JOB_HINT}"
    ;;
esac

OVERVIEW="$(curl -fsS "${FLINK_REST_URL}/jobs/overview")"
printf '%s\n' "${OVERVIEW}" | python3 -c '
import json
import sys

data = json.load(sys.stdin)
for job in data.get("jobs", []):
    if job.get("state") == "RUNNING":
        print("{} : {} ({})".format(job["jid"], job.get("name", ""), job["state"]))
'

JOB_ID="$(
  printf '%s\n' "${OVERVIEW}" | python3 -c '
import json
import re
import sys

pattern = re.compile(sys.argv[1])
data = json.load(sys.stdin)
for job in data.get("jobs", []):
    name = job.get("name", "")
    if job.get("state") == "RUNNING" and pattern.search(name):
        print(job["jid"])
        break
  ' "${JOB_PATTERN}"
)"

if [[ -z "${JOB_ID}" ]]; then
  echo "Could not find a RUNNING job matching: ${JOB_HINT}" >&2
  echo "Matched with pattern: ${JOB_PATTERN}" >&2
  echo "Usage: $0 [ingest-ux-events|ingest-review-current|ingest-order-current]" >&2
  exit 1
fi

echo
echo "Stopping ${JOB_HINT} (${JOB_ID}) with savepoint through Flink REST..."
docker compose -f "${COMPOSE_FILE}" exec -T flink-jobmanager \
  bash -lc 'mkdir -p /opt/flink/savepoints && chmod 777 /opt/flink/savepoints' </dev/null

TRIGGER_RESPONSE="$(
  curl -fsS -X POST "${FLINK_REST_URL}/jobs/${JOB_ID}/savepoints" \
    -H 'Content-Type: application/json' \
    -d "{\"target-directory\":\"${SAVEPOINT_DIR}\",\"cancel-job\":true,\"formatType\":\"CANONICAL\"}"
)"
echo "${TRIGGER_RESPONSE}"

REQUEST_ID="$(
  printf '%s\n' "${TRIGGER_RESPONSE}" | python3 -c '
import json
import sys

print(json.load(sys.stdin)["request-id"])
  '
)"

SAVEPOINT_PATH=""
for _ in $(seq 1 60); do
  STATUS_RESPONSE="$(curl -fsS "${FLINK_REST_URL}/jobs/${JOB_ID}/savepoints/${REQUEST_ID}")"
  echo "${STATUS_RESPONSE}"
  SAVEPOINT_PATH="$(
    printf '%s\n' "${STATUS_RESPONSE}" | python3 -c '
import json
import sys

data = json.load(sys.stdin)
if data.get("status", {}).get("id") == "COMPLETED":
    operation = data.get("operation") or {}
    if operation.get("failure-cause"):
        raise SystemExit("savepoint failed: " + str(operation["failure-cause"]))
    print(operation.get("location") or "")
    '
  )"
  if [[ -n "${SAVEPOINT_PATH}" ]]; then
    break
  fi
  sleep 2
done

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
