#!/usr/bin/env bash
set -euo pipefail

JOB_HINT="${1:-ingest-ux-events}"
BAD_SAVEPOINT_PATH="${BAD_SAVEPOINT_PATH:-file:///opt/flink/savepoints/missing-savepoint-for-demo}"

echo "Trying to restore ${JOB_HINT} from a deliberately broken savepoint path:"
echo "  ${BAD_SAVEPOINT_PATH}"
echo
echo "This is a mentor demo for the DISCARD decision: if state points to something invalid, resume repeats the failure."
echo

set +e
./scripts/ops-r2-restore-job-from-savepoint.sh "${JOB_HINT}" "${BAD_SAVEPOINT_PATH}"
STATUS=$?
set -e

if [[ "${STATUS}" -eq 0 ]]; then
  echo "WARN: The broken savepoint demo unexpectedly succeeded." >&2
  exit 1
fi

echo
echo "Expected failure observed. Recover with the last good savepoint if you just created one:"
echo "  ./scripts/ops-r2-restore-job-from-savepoint.sh ${JOB_HINT}"
echo
echo "If the real saved state is corrupted, the production decision is DISCARD: stateless restart + controlled replay + count validation."
