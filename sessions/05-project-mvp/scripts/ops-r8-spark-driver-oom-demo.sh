#!/usr/bin/env bash
# Mentor demo only. NOT a student hands-on.
# We expect this job to fail, so do not use `set -e`.
set -uo pipefail

# R8 (mentor demo): a deliberately memory-abusive Spark job that OOMs the driver.
# It is capped by --driver-memory (-Xmx), so the OOM stays inside this one spark-submit
# JVM and does not exhaust the shared Mac mini host or other containers.
COMPOSE_FILE="${COMPOSE_FILE:-docker-compose.lite.yml}"
DRIVER_MEM="${DRIVER_MEM:-512m}"

echo "Mentor demo: Spark driver OOM with --driver-memory ${DRIVER_MEM}."
echo "Expect java.lang.OutOfMemoryError / non-zero exit, contained to this spark-submit JVM."
echo

docker compose -f "${COMPOSE_FILE}" exec -T spark-client \
  /opt/spark/bin/spark-submit \
  --driver-memory "${DRIVER_MEM}" \
  --conf spark.driver.maxResultSize=0 \
  /workspace/labs/08-ops-recovery/spark_driver_oom_demo.py
STATUS=$?

echo
if [[ "${STATUS}" -ne 0 ]]; then
  echo "Job failed as expected (driver heap could not hold the collected result)."
  echo "This is a compute-layer resource failure, not data corruption: no table changed."
  echo "Production fix: avoid collect() of large results, aggregate/limit on the cluster,"
  echo "or raise driver memory deliberately after understanding why it grew."
else
  echo "NOTE: the OOM demo did not fail. Make it heavier and retry, e.g.:"
  echo "  DRIVER_MEM=384m OOM_ROWS=8000000 ./scripts/ops-r8-spark-driver-oom-demo.sh"
fi
