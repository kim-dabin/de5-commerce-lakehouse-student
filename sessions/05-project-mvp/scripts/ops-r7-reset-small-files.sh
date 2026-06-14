#!/usr/bin/env bash
set -euo pipefail

# R7 cleanup: drop the small-files demo table.
COMPOSE_FILE="${COMPOSE_FILE:-docker-compose.lite.yml}"

./scripts/run-spark-iceberg-sql.sh labs/08-ops-recovery/reset-small-files.sql

echo
echo "Dropped iceberg_lake.opsdemo.smallfiles_demo."
