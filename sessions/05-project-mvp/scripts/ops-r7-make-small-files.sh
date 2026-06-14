#!/usr/bin/env bash
set -euo pipefail

# R7 inject: create an Iceberg demo table and write many tiny commits, reproducing the
# "small files" problem in a dedicated namespace (iceberg_lake.opsdemo) so the real
# analytics marts are not touched.
COMPOSE_FILE="${COMPOSE_FILE:-docker-compose.lite.yml}"

./scripts/run-spark-iceberg-sql.sh labs/08-ops-recovery/make-small-files.sql

echo
echo "Created iceberg_lake.opsdemo.smallfiles_demo with 20 single-row commits -> 20 small data files."
echo "Observe the file count/size above (read amplification: many tiny files for few rows)."
echo
echo "Recover (compact) with:"
echo "  ./scripts/ops-r7-compact-small-files.sh"
echo "Clean up the demo table with:"
echo "  ./scripts/ops-r7-reset-small-files.sh"
