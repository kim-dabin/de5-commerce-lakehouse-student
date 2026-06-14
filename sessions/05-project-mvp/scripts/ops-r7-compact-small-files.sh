#!/usr/bin/env bash
set -euo pipefail

# R7 recover: compact the small files with Iceberg rewrite_data_files and show that the
# data file count drops while the row count is unchanged.
COMPOSE_FILE="${COMPOSE_FILE:-docker-compose.lite.yml}"

./scripts/run-spark-iceberg-sql.sh labs/08-ops-recovery/compact-small-files.sql

echo
echo "Compaction done: many small files were bin-packed into fewer larger files,"
echo "row count unchanged. In production this runs on a schedule (maintenance job)."
echo
echo "Clean up the demo table with:"
echo "  ./scripts/ops-r7-reset-small-files.sh"
