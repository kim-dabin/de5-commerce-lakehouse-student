#!/usr/bin/env bash
set -euo pipefail

./scripts/run-spark-iceberg-sql.sh labs/08-ops-recovery/drop-olist-category-daily.sql

echo
echo "Dropped iceberg_lake.analytics.olist_category_daily."
echo "Recover through Airflow:"
echo "  1. Open http://localhost:8080"
echo "  2. Trigger de5_olist_project_mvp_pipeline"
echo "  3. Check query_iceberg_tables and validate_bi_metric_counts"
