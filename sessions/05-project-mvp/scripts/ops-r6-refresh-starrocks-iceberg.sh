#!/usr/bin/env bash
set -euo pipefail

COMPOSE_FILE="${COMPOSE_FILE:-docker-compose.lite.yml}"

docker compose -f "${COMPOSE_FILE}" exec -T starrocks-fe \
  mysql -h127.0.0.1 -P9030 -uroot -e "
SET new_planner_optimize_timeout = 30000;
REFRESH EXTERNAL TABLE iceberg_olist.analytics.olist_ux_events_clean;
REFRESH EXTERNAL TABLE iceberg_olist.analytics.olist_review_current;
REFRESH EXTERNAL TABLE iceberg_olist.analytics.olist_order_current;
REFRESH EXTERNAL TABLE iceberg_olist.analytics.olist_event_type_daily;
REFRESH EXTERNAL TABLE iceberg_olist.analytics.olist_funnel_daily;
REFRESH EXTERNAL TABLE iceberg_olist.analytics.olist_category_daily;
REFRESH EXTERNAL TABLE iceberg_olist.analytics.olist_review_sentiment_by_category;
" || docker compose -f "${COMPOSE_FILE}" exec -T starrocks-fe \
  mysql -h127.0.0.1 -P9030 -uroot -e "
REFRESH EXTERNAL CATALOG iceberg_olist;
"

echo
echo "Requested StarRocks Iceberg external metadata refresh."
