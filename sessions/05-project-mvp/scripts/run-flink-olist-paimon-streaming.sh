#!/usr/bin/env bash
set -euo pipefail

COMPOSE_FILE="${COMPOSE_FILE:-docker-compose.lite.yml}"
JOB="${1:-all}"
INIT_SQL=/workspace/labs/04-flink-paimon/00-init-flink-session.sql

docker compose -f "${COMPOSE_FILE}" exec -T flink-jobmanager \
  /bin/bash -lc 'mkdir -p /opt/flink/log && chown -R flink:flink /opt/flink/log'

case "${JOB}" in
  all)
    SQL_FILES=(
      /workspace/labs/04-flink-paimon/13a-insert-olist-ux-events-streaming.sql
      /workspace/labs/04-flink-paimon/13b-insert-olist-review-current-streaming.sql
      /workspace/labs/04-flink-paimon/13c-insert-olist-order-current-streaming.sql
    )
    ;;
  append|ux|ux_events|ux_events_bronze)
    SQL_FILES=(/workspace/labs/04-flink-paimon/13a-insert-olist-ux-events-streaming.sql)
    ;;
  upsert)
    SQL_FILES=(
      /workspace/labs/04-flink-paimon/13b-insert-olist-review-current-streaming.sql
      /workspace/labs/04-flink-paimon/13c-insert-olist-order-current-streaming.sql
    )
    ;;
  review|review_current)
    SQL_FILES=(/workspace/labs/04-flink-paimon/13b-insert-olist-review-current-streaming.sql)
    ;;
  order|order_current)
    SQL_FILES=(/workspace/labs/04-flink-paimon/13c-insert-olist-order-current-streaming.sql)
    ;;
  *)
    echo "usage: $0 [all|append|upsert|ux|review|order]" >&2
    exit 2
    ;;
esac

for sql_file in "${SQL_FILES[@]}"; do
  echo "== submit Flink SQL job: ${sql_file} =="
  docker compose -f "${COMPOSE_FILE}" exec -T --user flink flink-jobmanager \
    /opt/flink/bin/sql-client.sh \
    -i "${INIT_SQL}" \
    -f "${sql_file}"
done
