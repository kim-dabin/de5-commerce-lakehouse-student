#!/usr/bin/env bash
set -euo pipefail

COMPOSE_FILE="${COMPOSE_FILE:-docker-compose.lite.yml}"

docker compose -f "${COMPOSE_FILE}" up -d --build starrocks-fe starrocks-cn
for _ in $(seq 1 90); do
  if docker compose -f "${COMPOSE_FILE}" exec -T starrocks-fe sh -lc \
    "mysql -h127.0.0.1 -P9030 -uroot -e 'SHOW COMPUTE NODES\\G' 2>/dev/null | grep 'Alive: true'" >/dev/null 2>&1; then
    break
  fi
  sleep 5
done

if ! docker compose -f "${COMPOSE_FILE}" exec -T starrocks-fe sh -lc \
  "mysql -h127.0.0.1 -P9030 -uroot -e 'SHOW COMPUTE NODES\\G' 2>/dev/null | grep 'Alive: true'" >/dev/null 2>&1; then
  echo "ERROR: StarRocks FE is reachable, but no CN node is alive." >&2
  docker compose -f "${COMPOSE_FILE}" exec -T starrocks-fe \
    mysql -h127.0.0.1 -P9030 -uroot -e "SHOW COMPUTE NODES\\G" >&2 || true
  exit 1
fi

docker compose -f "${COMPOSE_FILE}" exec -T starrocks-fe \
  mysql -h127.0.0.1 -P9030 -uroot < labs/07-realtime-olap/00-reset-starrocks.sql

echo "reset=starrocks_catalogs catalogs=paimon_olist,iceberg_olist views=de5_realtime_olap"
