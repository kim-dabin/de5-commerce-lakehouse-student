#!/usr/bin/env bash
set -euo pipefail

COMPOSE_FILE="${COMPOSE_FILE:-docker-compose.lite.yml}"

docker compose -f "${COMPOSE_FILE}" up -d --build iceberg-rest starrocks-fe starrocks-cn >/dev/null

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

python3 tools/query_starrocks_iceberg_bi_metrics.py
