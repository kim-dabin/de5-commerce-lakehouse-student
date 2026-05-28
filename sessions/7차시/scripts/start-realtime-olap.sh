#!/usr/bin/env bash
set -euo pipefail

COMPOSE_FILE="${COMPOSE_FILE:-docker-compose.lite.yml}"

docker compose -f "${COMPOSE_FILE}" up -d --build starrocks-fe starrocks-cn

for _ in $(seq 1 90); do
  if docker compose -f "${COMPOSE_FILE}" exec -T starrocks-fe sh -lc \
    "mysql -h127.0.0.1 -P9030 -uroot -e 'SHOW COMPUTE NODES\\G' 2>/dev/null | grep 'Alive: true'" >/dev/null 2>&1; then
    echo "StarRocks FE HTTP: http://localhost:${STARROCKS_FE_HTTP_PORT:-8030}"
    echo "StarRocks MySQL query port: localhost:${STARROCKS_FE_QUERY_PORT:-9030}"
    echo "StarRocks CN HTTP: http://localhost:${STARROCKS_CN_HTTP_PORT:-8040}"
    exit 0
  fi
  sleep 5
done

echo "ERROR: StarRocks shared-data cluster did not get an alive CN node in time." >&2
echo "Current StarRocks compute nodes:" >&2
docker compose -f "${COMPOSE_FILE}" exec -T starrocks-fe \
  mysql -h127.0.0.1 -P9030 -uroot -e "SHOW COMPUTE NODES\\G" >&2 || true
exit 1
