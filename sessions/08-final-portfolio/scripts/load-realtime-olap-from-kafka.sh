#!/usr/bin/env bash
set -euo pipefail

COMPOSE_FILE="${COMPOSE_FILE:-docker-compose.lite.yml}"
TOPIC="${KAFKA_TOPIC:-commerce-events}"
INPUT_FILE="${KAFKA_INPUT:-data/sample/commerce_events_sample.jsonl}"
MAX_MESSAGES="${MAX_MESSAGES:-}"

if [[ -z "${MAX_MESSAGES}" ]]; then
  MAX_MESSAGES="$(grep -cve '^[[:space:]]*$' "${INPUT_FILE}")"
fi

response="$(
docker compose -f "${COMPOSE_FILE}" exec -T kafka \
  /opt/kafka/bin/kafka-console-consumer.sh \
    --bootstrap-server kafka:19092 \
    --topic "${TOPIC}" \
    --from-beginning \
    --max-messages "${MAX_MESSAGES}" \
| curl -sS --location-trusted -u root: \
    --resolve "starrocks-cn:${STARROCKS_CN_HTTP_PORT:-8040}:127.0.0.1" \
    -H "label: de5_realtime_olap_$(date +%s)_$$" \
    -H "format: json" \
    -H "read_json_by_line: true" \
    -T - \
    "http://localhost:${STARROCKS_FE_HTTP_PORT:-8030}/api/de5_realtime_olap/commerce_events_rt/_stream_load"
)"

echo "${response}"

python3 - "${response}" <<'PY'
import json
import sys

payload = json.loads(sys.argv[1])
status = payload.get("Status")
if status != "Success":
    raise SystemExit(f"StarRocks Stream Load failed: {payload}")
PY

echo "loaded=${MAX_MESSAGES} source=kafka topic=${TOPIC} target=de5_realtime_olap.commerce_events_rt"
