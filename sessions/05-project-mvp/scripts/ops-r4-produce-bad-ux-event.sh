#!/usr/bin/env bash
set -euo pipefail

COMPOSE_FILE="${COMPOSE_FILE:-docker-compose.lite.yml}"

BAD_EVENT='{"event_id":"bad-price-demo-001","event_type":"product_view","event_time":"2026-06-14T12:00:00Z","order_id":null,"product_id":1000001,"source_product_id":"bad-demo","catalog_id":"bad-demo","category_id":999,"category_code":"debug.bad_record","brand":"debug","price":"BROKEN_PRICE","user_id":999999,"source_customer_id":"debug-customer","session_id":"bad-session-001","is_synthetic_ux":true}'

printf 'bad-session-001|%s\n' "${BAD_EVENT}" | docker compose -f "${COMPOSE_FILE}" exec -T kafka \
  /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server kafka:19092 \
  --topic ux-events \
  --property parse.key=true \
  --property key.separator='|'

echo
echo "Injected one malformed ux-events record."
echo "Observe Flink logs/UI. Kafka is immutable, so the clean recovery path is:"
echo "  ./scripts/reset-olist-kafka-topics.sh"
echo "  ./scripts/reset-olist-paimon.sh"
echo "  ./scripts/run-flink-olist-paimon-streaming.sh all"
echo "  ./scripts/produce-olist-ux-events.sh && ./scripts/produce-olist-review-events.sh && ./scripts/produce-olist-order-events.sh"
