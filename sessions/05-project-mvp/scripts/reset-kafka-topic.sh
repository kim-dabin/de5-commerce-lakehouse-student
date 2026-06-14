#!/usr/bin/env bash
set -euo pipefail

COMPOSE_FILE="${COMPOSE_FILE:-docker-compose.lite.yml}"
TOPIC="${KAFKA_TOPIC:-commerce-events}"
# Replication factor / partitions / min.insync.replicas are env-driven so the olist
# reset can ask for RF=2 + min.insync.replicas=2 (needed for the R3 real-ISR drill),
# while legacy single-broker topics keep RF=1 by default.
TOPIC_RF="${KAFKA_TOPIC_RF:-1}"
TOPIC_PARTITIONS="${KAFKA_TOPIC_PARTITIONS:-3}"
TOPIC_MIN_ISR="${KAFKA_TOPIC_MIN_ISR:-1}"

docker compose -f "${COMPOSE_FILE}" exec -T kafka \
  /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka:19092 \
  --delete \
  --if-exists \
  --topic "${TOPIC}"

for _ in $(seq 1 10); do
  if ! docker compose -f "${COMPOSE_FILE}" exec -T kafka \
    /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server kafka:19092 \
    --list | grep -Fx "${TOPIC}" >/dev/null; then
    break
  fi
  sleep 1
done

docker compose -f "${COMPOSE_FILE}" exec -T kafka \
  /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka:19092 \
  --create \
  --if-not-exists \
  --topic "${TOPIC}" \
  --partitions "${TOPIC_PARTITIONS}" \
  --replication-factor "${TOPIC_RF}" \
  --config "min.insync.replicas=${TOPIC_MIN_ISR}"

docker compose -f "${COMPOSE_FILE}" exec -T kafka \
  /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka:19092 \
  --describe \
  --topic "${TOPIC}"
