"""Airflow DAG for the DE5 Lite Lakehouse pipeline."""

from __future__ import annotations

import json
import os
import time
from pathlib import Path

import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic


KAFKA_BOOTSTRAP = os.getenv("DE5_KAFKA_BOOTSTRAP", "kafka:19092")
KAFKA_TOPIC = os.getenv("DE5_KAFKA_TOPIC", "commerce-events")
KAFKA_PARTITIONS = int(os.getenv("DE5_KAFKA_PARTITIONS", "3"))
SAMPLE_EVENTS_PATH = Path(
    os.getenv(
        "DE5_SAMPLE_EVENTS_PATH",
        "/opt/airflow/workspace/data/sample/commerce_events_sample.jsonl",
    )
)
REALTIME_OLAP_SQL_DIR = Path(
    os.getenv(
        "DE5_REALTIME_OLAP_SQL_DIR",
        "/opt/airflow/workspace/labs/07-realtime-olap",
    )
)

OM_TABLE_PREFIX = os.getenv("DE5_OPENMETADATA_TABLE_PREFIX", "de5_lakehouse_demo.de5_lite_pipeline")
OM_COMMERCE_EVENTS_TOPIC = os.getenv("DE5_OPENMETADATA_KAFKA_TOPIC_FQN", "de5_kafka.commerce-events")
OM_COMMERCE_EVENTS_BRONZE = f"{OM_TABLE_PREFIX}.bronze.commerce_events_bronze"
OM_REALTIME_EVENTS = f"{OM_TABLE_PREFIX}.realtime_olap.commerce_events_rt"
OM_REALTIME_EVENT_TYPE = f"{OM_TABLE_PREFIX}.realtime_olap.commerce_event_type_realtime"
OM_REALTIME_CATEGORY = f"{OM_TABLE_PREFIX}.realtime_olap.commerce_category_realtime"
OM_COMMERCE_EVENTS_CLEAN = f"{OM_TABLE_PREFIX}.analytics.commerce_events_clean"
OM_COMMERCE_EVENT_TYPE_DAILY = f"{OM_TABLE_PREFIX}.analytics.commerce_event_type_daily"
OM_COMMERCE_CATEGORY_DAILY = f"{OM_TABLE_PREFIX}.analytics.commerce_category_daily"


def om_entity(entity: str, fqn: str, key: str) -> dict[str, str]:
    return {"entity": entity, "fqn": fqn, "key": key}


def om_topic(fqn: str, key: str) -> dict[str, str]:
    return om_entity("topic", fqn, key)


def om_table(fqn: str, key: str) -> dict[str, str]:
    return om_entity("table", fqn, key)


def _wait_until_topic_absent(admin: AdminClient, topic: str, timeout_seconds: int = 30) -> None:
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        metadata = admin.list_topics(timeout=10)
        if topic not in metadata.topics:
            return
        time.sleep(1)
    raise TimeoutError(f"Topic still exists after delete timeout: {topic}")


def reset_kafka_topic() -> None:
    admin = AdminClient({"bootstrap.servers": KAFKA_BOOTSTRAP})
    topics = admin.list_topics(timeout=10).topics

    if KAFKA_TOPIC in topics:
        futures = admin.delete_topics([KAFKA_TOPIC], operation_timeout=30)
        futures[KAFKA_TOPIC].result()
        _wait_until_topic_absent(admin, KAFKA_TOPIC)

    futures = admin.create_topics(
        [NewTopic(KAFKA_TOPIC, num_partitions=KAFKA_PARTITIONS, replication_factor=1)]
    )
    futures[KAFKA_TOPIC].result()
    print(f"created topic={KAFKA_TOPIC} partitions={KAFKA_PARTITIONS}")


def _event_key(event: dict) -> str:
    return str(event.get("user_session") or event.get("user_id") or event.get("id") or "")


def produce_sample_events() -> None:
    if not SAMPLE_EVENTS_PATH.exists():
        raise FileNotFoundError(f"Sample events file not found: {SAMPLE_EVENTS_PATH}")

    delivered = []
    errors = []

    def on_delivery(error, message) -> None:
        if error is not None:
            errors.append(error)
            return
        delivered.append((message.partition(), message.offset()))

    producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP})
    with SAMPLE_EVENTS_PATH.open("r", encoding="utf-8") as file:
        for line in file:
            raw = line.strip()
            if not raw:
                continue
            event = json.loads(raw)
            producer.produce(
                KAFKA_TOPIC,
                key=_event_key(event),
                value=raw,
                callback=on_delivery,
            )
            producer.poll(0)

    producer.flush(30)
    if errors:
        raise RuntimeError(f"Kafka delivery failed: {errors[:3]}")

    print(f"sent={len(delivered)} topic={KAFKA_TOPIC} input={SAMPLE_EVENTS_PATH}")


def flink_sql_task(
    task_id: str,
    sql_file: str,
    inlets: dict[str, list[str]] | None = None,
    outlets: dict[str, list[str]] | None = None,
) -> BashOperator:
    return BashOperator(
        task_id=task_id,
        bash_command=f"""
set -euo pipefail
docker exec de5-flink-jobmanager /bin/bash -lc 'mkdir -p /opt/flink/log && chown -R flink:flink /opt/flink/log'
docker exec --user flink de5-flink-jobmanager /opt/flink/bin/sql-client.sh -f /workspace/labs/04-flink-paimon/{sql_file}
""",
        inlets=inlets,
        outlets=outlets,
    )


with DAG(
    dag_id="de5_lite_lakehouse_pipeline",
    description="Kafka -> StarRocks realtime OLAP plus Flink/Paimon -> Spark/Iceberg teaching pipeline",
    start_date=pendulum.datetime(2026, 5, 1, tz="Asia/Seoul"),
    schedule=None,
    catchup=False,
    tags=["de5", "lakehouse", "bootcamp"],
) as dag:
    start = EmptyOperator(task_id="start")

    reset_topic = PythonOperator(
        task_id="reset_kafka_topic",
        python_callable=reset_kafka_topic,
    )

    produce_events = PythonOperator(
        task_id="produce_sample_events",
        python_callable=produce_sample_events,
    )

    reset_realtime_olap = BashOperator(
        task_id="reset_starrocks_realtime_olap",
        bash_command=f"""
set -euo pipefail
docker exec -i de5-starrocks-fe mysql -h127.0.0.1 -P9030 -uroot < {REALTIME_OLAP_SQL_DIR}/00-reset-starrocks.sql
""",
    )

    load_realtime_olap = BashOperator(
        task_id="load_starrocks_realtime_olap_from_kafka",
        bash_command=f"""
set -euo pipefail
EVENT_COUNT="$(grep -cve '^[[:space:]]*$' {SAMPLE_EVENTS_PATH})"
RESPONSE="$(
  docker exec de5-kafka /opt/kafka/bin/kafka-console-consumer.sh \
    --bootstrap-server kafka:19092 \
    --topic "{KAFKA_TOPIC}" \
    --from-beginning \
    --max-messages "${{EVENT_COUNT}}" \
  | curl -sS --location-trusted -u root: \
      -H "label: de5_airflow_realtime_$(date +%s)_$$" \
      -H "format: json" \
      -H "read_json_by_line: true" \
      -T - \
      "http://starrocks-fe:8030/api/de5_realtime_olap/commerce_events_rt/_stream_load"
)"
echo "${{RESPONSE}}"
export RESPONSE
python - <<'PY'
import json
import os

payload = json.loads(os.environ["RESPONSE"])
if payload.get("Status") != "Success":
    raise SystemExit(f"StarRocks Stream Load failed: {{payload}}")
PY
echo "loaded=${{EVENT_COUNT}} source=kafka topic={KAFKA_TOPIC} target=de5_realtime_olap.commerce_events_rt"
""",
        inlets=[om_topic(OM_COMMERCE_EVENTS_TOPIC, "kafka_to_realtime_olap")],
        outlets=[om_table(OM_REALTIME_EVENTS, "kafka_to_realtime_olap")],
    )

    query_realtime_olap = BashOperator(
        task_id="query_starrocks_realtime_olap",
        bash_command=f"""
set -euo pipefail
docker exec -i de5-starrocks-fe mysql -h127.0.0.1 -P9030 -uroot < {REALTIME_OLAP_SQL_DIR}/01-query-starrocks.sql
""",
        inlets=[
            om_table(OM_REALTIME_EVENTS, "query_realtime_olap"),
            om_table(OM_REALTIME_EVENT_TYPE, "query_realtime_olap"),
            om_table(OM_REALTIME_CATEGORY, "query_realtime_olap"),
        ],
    )

    reset_paimon = flink_sql_task("reset_paimon_bronze", "03-reset-bronze.sql")
    ingest_bronze = flink_sql_task(
        "run_flink_paimon_bronze",
        "01-insert-bronze-bounded.sql",
        inlets=[om_topic(OM_COMMERCE_EVENTS_TOPIC, "kafka_to_bronze")],
        outlets=[om_table(OM_COMMERCE_EVENTS_BRONZE, "kafka_to_bronze")],
    )
    query_bronze = flink_sql_task("query_paimon_bronze", "02-query-bronze.sql")

    reset_iceberg = BashOperator(
        task_id="reset_iceberg_tables",
        bash_command="""
set -euo pipefail
docker exec de5-spark-client /workspace/scripts/spark-iceberg-reset-inner.sh
""",
    )

    transform_iceberg = BashOperator(
        task_id="run_spark_iceberg_transform",
        bash_command="""
set -euo pipefail
docker exec de5-spark-client /workspace/scripts/spark-iceberg-transform-inner.sh
""",
        inlets=[om_table(OM_COMMERCE_EVENTS_BRONZE, "bronze_to_analytics")],
        outlets=[
            om_table(OM_COMMERCE_EVENTS_CLEAN, "bronze_to_analytics"),
            om_table(OM_COMMERCE_EVENT_TYPE_DAILY, "bronze_to_analytics"),
            om_table(OM_COMMERCE_CATEGORY_DAILY, "bronze_to_analytics"),
        ],
    )

    query_iceberg = BashOperator(
        task_id="query_iceberg_tables",
        bash_command="""
set -euo pipefail
docker exec de5-spark-client /workspace/scripts/spark-iceberg-query-inner.sh
""",
    )

    finish = EmptyOperator(task_id="finish")

    start >> reset_topic >> produce_events

    produce_events >> reset_realtime_olap >> load_realtime_olap >> query_realtime_olap

    (
        produce_events
        >> reset_paimon
        >> ingest_bronze
        >> query_bronze
        >> reset_iceberg
        >> transform_iceberg
        >> query_iceberg
    )

    [query_realtime_olap, query_iceberg] >> finish
