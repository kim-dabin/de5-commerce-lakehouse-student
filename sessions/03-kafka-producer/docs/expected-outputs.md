# 기대 출력

이 문서는 로컬 실습을 정상 실행했을 때 확인할 수 있는 대표 출력입니다. 표의 border나 공백은 실행 환경에 따라 조금 달라질 수 있습니다.

## `docker compose -f docker-compose.lite.yml ps`

핵심 서비스가 아래와 같이 보여야 합니다.

```text
de5-kafka
de5-minio
de5-iceberg-postgres
de5-iceberg-rest
de5-flink-jobmanager
de5-flink-taskmanager
de5-spark-client
de5-starrocks-fe
de5-starrocks-cn
```

`de5-kafka-init`와 `de5-minio-init`는 topic과 bucket을 만든 뒤 정상 종료됩니다.

## Kafka topic

```text
Topic: commerce-events
PartitionCount: 3
ReplicationFactor: 1
```

## Kafka producer

```text
delivered topic=commerce-events partition=... offset=... key=...
sent=240 topic=commerce-events input=/workspace/data/sample/commerce_events_sample.jsonl
```

## Kafka consumer

```text
<user_session> | {"id":"evt-202605-000001","event_time":"2026-05-01T00:02:32Z","event_type":"purchase",...}
```

## Kafka offset

```text
commerce-events:0:...
commerce-events:1:...
commerce-events:2:...
```

`--repeat`, `--max-events`, `--key-field` 값을 바꾸면 offset은 달라질 수 있습니다.

## Paimon Bronze

`./scripts/run-flink-paimon-bronze.sh` 실행 후에는 아래 메시지를 확인합니다.

```text
[INFO] Execute statement succeeded.
```

`./scripts/query-paimon-bronze.sh` 실행 후 row count는 240이어야 합니다.

```text
row_count
240
```

event type 집계는 아래 형태로 보입니다.

```text
event_type          event_count
cart                36
purchase            29
remove_from_cart    19
view                156
```

샘플 row는 아래와 비슷합니다.

```text
event_id            event_time           event_type  category_code                 brand    price  user_id  user_session
evt-202605-000001   2026-05-01 00:02:32  purchase    electronics.smartphone        samsung  699.00 ...
evt-202605-000002   2026-05-01 00:03:42  view        grocery.drink.sparkling_water trevi     19.00 ...
```

Flink SQL Client의 tableau 출력 모드 때문에 border와 spacing은 환경에 따라 조금 다를 수 있습니다.

## Iceberg analytics tables

`./scripts/run-spark-iceberg-transform.sh` 실행 후 아래 table 생성 결과를 확인합니다.

```text
created=iceberg_lake.analytics.commerce_events_clean rows=240
created=iceberg_lake.analytics.commerce_event_type_daily rows=4
created=iceberg_lake.analytics.commerce_category_daily rows=11
```

`./scripts/query-iceberg-tables.sh` 실행 후 clean table row count는 240이어야 합니다.

```text
clean_row_count
240
```

일 단위 event type summary는 아래 형태입니다.

```text
event_date   event_type          event_count  user_count  session_count  product_count  revenue
2026-05-01   cart                36           32          36             11             0.00
2026-05-01   purchase            29           26          29             9              6142.00
2026-05-01   remove_from_cart    19           16          19             9              0.00
2026-05-01   view                156          69          156            11             0.00
```

카테고리/매출 summary에는 아래와 같은 row가 포함됩니다.

```text
category_code                    event_count  view_count  cart_count  purchase_count  revenue
electronics.smartphone           28           16          2           7               4893.00
electronics.keyboard             23           16          1           5               395.00
electronics.audio.headphone      24           17          3           3               387.00
```

## StarRocks realtime OLAP

`./scripts/load-realtime-olap-from-kafka.sh` 실행 후 Stream Load 성공 메시지를 확인합니다.

```text
"Status": "Success"
loaded=240 source=kafka topic=commerce-events target=de5_realtime_olap.commerce_events_rt
```

`./scripts/query-realtime-olap.sh` 실행 후 전체 지표는 아래와 같습니다.

```text
total_events  users  sessions  products  revenue
240           78     240       11        6142.00
```

realtime aggregate view는 아래 세 개입니다.

```text
commerce_event_type_realtime
commerce_category_realtime
commerce_minute_event_type_realtime
```

## MinIO bucket

```text
warehouse
paimon
```

## Web UI

- Flink UI: http://localhost:8081
- MinIO Console: http://localhost:9001
- StarRocks FE: http://localhost:8030
- Airflow UI: http://localhost:8080
- Streamlit BI: http://127.0.0.1:8501

## Airflow DAG

`./scripts/list-airflow-dags.sh` 실행 후 DAG 목록에 아래 이름이 포함되어야 합니다.

```text
de5_lite_lakehouse_pipeline
```

DAG를 trigger한 뒤 마지막 `query_iceberg_tables` task log에서 아래 값을 확인합니다.

```text
clean_row_count
240
```

## BI metrics

`./scripts/query-realtime-olap-metrics.sh` 출력에는 아래 JSON이 포함됩니다.

```text
REALTIME_OLAP_JSON={
  "totals": {
    "total_events": 240,
    "event_types": 4,
    "products": 11,
    "users": 78,
    "sessions": 240,
    "revenue": 6142.0
  },
  ...
}
```

`./scripts/query-bi-metrics.sh` 출력에는 아래 JSON이 포함됩니다.

```text
BI_METRICS_JSON={
  "totals": {
    "total_events": 240,
    "event_types": 4,
    "products": 11,
    "users": 78,
    "sessions": 240,
    "revenue": 6142.0
  },
  ...
}
```
