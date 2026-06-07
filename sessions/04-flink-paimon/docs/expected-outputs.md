# 기대 출력

이 문서는 Olist 기반 로컬 실습을 정상 실행했을 때 확인할 수 있는 대표 출력입니다. 표의 border나 공백은 실행 환경에 따라 조금 달라질 수 있습니다.

## 전체 실행

전체 Olist BI 파이프라인은 아래 명령으로 실행합니다.

```bash
./scripts/run-olist-bi-pipeline.sh
```

이 스크립트는 아래 순서로 진행합니다.

```text
Kafka topics reset
  -> ux/review/order events produce
  -> Flink bounded ingestion to Paimon
  -> Spark transform to Iceberg
  -> StarRocks Paimon external catalog setup
  -> query outputs
```

## `docker compose -f docker-compose.lite.yml ps`

핵심 서비스가 아래와 같이 보여야 합니다.

```text
de5-kafka
de5-kafka-ui
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

## Kafka topics

```text
Topic: ux-events
PartitionCount: 3
ReplicationFactor: 1

Topic: review-events
PartitionCount: 3
ReplicationFactor: 1

Topic: order-status-events
PartitionCount: 3
ReplicationFactor: 1
```

## Kafka producer

샘플 기준 이벤트 수입니다.

```text
sent=16693 topic=ux-events input=/workspace/data/sample/olist/ux_events.jsonl
sent=5943 topic=review-events input=/workspace/data/sample/olist/review_events.jsonl
sent=7886 topic=order-status-events input=/workspace/data/sample/olist/order_status_events.jsonl
```

## Kafka consumer

```text
<session_id> | {"event_id":"ux-...","event_time":"2017-...","event_type":"product_view",...}
```

왼쪽 값은 Kafka message key이고, 오른쪽 JSON이 실제 event payload입니다.

## Paimon Bronze/current-state

`./scripts/run-flink-olist-paimon.sh` 실행 후에는 아래 메시지를 확인합니다.

```text
[INFO] Execute statement succeeded.
```

`./scripts/query-olist-paimon.sh` 실행 후 주요 count입니다.

```text
ux_events_bronze_count
16693

review_current_count
1971

order_current_count
2000
```

UXLog event type 집계는 아래 값이 나와야 합니다.

```text
add_to_cart            2365
product_view           3132
purchase               2220
remove_from_cart          1
review_expand          2740
review_impression      3103
search_result_click    3132
```

review sentiment 집계는 아래 값이 나와야 합니다.

```text
negative    367
neutral     157
positive   1447
```

order latest status 집계는 아래 값이 나와야 합니다.

```text
order_approved      41
order_canceled       3
order_created        2
order_delivered   1933
order_shipped       21
```

## Iceberg analytics tables

`./scripts/run-spark-iceberg-transform.sh` 실행 후 생성되는 table입니다.

```text
iceberg_lake.analytics.olist_ux_events_clean
iceberg_lake.analytics.olist_review_current
iceberg_lake.analytics.olist_order_current
iceberg_lake.analytics.olist_event_type_daily
iceberg_lake.analytics.olist_funnel_daily
iceberg_lake.analytics.olist_category_daily
iceberg_lake.analytics.olist_review_sentiment_by_category
```

`./scripts/query-iceberg-tables.sh` 실행 후 row count입니다.

```text
olist_ux_events_clean                  16693
olist_review_current                    1971
olist_order_current                     2000
olist_event_type_daily                   256
olist_funnel_daily                        52
olist_category_daily                     759
olist_review_sentiment_by_category       120
```

## BI metrics

`./scripts/query-bi-metrics.sh` 출력에는 아래 prefix가 포함됩니다.
이 명령은 Spark가 만든 Iceberg Analytics table을 StarRocks Iceberg external catalog로 조회합니다.

```text
BI_METRICS_JSON={...}
```

대표 batch BI 지표입니다.

```text
total_events       16693
users               2875
sessions            2875
orders              1968
revenue           265036.00
reviews             1971
reviewed_products   1384
avg_rating             3.93
negative_reviews     367
```

## StarRocks realtime OLAP

`./scripts/reset-realtime-olap.sh` 실행 후 Paimon external catalog/view 생성 메시지를 확인합니다.

```text
reset=starrocks_paimon_catalog catalog=paimon_olist views=de5_realtime_olap
```

`./scripts/query-realtime-olap.sh` 실행 후 전체 지표는 아래와 같습니다.

```text
total_events  users  sessions  products  revenue
16693         2875   2875      1470      265036.00
```

리뷰 영향 분석 대표 지표입니다.

```text
review_seen_pairs                 2940
cart_click_after_review_rate      73.91
purchase_after_review_rate        68.98
pdp_exit_rate                     25.90
```

`./scripts/query-realtime-olap-metrics.sh` 출력에는 아래 prefix가 포함됩니다.

```text
REALTIME_OLAP_JSON={...}
```

## Web UI

- Kafka UI: http://localhost:8088
- Flink UI: http://localhost:8081
- MinIO Console: http://localhost:9001
- StarRocks FE: http://localhost:8030
- Streamlit BI: http://127.0.0.1:8501

## Streamlit BI

```bash
./scripts/start-streamlit-bi.sh
```

대시보드 제목은 아래와 같습니다.

```text
DE5 Olist UXLog + Review Lakehouse BI
```

확인할 화면입니다.

- `Lakehouse Ops · StarRocks(Paimon)`: Paimon Bronze/current table을 StarRocks external catalog로 직접 조회한 결과
- `Daily Business · Iceberg`: Iceberg Analytics table을 StarRocks Iceberg external catalog로 직접 조회한 기준 BI 결과

`Lakehouse Ops · StarRocks(Paimon)` 탭에서는 `Review Impact · 리뷰 노출 이후 전환과 이탈` 섹션을 확인합니다.
