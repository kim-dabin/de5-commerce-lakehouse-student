# 05 Spark to Iceberg Batch BI

## 목표

Spark로 Paimon Bronze/current-state table을 읽어 Iceberg analytics table을 만듭니다.

```text
Paimon Bronze
  ux_events_bronze
  review_current
  order_current
    -> Spark batch transform
    -> Iceberg REST catalog
    -> MinIO warehouse
    -> StarRocks Iceberg external catalog
    -> Batch Lakehouse BI
```

이 단계는 Realtime OLAP의 "지금 무슨 일이 일어나는가?"가 아니라, Batch Lakehouse BI의 "믿을 수 있는 기준 결과는 무엇인가?"에 답합니다.

## 사전 준비

Olist Kafka -> Flink -> Paimon 실습을 먼저 완료합니다.

```bash
docker compose -f docker-compose.lite.yml up -d --build \
  kafka kafka-init minio minio-init iceberg-postgres iceberg-rest \
  flink-jobmanager flink-taskmanager spark-client

./scripts/reset-olist-kafka-topics.sh
./scripts/produce-olist-ux-events.sh
./scripts/produce-olist-review-events.sh
./scripts/produce-olist-order-events.sh

./scripts/reset-olist-paimon.sh
./scripts/run-flink-olist-paimon.sh
./scripts/query-olist-paimon.sh
```

## Transform 실행

```bash
./scripts/reset-iceberg-tables.sh
./scripts/run-spark-iceberg-transform.sh
./scripts/query-iceberg-tables.sh
```

첫 실행은 Spark가 Paimon/Iceberg runtime package를 shared Ivy cache volume에 내려받기 때문에 오래 걸릴 수 있습니다.

## 생성되는 Iceberg table

```text
iceberg_lake.analytics.olist_ux_events_clean
iceberg_lake.analytics.olist_review_current
iceberg_lake.analytics.olist_order_current
iceberg_lake.analytics.olist_event_type_daily
iceberg_lake.analytics.olist_funnel_daily
iceberg_lake.analytics.olist_category_daily
iceberg_lake.analytics.olist_review_sentiment_by_category
```

## L0/L1/L2로 읽기

```text
L0 - Paimon Bronze/current
  ux_events_bronze      event_id 기준 append fact
  review_current        review_id 기준 current-state upsert
  order_current         order_id 기준 current-state upsert

L1 - Iceberg base mart
  olist_ux_events_clean     event_date partition
  olist_review_current      no partition
  olist_order_current       no partition

L2 - Iceberg aggregate mart
  olist_event_type_daily              event_date partition
  olist_funnel_daily                  event_date partition
  olist_category_daily                event_date partition
  olist_review_sentiment_by_category  category/sentiment aggregate
```

`olist_review_current`는 `category_code`로 partition하지 않습니다. 리뷰의 카테고리는 매칭/보정 과정에서 바뀔 수 있는 속성이므로, current-state 기준 테이블은 무파티션으로 두고 카테고리 조회는 aggregate mart나 query layer에서 처리합니다.

## 기대 결과

샘플 기준 row count입니다.

```text
olist_ux_events_clean                  16,693
olist_review_current                    1,971
olist_order_current                     2,000
olist_event_type_daily                    256
olist_funnel_daily                         52
olist_category_daily                      759
olist_review_sentiment_by_category        120
```

Batch BI 대표 지표입니다.

```text
total_events      16,693
users              2,875
sessions           2,875
orders             1,968
revenue          265,036.00
reviews            1,971
avg_rating             3.93
negative_reviews     367
```

## BI metrics 조회

```bash
./scripts/query-bi-metrics.sh
```

출력에는 아래 prefix가 포함됩니다.

```text
BI_METRICS_JSON={...}
```

`query-bi-metrics.sh`는 Spark가 만든 Iceberg Analytics table을 StarRocks Iceberg external catalog로 조회하고, Streamlit BI는 이 JSON을 읽어 `Daily Business · Iceberg` 화면을 구성합니다.

## 수업 중 사용할 질문

- Spark가 Paimon Bronze/current-state를 읽고 Iceberg analytics에 쓰는 이유는 무엇일까요?
- Iceberg table을 기준 결과로 두면 재처리와 검증에서 어떤 이점이 생길까요?
- Realtime OLAP과 Batch BI의 숫자가 다르면 어느 쪽을 어떤 목적으로 봐야 할까요?
- `review_current`와 `ux_events_clean`을 결합하면 어떤 분석 질문을 만들 수 있을까요?
