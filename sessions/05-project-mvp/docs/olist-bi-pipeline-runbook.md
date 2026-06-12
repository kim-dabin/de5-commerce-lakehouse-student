# Olist BI Pipeline Runbook

이 문서는 학생용 레포에 푸시하기 전에 멘토가 로컬에서 전체 파이프라인을 검증하는 기준입니다.

## 목표

새로운 Olist 기반 데이터로 아래 경로가 끝까지 동작하는지 확인합니다.

```text
Olist CSV
  -> generated JSONL sample
  -> Kafka ux/review/order topics
  -> Flink bounded ingestion
  -> Paimon Bronze/current-state
  -> Spark batch transform
  -> Iceberg analytics
  -> StarRocks Paimon external catalog
  -> StarRocks Iceberg external catalog
  -> Streamlit BI
```

## 한 번에 실행

```bash
./scripts/run-olist-bi-pipeline.sh
```

완료 후 BI를 실행합니다.

```bash
./scripts/start-streamlit-bi.sh
```

브라우저:

```text
http://127.0.0.1:8501
```

## 단계별 실행

```bash
docker compose -f docker-compose.lite.yml up -d --build \
  kafka kafka-init kafka-ui \
  minio minio-init \
  iceberg-postgres iceberg-rest \
  flink-jobmanager flink-taskmanager \
  spark-client \
  starrocks-fe starrocks-cn

./scripts/reset-olist-kafka-topics.sh
./scripts/produce-olist-ux-events.sh
./scripts/produce-olist-review-events.sh
./scripts/produce-olist-order-events.sh

./scripts/reset-olist-paimon.sh
./scripts/run-flink-olist-paimon.sh
./scripts/query-olist-paimon.sh

./scripts/reset-iceberg-tables.sh
./scripts/run-spark-iceberg-transform.sh
./scripts/query-iceberg-tables.sh

./scripts/reset-realtime-olap.sh
./scripts/query-realtime-olap.sh
./scripts/query-realtime-olap-metrics.sh
./scripts/query-bi-metrics.sh
```

## 검증 기준

```text
Kafka ux-events              16,693
Kafka review-events           5,943
Kafka order-status-events     7,886

Paimon ux_events_bronze      16,693
Paimon review_current         1,971
Paimon order_current          2,000

Iceberg olist_ux_events_clean 16,693
Iceberg olist_review_current   1,971
Iceberg olist_order_current    2,000

StarRocks Paimon catalog events 16,693
StarRocks Iceberg catalog events 16,693
Streamlit realtime events     16,693
Streamlit batch events        16,693
```

## 수업 메시지

```text
UXLog는 append fact다.
Review/Order는 current-state entity다.
Paimon은 append와 upsert를 함께 보여주는 Bronze 계층이다.
Iceberg는 batch 기준 결과를 남기는 analytics 계층이다.
StarRocks는 OLAP serving/query 계층이다. 현재 로컬 데모에서는 Paimon external catalog로 Paimon `ux_events_bronze`, `review_current`, `order_current`를 조회하고, Iceberg external catalog로 `olist_*` analytics table을 조회한다. 즉, StarRocks internal table에 raw UXLog serving copy를 만들지 않는다.
```
