# 5차시 자료: 프로젝트 MVP 완성

이번 차시는 개념을 새로 많이 넣는 시간이 아니라, 지금까지 만든 파이프라인을 하나의 프로젝트 결과물로 연결하고 검증하는 시간입니다.

핵심 질문은 하나입니다.

```text
이 파이프라인이 어디까지 정상인지 어떤 증거로 말할 수 있는가?
```

## 오늘 완성할 흐름

```text
Olist sample events
  -> Kafka topics
  -> Flink streaming jobs
  -> Paimon L0 tables
  -> StarRocks(Paimon external catalog) + Realtime BI
  -> Spark batch transform
  -> Iceberg L1/L2 marts
  -> StarRocks(Iceberg external catalog) + Daily BI
  -> Airflow DAG orchestration
```

## 먼저 확인할 것

1. Docker Desktop을 실행합니다.
2. 터미널에서 현재 폴더로 이동합니다.
3. 처음 실행이면 `.env` 파일을 만듭니다.

```bash
cp .env.example .env
./scripts/check-env.sh
```

첫 Docker 이미지 다운로드와 빌드는 PC와 네트워크에 따라 오래 걸릴 수 있습니다.

## 수업 중 대표 실행 명령

### 1. 전체 스택 실행

```bash
docker compose -f docker-compose.lite.yml up -d --build
docker compose -f docker-compose.lite.yml ps
```

### 2. Kafka -> Flink -> Paimon 확인

```bash
./scripts/reset-olist-kafka-topics.sh
./scripts/produce-olist-ux-events.sh
./scripts/produce-olist-review-events.sh
./scripts/produce-olist-order-events.sh

./scripts/reset-olist-paimon.sh
./scripts/run-flink-olist-paimon-streaming.sh
./scripts/query-olist-paimon.sh
```

Flink UI에서 아래 3개 job이 `RUNNING`인지 확인합니다.

```text
ux_events_bronze
review_current
order_current
```

Paimon 기대 count입니다.

```text
ux_events_bronze      16,693
review_current         1,971
order_current          2,000
```

`review-events`와 `order-status-events`의 Kafka 입력 수보다 Paimon current table count가 작아도 유실이 아닙니다. `review_current`는 `review_id`, `order_current`는 `order_id` 기준 최신 상태만 남기는 upsert/current-state 테이블입니다.

### 3. StarRocks/Paimon Realtime BI 확인

```bash
./scripts/reset-realtime-olap.sh
./scripts/query-realtime-olap.sh
./scripts/query-realtime-olap-metrics.sh
```

### 4. Spark -> Iceberg mart 생성

```bash
./scripts/reset-iceberg-tables.sh
./scripts/run-spark-iceberg-transform.sh
./scripts/query-iceberg-tables.sh
```

Iceberg 기대 count입니다.

```text
olist_ux_events_clean                  16,693
olist_review_current                    1,971
olist_order_current                     2,000
olist_event_type_daily                    256
olist_funnel_daily                         52
olist_category_daily                      759
olist_review_sentiment_by_category        120
```

### 5. BI metric 확인

```bash
./scripts/query-bi-metrics.sh
```

대표 지표입니다.

```text
total_events       16,693
users               2,875
sessions            2,875
orders              1,968
revenue           265,036.00
reviews             1,971
avg_rating             3.93
negative_reviews     367
```

`orders = 1,968`은 UX 이벤트 기준 purchase/order-linked BI 지표이고, `order_current = 2,000`은 주문 엔티티 current-state row count입니다.

### 6. Streamlit BI 실행

```bash
./scripts/start-streamlit-bi.sh
```

브라우저에서 접속합니다.

```text
http://127.0.0.1:8501
```

## Airflow DAG 확인

Airflow는 Flink streaming job을 대신 처리하지 않습니다. 이미 떠 있는 streaming ingestion이 정상인지 검증하고, Paimon 이후의 Spark/Iceberg/BI 검증 순서를 운영 관점에서 묶습니다.

```bash
./scripts/start-airflow.sh
./scripts/trigger-airflow-pipeline.sh
```

Airflow UI:

```text
http://localhost:8080
admin / admin
```

이번 차시 기준 DAG:

```text
de5_olist_project_mvp_pipeline
```

핵심 task:

```text
validate_runtime_services
validate_flink_streaming_jobs
validate_paimon_fresh
reset_iceberg_tables
build_iceberg_analytics_mart
query_iceberg_tables
validate_bi_metric_counts
```

UI에는 `start`, `finish` dummy task까지 보일 수 있습니다.

## 보조 자료

| 문서 | 용도 |
|---|---|
| `docs/session-05-mart-design-guide.html` | L0/L1/L2 mart 설계 설명 |
| `docs/bi-dashboard-student-guide.md` | BI 대시보드 읽는 법 |
| `docs/session-05-airflow-dag-summary.md` | Airflow DAG 설계 요약 |
| `docs/iceberg-catalog-metadata-guide.html` | Iceberg REST/PG/MinIO/StarRocks cache 구조 |
| `docs/olist-bi-pipeline-runbook.md` | 전체 파이프라인 검증 runbook |

## 실패 시 제출할 것

실패도 정상 산출물입니다. 아래를 함께 남겨주세요.

- 실행한 명령어
- 터미널 에러 메시지
- 어느 계층까지 정상이라고 판단했는지
- 다음에 확인할 계층

예시:

```text
Kafka produce는 성공했고 topic count는 증가했다.
Flink job 3개는 RUNNING이다.
Paimon query에서 review_current count가 기대값과 다르다.
다음 확인: Flink sink log / Paimon warehouse path / reset 시점
```
