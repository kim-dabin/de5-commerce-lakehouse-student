# 06 Airflow Orchestration

## 목표

Olist 프로젝트 MVP에서 이미 검증한 흐름을 Airflow DAG로 묶습니다.

이번 DAG의 핵심은 "데이터를 새로 처리하는 도구를 하나 더 배우기"가 아니라, 이미 만든 파이프라인을 운영 관점에서 순서, 로그, 재시도, 검증 단위로 나누는 것입니다.

```text
Kafka -> Flink streaming -> Paimon
                         |
                         v
Airflow DAG: freshness check -> Spark/Iceberg mart -> StarRocks BI metric validation
```

Airflow는 Kafka, Flink, Spark, Paimon, Iceberg를 대체하지 않습니다.

- Kafka: 이벤트를 보관합니다.
- Flink: streaming ingestion job을 계속 실행합니다.
- Paimon: bronze/current-state 테이블을 저장합니다.
- Spark: analytics mart를 만듭니다.
- Iceberg: batch BI 기준 테이블을 저장합니다.
- Airflow: 위 작업들의 실행 순서, 로그, 실패 지점, 재시도 단위를 관리합니다.

## DAG 설계

현재 기준 DAG는 `de5_olist_project_mvp_pipeline`입니다.

```text
start
  -> validate_runtime_services
  -> validate_flink_streaming_jobs
  -> validate_paimon_fresh
  -> reset_iceberg_tables
  -> build_iceberg_analytics_mart
  -> query_iceberg_tables
  -> validate_bi_metric_counts
  -> finish
```

### 왜 Flink streaming job을 DAG에서 직접 끝내지 않나요?

Flink streaming job은 계속 떠 있어야 하는 수집 계층입니다. Airflow batch task처럼 `SUCCESS`로 끝나는 작업이 아닙니다.

그래서 이 DAG는 Flink job을 "실행해서 끝내는" 방식이 아니라, 이미 떠 있는 streaming job이 정상인지 확인한 뒤 Paimon 이후의 batch mart 작업을 오케스트레이션합니다.

## 실행 전 준비

core stack과 Olist streaming ingestion이 먼저 떠 있어야 합니다.

```bash
docker compose -f docker-compose.lite.yml up -d --build

./scripts/reset-olist-kafka-topics.sh
./scripts/produce-olist-ux-events.sh
./scripts/produce-olist-review-events.sh
./scripts/produce-olist-order-events.sh

./scripts/reset-olist-paimon.sh
./scripts/run-flink-olist-paimon-streaming.sh
```

Flink UI에서 아래 3개 job이 `RUNNING`인지 확인합니다.

```text
ux_events_bronze
review_current
order_current
```

## Airflow 시작

```bash
./scripts/start-airflow.sh
```

Airflow UI입니다.

```text
http://localhost:8080
username: admin
password: admin
```

port `8080`이 이미 사용 중이면 `.env`에서 `AIRFLOW_HOST_PORT`를 변경합니다.

## DAG 실행

터미널에서 실행합니다.

```bash
./scripts/trigger-airflow-pipeline.sh
```

또는 Airflow UI에서 `de5_olist_project_mvp_pipeline` DAG를 열고 직접 trigger합니다.

이전 lite DAG를 실행해야 하면 명시적으로 지정합니다.

```bash
DAG_ID=de5_lite_lakehouse_pipeline ./scripts/trigger-airflow-pipeline.sh
```

## WAP staging publish DAG

실무화 관점에서 `Write -> Audit -> Publish` 순서를 보여주는 별도 DAG도 제공합니다.

```text
de5_olist_wap_staging_publish_pipeline
```

기존 `de5_olist_project_mvp_pipeline`은 Iceberg analytics mart를 만든 뒤 검증합니다.

```text
build analytics mart -> query -> validate -> BI
```

WAP DAG는 새 결과를 바로 BI serving namespace에 쓰지 않고, 먼저 staging namespace에 씁니다.

```text
write analytics_wap_stage
-> audit staging count / BI metrics
-> publish analytics
-> validate published analytics
```

실행은 같은 trigger 스크립트에 DAG ID만 바꿔서 합니다.

```bash
DAG_ID=de5_olist_wap_staging_publish_pipeline ./scripts/trigger-airflow-pipeline.sh
```

이 DAG는 Iceberg branch fast-forward를 직접 쓰는 구현은 아닙니다. 학생 실습 환경에서는 이해와 재현이 쉬운 `staging namespace` 방식으로 WAP의 핵심인 "검증 전 데이터는 serving에 노출하지 않는다"를 보여줍니다.

## 기대 검증값

### Paimon

`validate_paimon_fresh` task log에서 아래 값을 확인합니다.

```text
ux_events_bronze  16693
review_current     1971
order_current      2000
```

`review-events`는 Kafka에 5,943건이 들어가지만 `review_current`는 1,971건입니다. 이는 유실이 아니라 `review_id` 기준 current-state upsert 테이블이기 때문입니다.

`order-status-events`는 Kafka에 7,886건이 들어가지만 `order_current`는 2,000건입니다. 이것도 `order_id` 기준 최신 상태만 남기는 구조입니다.

### Iceberg Analytics

`query_iceberg_tables` task log에서 아래 count를 확인합니다.

```text
olist_ux_events_clean                 16693
olist_review_current                   1971
olist_order_current                    2000
olist_event_type_daily                  256
olist_funnel_daily                       52
olist_category_daily                    759
olist_review_sentiment_by_category      120
```

### BI Metrics

`validate_bi_metric_counts` task는 StarRocks Iceberg external catalog로 아래 핵심 값을 검증합니다.

```text
total_events       16693
users               2875
sessions            2875
orders              1968
revenue        265036.00
reviews             1971
avg_rating          3.93
negative_reviews     367
order_current_rows  2000
```

주의할 점:

- `users = sessions = 2875`는 이번 생성 UX 시나리오의 특성입니다. 운영 일반 규칙이 아닙니다.
- `orders = 1968`은 UX 이벤트 기준 purchase/order-linked BI 지표입니다.
- `order_current_rows = 2000`은 주문 엔티티 current-state 테이블의 row count입니다.

## 수업 중 사용할 질문

- Airflow가 직접 처리하는 것과 처리하지 않는 것은 무엇인가요?
- Flink streaming job은 왜 Airflow task처럼 끝나면 안 되나요?
- `RUNNING job`과 `row count`는 각각 무엇을 증명하나요?
- Kafka count와 Paimon count가 다르면 유실이라고 바로 말해도 될까요?
- `validate_bi_metric_counts`가 실패하면 어느 계층부터 확인해야 할까요?

## 실패 시 확인 순서

1. `validate_runtime_services` 실패
   - Docker stack이 떠 있는지 확인합니다.
   - `docker compose -f docker-compose.lite.yml ps`

2. `validate_flink_streaming_jobs` 실패
   - Flink UI에서 job이 `RUNNING`인지 확인합니다.
   - 기대 job 이름은 `ingest-ux-events`, `ingest-review-current`, `ingest-order-current`입니다.
   - `./scripts/run-flink-olist-paimon-streaming.sh`를 다시 확인합니다.

3. `validate_paimon_fresh` 실패
   - MinIO bucket과 Paimon catalog 설정을 확인합니다.
   - `./scripts/query-olist-paimon.sh`

4. `build_iceberg_analytics_mart` 실패
   - Spark가 Paimon을 읽고 Iceberg REST Catalog에 쓸 수 있는지 확인합니다.
   - `./scripts/run-spark-iceberg-transform.sh`

5. `validate_bi_metric_counts` 실패
   - 지표 정의 차이인지, 실제 count 불일치인지 나눠서 봅니다.
   - `./scripts/query-bi-metrics.sh`
