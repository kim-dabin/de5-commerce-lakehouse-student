# 5차시 프로젝트 커스터마이징 가이드

이번 프로젝트는 공통 실습을 그대로 제출하는 것이 목표가 아닙니다.
수업 시간에 사용한 스크립트와 SQL을 자기 질문에 맞게 바꾸고, 그 결과를 증거로 설명하는 것이 최종 포트폴리오의 핵심입니다.

핵심 방향은 아래 한 문장입니다.

```text
공통 파이프라인을 기반으로 하되, 비즈니스 질문 · 지표 · 검증 포인트 · 개선 방향은 각자의 것으로 만든다.
```

## 0. 먼저 지켜야 할 원칙

바로 운영 스크립트를 크게 고치지 마세요.
아래 순서대로 작게 바꾸고, 각 단계마다 검증 결과를 남기는 것이 좋습니다.

1. **질문을 먼저 바꾼다.**
   - 예: "리뷰를 본 사용자는 구매로 이어졌는가?"
   - 예: "부정 리뷰가 많은 카테고리에서 상세페이지 이탈이 높은가?"
   - 예: "주문 상태 변경이 늦은 카테고리는 무엇인가?"

2. **지표를 정의한다.**
   - 어떤 테이블에서 읽을지
   - row 한 줄의 의미(grain)가 무엇인지
   - count가 왜 이 숫자인지

3. **스크립트는 한 계층씩 바꾼다.**
   - Kafka replay
   - Flink/Paimon 적재
   - Spark/Iceberg mart
   - StarRocks/BI 조회
   - Airflow 검증

4. **바꾼 뒤에는 반드시 증거를 남긴다.**
   - 실행 명령어
   - count 변화
   - UI 캡처
   - task log
   - 기대값과 다른 경우의 해석

## 1. 전체 커스터마이징 지도

| 계층 | 수업 파일 | 바꿀 수 있는 것 | 검증 방법 |
|---|---|---|---|
| 데이터 | `data/sample/olist/*.jsonl` | 입력 이벤트, 필드, 샘플 크기 | `wc -l`, JSON 샘플 확인 |
| Kafka replay | `scripts/produce-olist-*.sh` | topic, input file, key field | Kafka UI, consume, offset |
| Producer | `labs/03-kafka-producer/producer.py` | 전송 옵션, key 추출 방식, dry-run | `--dry-run`, sent count |
| Flink source/sink | `labs/04-flink-paimon/13-insert-olist-streaming.sql` | Kafka topic, JSON parsing, Paimon table schema | Flink UI, Paimon count |
| Paimon 검증 | `scripts/query-olist-paimon.sh`, `labs/04-flink-paimon/12-query-olist-tables.sql` | count, 분포, snapshot/files 조회 | 쿼리 결과 |
| Spark mart | `labs/05-spark-iceberg/transform_to_iceberg.py` | clean/current/aggregate table, 지표 계산 | Iceberg table count |
| BI metric | `labs/05-spark-iceberg/query_bi_metrics.py` | 최종 지표, JSON payload | `./scripts/query-bi-metrics.sh` |
| Streamlit BI | `tools/streamlit_bi.py` | 화면 구성, chart, 설명 문구 | `./scripts/start-streamlit-bi.sh` |
| Airflow DAG | `labs/06-airflow-orchestration/dags/de5_olist_project_mvp_pipeline.py` | task 순서, 기대값 검증, 추가 validation | Airflow UI task log |

## 2. 추천 커스터마이징 레벨

처음부터 모든 계층을 바꾸려고 하면 쉽게 깨집니다.
본인 목표에 맞게 아래 세 단계 중 하나를 선택하세요.

### Level 1. 지표와 해석만 바꾸기

가장 안전합니다.
기존 파이프라인은 그대로 두고, BI에서 보는 질문과 해석을 다르게 가져갑니다.

주로 바꿀 파일:

```text
labs/05-spark-iceberg/query_bi_metrics.py
tools/streamlit_bi.py
docs/bi-dashboard-student-guide.md
```

예시:

- 매출 상위 카테고리보다 부정 리뷰 비율이 높은 카테고리에 집중
- `review_seen -> add_to_cart -> purchase` 흐름을 본인만의 funnel로 설명
- `orders = 1,968`과 `order_current = 2,000`의 정의 차이를 포트폴리오에 명시

검증:

```bash
./scripts/query-bi-metrics.sh
./scripts/start-streamlit-bi.sh
```

### Level 2. Kafka replay와 key 설계 바꾸기

입력 이벤트나 key 설계를 일부 바꾸는 단계입니다.
데이터 엔지니어링 포트폴리오에서 "왜 이 key로 partitioning 했는가"를 설명하기 좋습니다.

주로 바꿀 파일:

```text
scripts/produce-olist-ux-events.sh
scripts/produce-olist-review-events.sh
scripts/produce-olist-order-events.sh
scripts/reset-olist-kafka-topics.sh
data/sample/olist/*.jsonl
```

현재 기본 key:

| topic | script | key field | 이유 |
|---|---|---|---|
| `ux-events` | `produce-olist-ux-events.sh` | `session_id` | 같은 세션의 행동 순서를 한 partition에 모으기 |
| `review-events` | `produce-olist-review-events.sh` | `review_id` | 같은 리뷰의 상태 변경을 같은 key로 모으기 |
| `order-status-events` | `produce-olist-order-events.sh` | `order_id` | 같은 주문의 상태 변경을 같은 key로 모으기 |

바꿔볼 수 있는 질문:

- `ux-events` key를 `user_id`로 바꾸면 어떤 장단점이 있을까?
- `review-events` key가 없다면 upsert current table 검증은 어떻게 달라질까?
- 특정 key에 이벤트가 몰리는 skew가 생기면 어떻게 확인할까?

검증:

```bash
./scripts/reset-olist-kafka-topics.sh
./scripts/produce-olist-ux-events.sh
./scripts/consume-kafka.sh ux-events 5
./scripts/check-kafka-lag.sh
```

## 3. Level 3. Paimon / Iceberg mart 바꾸기

가장 포트폴리오 효과가 큽니다.
다만 schema와 downstream table이 같이 바뀌므로 한 번에 크게 고치지 말고, 새 table을 하나 추가하는 방식이 안전합니다.

### 3-1. Paimon table을 추가하거나 컬럼을 추가하기

주로 바꿀 파일:

```text
labs/04-flink-paimon/13-insert-olist-streaming.sql
labs/04-flink-paimon/12-query-olist-tables.sql
scripts/query-olist-paimon.sh
```

확인할 것:

- `CREATE TEMPORARY TABLE ... WITH ('connector' = 'kafka')`
- `CREATE TABLE IF NOT EXISTS paimon_lake.bronze...`
- `INSERT INTO paimon_lake.bronze... SELECT JSON_VALUE(...)`
- append table인지 primary key upsert table인지

포트폴리오 질문 예시:

```text
UX 행동 로그는 append-only가 맞는가?
리뷰와 주문은 왜 current-state upsert가 필요한가?
review/order 이벤트 이력도 append bronze로 남긴다면 어떤 장점이 있는가?
```

검증:

```bash
./scripts/reset-olist-paimon.sh
./scripts/run-flink-olist-paimon-streaming.sh
./scripts/query-olist-paimon.sh
```

### 3-2. Iceberg mart를 추가하기

주로 바꿀 파일:

```text
labs/05-spark-iceberg/transform_to_iceberg.py
labs/05-spark-iceberg/01-query-iceberg.sql
scripts/query-iceberg-tables.sh
```

현재 mart 구조:

| 구분 | table | grain |
|---|---|---|
| L1 clean | `olist_ux_events_clean` | event 1건 |
| L1 current | `olist_review_current` | review 1건 |
| L1 current | `olist_order_current` | order 1건 |
| L2 aggregate | `olist_event_type_daily` | date + event_type |
| L2 aggregate | `olist_funnel_daily` | date |
| L2 aggregate | `olist_category_daily` | date + category_code |
| L2 aggregate | `olist_review_sentiment_by_category` | category_code + sentiment |

추가해볼 만한 mart:

```text
olist_review_impact_by_category
olist_order_status_daily
olist_negative_review_funnel
olist_user_session_summary
```

검증:

```bash
./scripts/reset-iceberg-tables.sh
./scripts/run-spark-iceberg-transform.sh
./scripts/query-iceberg-tables.sh
```

## 4. Airflow 검증 task를 자기 것으로 바꾸기

Airflow는 데이터가 흘러가는 길이 아니라, 어디까지 정상인지 증거를 남기고 다음 단계를 실행하는 운영 지도입니다.

주로 바꿀 파일:

```text
labs/06-airflow-orchestration/dags/de5_olist_project_mvp_pipeline.py
```

현재 주요 task:

```text
validate_runtime_services
validate_flink_streaming_jobs
validate_paimon_fresh
reset_iceberg_tables
build_iceberg_analytics_mart
query_iceberg_tables
validate_bi_metric_counts
```

바꿔볼 수 있는 것:

- 기대 count 추가
- 새 mart table count 검증
- StarRocks external catalog refresh task 추가
- `users == sessions`처럼 샘플 데이터 특성 검증
- 고정 count 대신 관계 기반 검증 추가

예시 검증 아이디어:

```text
ux_events_clean row 수 == ux_events_bronze row 수
review_current row 수 == distinct review_id 수
order_current row 수 == distinct order_id 수
max(event_date)가 기대 범위 안에 있음
negative review ratio가 0~1 사이임
```

실행:

```bash
./scripts/start-airflow.sh
./scripts/trigger-airflow-pipeline.sh
```

UI:

```text
http://localhost:8080
admin / admin
```

## 5. 커스터마이징 후 반드시 남길 증거

최종 발표에서는 "코드를 바꿨습니다"보다 아래 증거가 더 중요합니다.

| 계층 | 남길 증거 |
|---|---|
| Kafka | topic, message count, key/partition/offset 샘플 |
| Flink | job RUNNING, records 변화, checkpoint 또는 task log |
| Paimon | table count, append/upsert 해석, raw_json 샘플 |
| Iceberg | mart table count, `.snapshots`, query 결과 |
| StarRocks/BI | dashboard 캡처, 쿼리 결과, 지표 정의 |
| Airflow | DAG graph, 실패/성공 task log, validate 결과 |

## 6. 권장 작업 순서

아래 순서대로 진행하면 수업 프로젝트를 자기 프로젝트로 바꾸기 쉽습니다.

```bash
# 1. 최신 코드 받기
git pull
cd sessions/05-project-mvp

# 2. 기본 파이프라인이 정상인지 먼저 확인
docker compose -f docker-compose.lite.yml up -d --build
./scripts/reset-olist-kafka-topics.sh
./scripts/produce-olist-ux-events.sh
./scripts/produce-olist-review-events.sh
./scripts/produce-olist-order-events.sh
./scripts/reset-olist-paimon.sh
./scripts/run-flink-olist-paimon-streaming.sh
./scripts/query-olist-paimon.sh
./scripts/reset-iceberg-tables.sh
./scripts/run-spark-iceberg-transform.sh
./scripts/query-iceberg-tables.sh
./scripts/query-bi-metrics.sh

# 3. 여기서부터 한 계층씩 커스터마이징
# - produce script
# - Flink SQL
# - Spark mart
# - BI query/dashboard
# - Airflow validation
```

## 7. 최종 발표에 넣을 문장 템플릿

아래 문장을 본인 프로젝트에 맞게 바꿔보세요.

```text
저는 공통 Olist 파이프라인을 기반으로, _______ 질문에 집중했습니다.

이 질문에 답하기 위해 _______ 테이블을 사용했고,
row 한 줄의 grain은 _______ 입니다.

Kafka에서는 _______ 를 증거로 보았고,
Flink/Paimon에서는 _______ 를 확인했으며,
Spark/Iceberg에서는 _______ mart를 만들었습니다.

BI에서는 _______ 지표를 해석했습니다.
이 지표를 볼 때 주의할 점은 _______ 입니다.

현재 MVP의 알려진 갭은 _______ 이고,
운영형으로 보강한다면 다음 단계는 _______ 입니다.
```

## 8. 피해야 할 커스터마이징

아래는 수업 기간 안에 리스크가 큽니다.

- `docker-compose.lite.yml`의 서비스 이름을 한 번에 많이 바꾸기
- Kafka topic 이름만 바꾸고 Flink SQL topic을 안 바꾸기
- Paimon table schema를 바꾸고 Spark transform을 안 바꾸기
- Iceberg table 이름을 바꾸고 BI query/Airflow expected count를 안 바꾸기
- count가 달라졌는데 append/upsert/grain을 확인하지 않고 유실로 단정하기

## 9. 좋은 포트폴리오의 기준

좋은 포트폴리오는 코드량이 많은 프로젝트가 아닙니다.

아래를 말할 수 있으면 충분히 강합니다.

```text
왜 이 데이터를 선택했는가?
왜 Kafka key를 이렇게 잡았는가?
왜 append와 upsert를 나눴는가?
왜 Paimon과 Iceberg를 나눠 썼는가?
왜 StarRocks를 serving layer로 두었는가?
왜 Airflow task를 이 순서로 설계했는가?
어떤 지표를 믿을 수 있고, 어떤 갭이 남아 있는가?
```

실무형 프로젝트는 "완벽하게 만들었다"보다 "어디까지 검증했고, 다음에 무엇을 고쳐야 하는지 안다"가 더 중요합니다.
