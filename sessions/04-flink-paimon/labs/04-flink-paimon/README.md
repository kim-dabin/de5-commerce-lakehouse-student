# 04 Flink to Paimon

## Olist 기반 신규 방향

수업 방향은 Olist 공개 이커머스 데이터셋 기반으로 전환한다. 기존 `commerce-events` 실습은 작은 smoke test로 남겨둘 수 있지만, 4차시의 핵심 설명은 아래 구조로 잡는다.

```text
ux-events
  -> append 행동 로그
  -> paimon_lake.bronze.ux_events_bronze

review-events
  -> review_id 기준 상태 변경 이벤트
  -> paimon_lake.bronze.review_current
```

이 구조에서 Paimon을 쓰는 주된 명분은 `review_current`다. UXLog는 발생 사실 자체가 중요하므로 append로 쌓고, review는 같은 `review_id`에 감성 분석/답변 상태가 나중에 붙기 때문에 primary key table로 최신 상태를 유지한다.

### Olist 샘플 실행

```bash
docker compose -f docker-compose.lite.yml up -d --build kafka kafka-init kafka-producer flink-jobmanager flink-taskmanager
./scripts/reset-olist-kafka-topics.sh
./scripts/produce-olist-ux-events.sh
./scripts/produce-olist-review-events.sh
./scripts/reset-olist-paimon.sh
./scripts/run-flink-olist-paimon.sh
./scripts/query-olist-paimon.sh
```

검증된 샘플 기준 결과:

```text
ux_events_bronze     13,023 rows
review_events input   5,943 rows
review_current        1,971 rows
```

`review_events`가 5,943건인데 `review_current`가 1,971건인 이유는 같은 `review_id`에 대해 `review_created`, `sentiment_scored`, `review_answered`가 들어오고, Paimon primary key table이 이를 최신 상태로 접기 때문이다.

---

## 목표

Flink SQL로 Kafka의 Commerce Events를 읽고 Paimon Bronze table에 저장합니다.

```text
Kafka topic: commerce-events
  -> Flink SQL raw source
  -> Paimon catalog: file:/warehouse/paimon
  -> bronze.commerce_events_bronze
```

## 사전 준비

Lite stack을 실행하고 샘플 이벤트를 Kafka에 넣습니다.

```bash
docker compose -f docker-compose.lite.yml up -d --build
./scripts/smoke-test.sh
./scripts/produce-kafka.sh
```

## Bounded ingestion 실행

```bash
./scripts/run-flink-paimon-bronze.sh
```

이 실습은 Kafka bounded scan mode를 사용합니다. Flink는 가장 이른 offset부터 job 시작 시점에 확인한 최신 offset까지 읽고, row를 Paimon에 쓴 뒤 종료합니다.

`docker compose exec ... sql-client.sh`를 직접 호출하지 말고 제공된 스크립트를 사용합니다. 이 스크립트는 SQL Client를 `flink` 사용자로 실행해 Paimon table file 권한이 꼬이지 않도록 합니다.

SQL 파일은 학습을 위해 나누어두었습니다. 실제 실행 스크립트는 `01-insert-bronze-bounded.sql`을 사용하며, 이 파일 안에서 catalog, database, table 생성 후 insert까지 처리합니다. 따라서 `00-create-bronze.sql`을 따로 먼저 실행할 필요는 없습니다.

Docker 내부 컨테이너끼리는 Kafka broker를 `kafka:19092`로 접근합니다. host machine에서 접근할 때는 `localhost:9092`를 사용합니다.

## Bronze table 조회

```bash
./scripts/query-paimon-bronze.sh
```

샘플 데이터 기준 기대 count입니다.

```text
row_count
240
```

event type summary는 아래 형태입니다.

```text
cart                ...
purchase            ...
remove_from_cart    ...
view                ...
```

## Bronze table 초기화

```bash
./scripts/reset-paimon-bronze.sh
```

## 수업 중 사용할 질문

- Kafka source에서 `format = 'raw'`를 사용한 이유는 무엇일까요?
- Bronze에 parsed column과 `raw_json`을 함께 저장하는 이유는 무엇일까요?
- Paimon table에 primary key가 없으면 어떤 점이 달라질까요?
- 수업 실습에서 bounded ingestion이 유용한 이유는 무엇일까요?
