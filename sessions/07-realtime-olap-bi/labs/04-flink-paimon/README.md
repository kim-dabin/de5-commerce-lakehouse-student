# 04 Flink to Paimon

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
