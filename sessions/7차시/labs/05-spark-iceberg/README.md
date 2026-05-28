# 05 Spark to Iceberg

## 목표

Spark로 Paimon Bronze table을 읽어 정제된 event row를 만들고, Iceberg REST catalog를 통해 analytics table을 저장합니다.

```text
Paimon Bronze: file:/warehouse/paimon/bronze.db/commerce_events_bronze
  -> Spark batch transform
  -> Iceberg REST catalog: http://iceberg-rest:8181
  -> MinIO bucket: s3://warehouse/
```

## 사전 준비

이전 실습을 먼저 완료합니다.

```bash
docker compose -f docker-compose.lite.yml up -d --build
./scripts/reset-kafka-topic.sh
./scripts/produce-kafka.sh
./scripts/reset-paimon-bronze.sh
./scripts/run-flink-paimon-bronze.sh
./scripts/query-paimon-bronze.sh
```

Paimon query에서 built-in classroom sample 기준 `row_count = 240`을 확인해야 합니다.

## Transform 실행

```bash
./scripts/run-spark-iceberg-transform.sh
```

첫 실행은 Spark가 Paimon/Iceberg runtime package를 shared Ivy cache volume에 내려받기 때문에 오래 걸릴 수 있습니다.

이 job은 세 개의 Iceberg table을 만듭니다.

- `iceberg_lake.analytics.commerce_events_clean`: 커머스 이벤트별 정제 row
- `iceberg_lake.analytics.commerce_event_type_daily`: 일 단위 event type 집계
- `iceberg_lake.analytics.commerce_category_daily`: 일 단위 category funnel과 revenue 지표

## Iceberg table 조회

```bash
./scripts/query-iceberg-tables.sh
```

query script는 Iceberg namespace와 table 목록을 먼저 출력하고, row count, daily summary, clean event row를 이어서 출력합니다.

기대 clean row count입니다.

```text
clean_row_count
240
```

기대 daily summary입니다.

```text
event_date   event_type          event_count  user_count  session_count  product_count  revenue
2026-05-01   cart                ...
2026-05-01   purchase            ...
2026-05-01   remove_from_cart    ...
2026-05-01   view                ...
```

## Iceberg table 초기화

```bash
./scripts/reset-iceberg-tables.sh
```

## 수업 중 사용할 질문

- Spark가 Bronze는 Paimon에서 읽고 serving table은 Iceberg에 쓰는 이유는 무엇일까요?
- Iceberg REST catalog는 어떤 역할을 할까요?
- Iceberg table을 `event_date`로 partitioning한 이유는 무엇일까요?
- 같은 transform을 두 번 실행하면 어떤 일이 생길까요?
