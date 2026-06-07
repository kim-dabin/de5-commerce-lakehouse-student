# 04 Flink to Paimon Bronze

## 목표

Kafka의 Olist 이벤트를 Flink SQL streaming job으로 계속 읽어 Paimon Bronze/current-state 테이블에 저장합니다.

이번 차시의 핵심은 "Flink job이 실행됐다"가 아니라, Kafka -> Flink -> Paimon 각 계층에서 정상이라고 말할 증거를 남기는 것입니다.

```text
ux-events
  -> Flink SQL
  -> paimon_lake.bronze.ux_events_bronze
  -> append 행동 로그

review-events
  -> Flink SQL
  -> paimon_lake.bronze.review_current
  -> review_id primary key current-state

order-status-events
  -> Flink SQL
  -> paimon_lake.bronze.order_current
  -> order_id primary key current-state
```

## 왜 Paimon을 쓰는가

UXLog 자체는 append로 쌓아도 됩니다. 같은 사용자가 같은 상품을 두 번 보면 두 번 모두 의미 있는 행동입니다.

하지만 review와 order는 다릅니다.

- `review_current`: 같은 `review_id`에 리뷰 생성, 감성 분석, 답변 상태가 나중에 붙습니다.
- `order_current`: 같은 `order_id`에 주문 생성, 승인, 배송, 완료/취소 상태가 붙습니다.

따라서 이 데이터는 이벤트처럼 들어오지만 저장 모델은 current-state입니다. Paimon primary key table은 이런 "계속 보강되는 엔티티 상태"를 Lakehouse 안에서 다루기 위한 좋은 실습 대상입니다.

또한 Bronze에는 parsed column과 `raw_json`을 같이 남깁니다. 파싱 로직이 틀렸거나 스키마가 바뀌면 parsed column만으로는 원인을 찾기 어렵습니다. raw payload를 남겨야 나중에 다시 해석할 수 있습니다.

## 실행

```bash
docker compose -f docker-compose.lite.yml up -d --build \
  kafka kafka-init minio minio-init flink-jobmanager flink-taskmanager

./scripts/reset-olist-kafka-topics.sh
./scripts/produce-olist-ux-events.sh
./scripts/produce-olist-review-events.sh
./scripts/produce-olist-order-events.sh

./scripts/reset-olist-paimon.sh
./scripts/run-flink-olist-paimon-streaming.sh
./scripts/query-olist-paimon.sh
```

## 기대 결과

샘플 기준 검증 결과입니다.

```text
ux_events_bronze      16,693 rows
review_events input    5,943 rows
review_current         1,971 rows
order_events input     7,886 rows
order_current          2,000 rows
```

`review_events`가 5,943건인데 `review_current`가 1,971건인 이유는 같은 `review_id`에 대해 `review_created`, `sentiment_scored`, `review_answered`가 들어오고, Paimon primary key table이 이를 최신 상태로 접기 때문입니다.

`order_status_events`가 7,886건인데 `order_current`가 2,000건인 이유도 같습니다. 같은 `order_id`의 상태 변경 이력이 하나의 최신 주문 상태로 접힙니다.

## Streaming ingestion 주의점

이번 실습의 기준 경로는 streaming job입니다.

```text
Flink job은 Kafka topic을 계속 구독합니다.
Flink UI에서 ingestion job이 RUNNING이면 정상입니다.
새로 replay한 메시지는 job이 살아 있는 동안 계속 Paimon에 반영됩니다.
```

환경 문제를 분리하거나 빠르게 count만 재현해야 할 때는 보조 경로로 bounded job을 사용할 수 있습니다.

```bash
./scripts/run-flink-olist-paimon.sh
```

bounded job은 job 시작 시점의 latest offset까지만 읽고 종료합니다. 그래서 Flink UI에서 `FINISHED`가 정상입니다. 반대로 streaming job은 운영형 경로이므로 "job이 끝났는가"보다 "checkpoint가 계속 정상적으로 도는가"가 더 중요합니다.

## Iceberg-compatible metadata

현재 Paimon 테이블은 Iceberg-compatible metadata 생성을 켜둔 상태로 생성합니다.

```text
metadata.iceberg.storage = hadoop-catalog
```

`ux_events_bronze` 같은 append table은 Iceberg reader가 그대로 읽기 쉽습니다. 반면 `review_current`, `order_current` 같은 primary key table은 Paimon의 LSM 구조 때문에 Iceberg reader가 항상 최신 상태를 즉시 merge해서 읽는 것은 아닙니다. 그래서 local demo에서는 full compaction을 빠르게 발생시키도록 `full-compaction.delta-commits = 1`을 둡니다. 운영에서는 테이블 크기와 compaction 비용을 보고 더 긴 주기로 조정합니다.

## 검증 체인

Paimon row count 하나만으로 전체 정상이라고 말하지 않습니다.

| 계층 | 내가 확인할 증거 | 정상 기준 | 이상할 때 다음 확인 |
|---|---|---|---|
| Kafka topic | topic/message/offset | 메시지 존재, offset 증가 | producer log, listener, topic |
| Flink job | SQL Client output, Flink UI/log | job 성공 또는 실패 원인 확인 | source/sink connector, JSON parsing |
| Paimon Bronze | row count, event type, raw_json | 기대 count와 분포 일치 | PK, warehouse path, raw payload |

## 수업 중 사용할 질문

- `ux_events_bronze`는 append인데 `review_current`는 upsert인 이유는 무엇일까요?
- 같은 review 데이터를 한 번 더 replay하면 row count는 어떻게 될까요?
- Paimon row count가 0이면 Kafka, Flink, Paimon 중 어디부터 확인해야 할까요?
- `raw_json`을 남기는 비용과 얻는 이점은 무엇일까요?
