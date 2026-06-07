# 03 Kafka Producer

## 목표

Olist 기반 수업용 이벤트를 Kafka topic에 넣고, topic/message/offset/lag를 근거로 수집 상태를 확인합니다.

이번 프로젝트의 Kafka 입력은 세 갈래입니다.

```text
ux-events
  - 사용자가 무엇을 했는가
  - append 행동 로그

review-events
  - 리뷰 상태가 어떻게 보강되는가
  - review_id 기준 current-state 변경 이벤트

order-status-events
  - 주문 상태가 어떻게 바뀌는가
  - order_id 기준 current-state 변경 이벤트
```

3차시의 핵심은 "Kafka에 보냈다"가 아니라, 어디까지 정상이라고 말할 증거가 있는지 남기는 것입니다.

## 사전 준비

Lite stack을 먼저 실행합니다.

```bash
docker compose -f docker-compose.lite.yml up -d --build kafka kafka-init kafka-ui
./scripts/smoke-test.sh
```

## UXLog replay

가장 먼저 `ux-events` topic에 append 행동 로그를 replay합니다.

```bash
./scripts/reset-olist-kafka-topics.sh ux-events
./scripts/produce-olist-ux-events.sh --max-events 100
KAFKA_TOPIC=ux-events MAX_MESSAGES=5 ./scripts/consume-kafka.sh
KAFKA_TOPIC=ux-events ./scripts/check-kafka-lag.sh
```

전체 샘플을 넣을 때는 `--max-events`를 빼면 됩니다.

```bash
./scripts/reset-olist-kafka-topics.sh ux-events
./scripts/produce-olist-ux-events.sh
```

샘플 기준 전체 `ux-events`는 16,693건입니다.

## Review/Order 변경 이벤트 replay

Paimon upsert 실습까지 이어갈 때는 review와 order status topic도 함께 준비합니다.

```bash
./scripts/reset-olist-kafka-topics.sh review-events order-status-events
./scripts/produce-olist-review-events.sh
./scripts/produce-olist-order-events.sh
```

샘플 기준 입력 건수입니다.

```text
ux-events              16,693
review-events           5,943
order-status-events     7,886
```

## Kafka UI

브라우저에서 Kafka UI를 엽니다.

```text
http://localhost:8088
```

확인할 항목입니다.

- `ux-events`, `review-events`, `order-status-events` topic이 있는가?
- partition count가 3개인가?
- message key가 `session_id`, `review_id`, `order_id`로 들어가는가?
- offset이 증가하는가?

## Replay vs CDC vs API

수업용 데이터는 과거 CSV를 JSONL 이벤트로 변환해서 Kafka에 다시 흘리는 replay입니다.

```text
Replay
  - 이미 존재하는 과거 데이터를 시간 순서대로 다시 흘림
  - 실습 재현성과 반복 실행에 좋음

CDC
  - DB의 insert/update/delete 변경 로그를 캡처해 흘림
  - 실제 운영의 current-state 동기화에 자주 사용

API
  - 외부 서비스가 제공하는 endpoint를 호출해 데이터를 가져옴
  - rate limit, pagination, 재시도 설계가 중요
```

같은 데이터를 다시 흘려도 결과가 항상 같지는 않습니다. Kafka offset, consumer group, sink primary key, startup mode가 결과를 바꿀 수 있습니다.

## 수업 중 사용할 질문

- Kafka topic에 메시지가 있다는 것과 downstream table이 정상이라는 것은 왜 다른 말일까요?
- `ux-events`는 왜 append가 자연스럽고, `review-events`는 왜 current-state로 접을까요?
- offset이 계속 증가하는 것은 어떤 사실을 의미하고, 어떤 사실은 말해주지 못할까요?
- lag가 커졌을 때 retention이 지나면 어떤 운영 리스크가 생길까요?
