# 03 Kafka Producer

## 목표

Olist 기반 수업용 이벤트를 Kafka topic에 replay하고, broker에서 메시지를 읽어 정상 수집 여부를 확인합니다.

핵심은 실행 성공이 아니라 아래 증거를 남기는 것입니다.

```text
topic 존재
message key와 payload
partition별 offset 증가
consumer group lag
```

## 사전 준비

Kafka stack을 먼저 실행합니다.

```bash
docker compose -f docker-compose.lite.yml up -d --build kafka kafka-init kafka-ui
./scripts/smoke-test.sh
```

## Topic 설정 확인

```bash
./scripts/reset-olist-kafka-topics.sh
./scripts/describe-olist-kafka-topics.sh
```

확인할 내용입니다.

```text
Topic: ux-events
PartitionCount: 3
ReplicationFactor: 1
Replicas: 1
Isr: 1
```

## UX 이벤트 전송

처음에는 100건만 보냅니다.

```bash
VERBOSE=true ./scripts/produce-olist-ux-events.sh --max-events 100
```

기대 출력입니다.

```text
delivered topic=ux-events partition=... offset=... key=sess-...
sent=100 topic=ux-events input=/workspace/data/sample/olist/ux_events.jsonl
```

## 메시지 읽기

```bash
GROUP_ID=de5-debug-consumer \
KAFKA_TOPIC=ux-events \
MAX_MESSAGES=5 \
./scripts/consume-kafka.sh
```

기대 출력입니다.

```text
sess-... | {"id":"ux-...","event_type":"product_view","session_id":"sess-...",...}
```

왼쪽 값은 Kafka message key이고, 오른쪽 JSON이 실제 event payload입니다.

## Offset과 Lag 확인

topic 끝 offset을 확인합니다.

```bash
KAFKA_TOPIC=ux-events ./scripts/get-kafka-offsets.sh
```

consumer group lag를 확인합니다.

```bash
GROUP_ID=de5-debug-consumer ./scripts/check-kafka-lag.sh
```

## Replay 관찰

같은 sample을 다시 보내면 Kafka offset은 증가합니다.

```bash
./scripts/produce-olist-ux-events.sh --max-events 100
KAFKA_TOPIC=ux-events ./scripts/get-kafka-offsets.sh
```

멱등 producer는 producer 재시도 중복을 막는 것이지, 같은 파일을 다시 replay하는 것까지 막지는 않습니다.

## 4차시 입력 준비

```bash
./scripts/reset-olist-kafka-topics.sh
./scripts/produce-olist-ux-events.sh
./scripts/produce-olist-review-events.sh
./scripts/produce-olist-order-events.sh
```

기대 입력입니다.

```text
ux-events            13,023건
review-events         5,943건
order-status-events   7,886건
```

## 수업 중 사용할 질문

- message key를 바꾸면 partition 분배가 어떻게 달라질까요?
- ux-events는 왜 `session_id`를 key로 쓸까요?
- review-events는 왜 `review_id`를 key로 쓸까요?
- order-status-events는 왜 `order_id`를 key로 쓸까요?
- offset은 무엇을 나타낼까요?
- lag는 topic 기준일까요, consumer group 기준일까요?
- 멱등 producer는 replay 중복까지 막아줄까요?
