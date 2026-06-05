# 3차시 기대 출력

이 문서는 3차시 Kafka 실습을 정상 실행했을 때 확인할 수 있는 대표 출력입니다. 공백, partition별 offset 숫자, row 순서는 실행 환경과 replay 횟수에 따라 달라질 수 있습니다.

## `docker compose -f docker-compose.lite.yml ps`

3차시 필수 서비스입니다.

```text
de5-kafka
de5-kafka-ui
```

`de5-kafka-init`는 topic을 만든 뒤 정상 종료됩니다.

## Topic describe

```bash
./scripts/describe-olist-kafka-topics.sh
```

대표 출력입니다.

```text
== topic: ux-events ==
Topic: ux-events	TopicId: ...	PartitionCount: 3	ReplicationFactor: 1	Configs:
	Topic: ux-events	Partition: 0	Leader: 1	Replicas: 1	Isr: 1
	Topic: ux-events	Partition: 1	Leader: 1	Replicas: 1	Isr: 1
	Topic: ux-events	Partition: 2	Leader: 1	Replicas: 1	Isr: 1
```

해석입니다.

```text
partition 3개, RF 1, ISR 1입니다.
브로커가 1개라 복제 안정성은 없지만, partition/offset/consumer 병렬성 개념을 보기에는 충분합니다.
```

## UX producer

```bash
VERBOSE=true ./scripts/produce-olist-ux-events.sh --max-events 100
```

대표 출력입니다.

```text
delivered topic=ux-events partition=0 offset=0 key=sess-...
delivered topic=ux-events partition=1 offset=0 key=sess-...
delivered topic=ux-events partition=2 offset=0 key=sess-...
sent=100 topic=ux-events input=/workspace/data/sample/olist/ux_events.jsonl
```

확인할 것:

```text
topic
partition
offset
key
sent count
```

## UX consumer

```bash
GROUP_ID=de5-debug-consumer KAFKA_TOPIC=ux-events MAX_MESSAGES=5 ./scripts/consume-kafka.sh
```

대표 출력입니다.

```text
sess-... | {"id":"ux-...","event_id":"ux-...","event_time":"2017-...","event_type":"product_view","user_id":...,"user_session":"sess-...","product_id":...}
```

왼쪽 값은 Kafka message key이고, 오른쪽 JSON은 실제 payload입니다.

## Topic offsets

```bash
KAFKA_TOPIC=ux-events ./scripts/get-kafka-offsets.sh
```

대표 출력입니다.

```text
ux-events:0:...
ux-events:1:...
ux-events:2:...
```

각 줄은 partition별 끝 offset입니다.

## Consumer group lag

```bash
GROUP_ID=de5-debug-consumer ./scripts/check-kafka-lag.sh
```

대표 출력입니다.

```text
GROUP               TOPIC      PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG  CONSUMER-ID  HOST  CLIENT-ID
de5-debug-consumer  ux-events  0          ...             ...             ...  -            -     -
```

해석입니다.

```text
lag = topic 끝 offset - consumer group이 처리한 offset
```

`GROUP_ID`를 고정하지 않고 consume하면 lag 조회가 비어 보일 수 있습니다.

## 같은 sample 재replay

```bash
./scripts/produce-olist-ux-events.sh --max-events 100
KAFKA_TOPIC=ux-events ./scripts/get-kafka-offsets.sh
```

기대 현상입니다.

```text
offset이 증가합니다.
```

해석입니다.

```text
멱등 producer는 producer 재시도 중복을 막습니다.
같은 파일을 다시 replay하는 것은 Kafka 입장에서 새로운 전송이므로 다시 append될 수 있습니다.
```

## 4차시 입력 이벤트

```bash
./scripts/reset-olist-kafka-topics.sh
./scripts/produce-olist-ux-events.sh
./scripts/produce-olist-review-events.sh
./scripts/produce-olist-order-events.sh
```

대표 결과입니다.

```text
sent=13023 topic=ux-events input=/workspace/data/sample/olist/ux_events.jsonl
sent=5943 topic=review-events input=/workspace/data/sample/olist/review_events.jsonl
sent=7886 topic=order-status-events input=/workspace/data/sample/olist/order_status_events.jsonl
```

## Web UI

Kafka UI:

```text
http://localhost:8088
```

확인할 것:

- `ux-events`, `review-events`, `order-status-events` topic 존재
- message key 표시
- partition과 offset 표시
- payload JSON 확인
