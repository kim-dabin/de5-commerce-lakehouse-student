# 03 Kafka Producer Preview

2차시에서는 Kafka를 깊게 다루지 않습니다. 대신 과거 커머스 행동 로그 파일이 Kafka topic으로 들어가면 어떤 모습인지 짧게 확인합니다.

## 오늘 확인할 것

- `data/sample/commerce_events_sample.jsonl`은 과거 커머스 행동 로그 파일입니다.
- `producer.py`는 이 파일을 한 줄씩 읽어 Kafka topic에 발행합니다.
- 이 방식은 CDC가 아니라 event replay/log replay입니다.
- partition, offset, consumer group은 3차시에서 본격적으로 다룹니다.

## 샘플 이벤트 10건 전송

```bash
./scripts/produce-kafka.sh --max-events 10 --rate-per-second 2
```

## topic에서 5건 읽기

```bash
MAX_MESSAGES=5 ./scripts/consume-kafka.sh
```

기대 출력은 아래와 비슷합니다.

```text
34700982758d19ec-a472f96a | {"id":"evt-202605-000001",...}
```

왼쪽은 Kafka message key이고, 오른쪽 JSON이 실제 event payload입니다.

## 수업 중 사용할 질문

- 파일에 있던 과거 로그가 Kafka topic에 들어가면 왜 스트리밍 입력처럼 다룰 수 있을까요?
- 오늘 확인한 것은 Kafka의 어떤 부분이고, 아직 확인하지 않은 부분은 무엇일까요?
- 같은 JSON 이벤트가 이후 Flink, Paimon, Spark, Iceberg로 넘어가면 어떤 역할을 하게 될까요?
