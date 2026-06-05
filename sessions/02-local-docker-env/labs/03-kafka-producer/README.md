# 03 Kafka Producer

## 목표

커머스 이벤트 샘플을 Kafka topic에 넣고 broker에서 메시지를 읽어 정상 수집 여부를 확인합니다.

## 사전 준비

Lite stack을 먼저 실행합니다.

```bash
docker compose -f docker-compose.lite.yml up -d --build
./scripts/smoke-test.sh
```

## 샘플 이벤트 전송

권장 경로는 Docker 기반 스크립트입니다.

```bash
./scripts/produce-kafka.sh
```

로컬 Python으로 직접 실행할 수도 있습니다.

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r labs/03-kafka-producer/requirements.txt
python labs/03-kafka-producer/producer.py \
  --bootstrap-server localhost:9092 \
  --topic commerce-events \
  --input data/sample/commerce_events_sample.jsonl
```

## 메시지 읽기

```bash
./scripts/consume-kafka.sh
```

기대 출력입니다.

```text
34700982758d19ec-a472f96a | {"id":"evt-202605-000001",...}
```

왼쪽 값은 Kafka message key이고, 오른쪽 JSON이 실제 event payload입니다.

## 유용한 변형

천천히 전송합니다.

```bash
./scripts/produce-kafka.sh --rate-per-second 1
```

같은 파일을 여러 번 반복 전송합니다.

```bash
./scripts/produce-kafka.sh --repeat 3
```

`user_id`를 Kafka message key로 사용합니다.

```bash
./scripts/produce-kafka.sh --key-field user_id
```

## Offset 확인

```bash
docker compose -f docker-compose.lite.yml exec kafka \
  /opt/kafka/bin/kafka-get-offsets.sh \
  --bootstrap-server kafka:19092 \
  --topic commerce-events
```

consumer group lag를 확인합니다.

```bash
GROUP_ID=de5-debug-consumer ./scripts/consume-kafka.sh
./scripts/check-kafka-lag.sh
```

반복 실행으로 topic 상태가 헷갈리면 topic을 초기화합니다.

```bash
./scripts/reset-kafka-topic.sh
```

## 수업 중 사용할 질문

- message key를 바꾸면 partition 분배가 어떻게 달라질까요?
- 이 topic을 partition 3개로 만든 이유는 무엇일까요?
- offset은 무엇을 나타낼까요?
- producer에서 retry와 delivery acknowledgement가 왜 중요할까요?
