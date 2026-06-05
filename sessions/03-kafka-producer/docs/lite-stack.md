# 3차시 Kafka 실습 스택

이 문서는 3차시에서 사용하는 로컬 Docker 스택을 설명합니다.

## 이번 차시에서 실제로 쓰는 서비스

3차시는 Kafka 수집 계층만 다룹니다.

```text
Olist sample JSONL
  -> Kafka producer
  -> Kafka topic
  -> Kafka consumer / Kafka UI
```

## 필수 서비스

| 서비스 | 포트 | 역할 |
|---|---:|---|
| Kafka | 9092 | 로컬 KRaft broker |
| Kafka UI | 8088 | topic/message 확인 |
| Kafka producer | 없음 | JSONL 이벤트 replay |

## 왜 compose 파일에는 다른 서비스도 있나요?

`docker-compose.lite.yml`에는 4차시 이후에 사용할 Flink, Paimon, Spark, Iceberg, StarRocks 관련 서비스도 함께 들어 있습니다.

하지만 3차시 수업에서는 아래처럼 Kafka 관련 서비스만 띄웁니다.

```bash
docker compose -f docker-compose.lite.yml up -d --build kafka kafka-init kafka-ui
```

3차시에서 Flink/Paimon/StarRocks/BI까지 실행하지 않는 이유는, 오늘의 목표가 "Kafka 수집이 정상인지 증거로 설명하는 것"이기 때문입니다.

## 시작

```bash
cp .env.example .env
./scripts/check-env.sh
docker compose -f docker-compose.lite.yml up -d --build kafka kafka-init kafka-ui
./scripts/smoke-test.sh
```

명령을 실행하기 전에 Docker Desktop을 먼저 켜야 합니다. `check-env.sh`는 Docker daemon이 준비되지 않았을 때 초기에 실패하도록 만든 진단 스크립트입니다.

## 주요 URL

Kafka UI:

```text
http://localhost:8088
```

Kafka UI에서 확인할 것:

- `ux-events`, `review-events`, `order-status-events` topic
- message key
- partition
- offset
- JSON payload

## 중지와 초기화

Kafka stack을 중지합니다.

```bash
docker compose -f docker-compose.lite.yml down
```

로컬 volume까지 포함해 실습 상태를 초기화합니다.

```bash
./scripts/reset-local-state.sh
```

주의: `reset-local-state.sh`는 Kafka data volume도 삭제합니다. 이전에 produce한 메시지도 사라집니다.

## 3차시에서 남겨야 할 증거

| 증거 | 확인 방법 |
|---|---|
| topic 존재 | `./scripts/describe-olist-kafka-topics.sh` |
| partition/RF/ISR | `./scripts/describe-olist-kafka-topics.sh` |
| message key/payload | `./scripts/consume-kafka.sh` 또는 Kafka UI |
| partition별 offset | `./scripts/get-kafka-offsets.sh` |
| consumer lag | `./scripts/check-kafka-lag.sh` |
