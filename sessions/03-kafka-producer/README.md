# 3차시 자료: Kafka 수집과 이벤트 replay

이 폴더는 데이터 엔지니어 부트캠프 5기 B주제 라이브 스터디의 3차시 수강생 실습 자료입니다.

## 이번 차시 범위

Olist 기반 수업용 이벤트를 Kafka topic에 replay하고, topic, message key, partition, offset, lag로 수집 상태를 검증합니다.

이번 차시의 목표는 "Kafka에 넣었다"가 아닙니다.

```text
Kafka 수집이 어디까지 정상인지 증거로 설명한다.
```

## 먼저 확인할 것

1. Docker Desktop을 실행합니다.
2. 터미널에서 현재 차시 폴더로 이동합니다.
   - GitHub로 받은 경우: `de5-commerce-lakehouse-student/sessions/03-kafka-producer`
   - zip으로 받은 경우: 압축을 푼 3차시 자료 폴더
3. Windows 사용자는 WSL2 Ubuntu 또는 Git Bash에서 명령을 실행합니다.
4. 처음 실행할 때는 아래 공통 명령을 먼저 실행합니다.

```bash
cp .env.example .env
./scripts/check-env.sh
```

## 실습 흐름

| 단계 | 명령 | 확인할 개념 |
|---:|---|---|
| 1 | Kafka stack 실행 | broker, topic 입구, Kafka UI |
| 2 | topic reset/describe | partition, RF, ISR |
| 3 | UXLog 100건 replay | message key, producer ack, partition/offset |
| 4 | consumer 5건 읽기 | key와 payload 확인 |
| 5 | lag 확인 | topic 끝 offset과 consumer offset 차이 |
| 6 | 같은 sample 재replay | replay는 새 append라는 점 |
| 7 | review/order 이벤트 준비 | key 설계와 4차시 current-state 연결 |

## 1. Kafka stack 실행

3차시는 Kafka와 Kafka UI만 사용합니다.

```bash
docker compose -f docker-compose.lite.yml up -d --build kafka kafka-init kafka-ui
./scripts/smoke-test.sh
```

Kafka UI:

```text
http://localhost:8088
```

## 2. topic reset과 설정 확인

```bash
./scripts/reset-olist-kafka-topics.sh
./scripts/describe-olist-kafka-topics.sh
```

확인할 것:

```text
PartitionCount: 3
ReplicationFactor: 1
Leader: 1
Replicas: 1
Isr: 1
```

해석:

```text
브로커 1개 로컬 실습이라 RF=1입니다.
partition은 3개라 key에 따라 메시지가 나뉘고, partition별 offset이 따로 증가합니다.
```

## 3. UXLog 100건 replay

처음에는 일부러 100건만 보냅니다. 이 단계는 전체 적재가 아니라 producer 출력에서 key, partition, offset을 보는 단계입니다.

```bash
VERBOSE=true ./scripts/produce-olist-ux-events.sh --max-events 100
```

확인할 출력:

```text
delivered topic=ux-events partition=... offset=... key=sess-...
sent=100 topic=ux-events input=/workspace/data/sample/olist/ux_events.jsonl
```

해석:

```text
key는 session_id입니다.
같은 세션의 행동 흐름을 같은 partition에 모아 순서를 해석하기 쉽게 하기 위한 설계입니다.
```

## 4. consumer로 JSON 5건 읽기

consumer group id를 고정해서 읽습니다. 그래야 뒤에서 lag를 볼 수 있습니다.

```bash
GROUP_ID=de5-debug-consumer \
KAFKA_TOPIC=ux-events \
MAX_MESSAGES=5 \
./scripts/consume-kafka.sh
```

확인할 것:

```text
<message-key> | {"id":"...","event_type":"...","session_id":"...","product_id":...}
```

해석:

```text
왼쪽은 Kafka message key이고, 오른쪽은 실제 JSON payload입니다.
message key와 payload 안의 session_id가 같은지 확인합니다.
```

## 5. offset과 lag 확인

topic 끝 offset을 확인합니다.

```bash
KAFKA_TOPIC=ux-events ./scripts/get-kafka-offsets.sh
```

consumer group lag를 확인합니다.

```bash
GROUP_ID=de5-debug-consumer ./scripts/check-kafka-lag.sh
```

해석:

```text
lag = topic 끝 offset - consumer group이 처리한 offset
```

100건을 replay하고 5건만 consume했다면 lag는 남아 있어야 정상입니다. 정확한 partition별 숫자는 key 분포에 따라 달라질 수 있습니다.

## 6. 같은 sample 재replay

같은 100건을 다시 replay합니다.

```bash
./scripts/produce-olist-ux-events.sh --max-events 100
KAFKA_TOPIC=ux-events ./scripts/get-kafka-offsets.sh
```

해석:

```text
offset은 증가합니다.
멱등 producer는 producer 재시도 중복을 막는 것이지, 우리가 같은 파일을 다시 replay하는 것까지 막지 않습니다.
```

## 7. 4차시 입력 이벤트 준비

4차시에서 Kafka -> Flink -> Paimon으로 이어지려면 세 topic이 모두 필요합니다.

```bash
./scripts/reset-olist-kafka-topics.sh
./scripts/produce-olist-ux-events.sh
./scripts/produce-olist-review-events.sh
./scripts/produce-olist-order-events.sh
./scripts/describe-olist-kafka-topics.sh
```

기대 입력:

```text
ux-events            13,023건
review-events         5,943건
order-status-events   7,886건
```

topic별 message key:

| Topic | Message key | 이유 |
|---|---|---|
| `ux-events` | `session_id` | 같은 세션의 행동 순서 확인 |
| `review-events` | `review_id` | 같은 리뷰의 상태 변화 순서 확인 |
| `order-status-events` | `order_id` | 같은 주문의 상태 변화 순서 확인 |

## 제출/질문 기준

- 과제 제출은 디스코드의 해당 차시 채널에 올립니다.
- 실행이 실패하면 전체 화면보다 터미널의 에러 메시지와 실행한 명령어를 함께 캡처합니다.
- 성공 캡처만 올리지 말고, "무엇을 정상 증거로 봤는지" 한 줄을 같이 적습니다.
- 로컬 PC와 네트워크에 따라 첫 Docker 이미지 다운로드/빌드는 30분 이상 걸릴 수 있습니다. 수업 직전에 처음 실행하지 말고 미리 빌드해두는 것을 권장합니다.

## 포함하지 않은 것

- 3차시에서는 Flink/Paimon 적재를 완료하지 않습니다.
- 3차시에서는 StarRocks/BI를 실행하지 않습니다.
- 멘토 운영 문서, 출석/녹화 관리 문서, 내부 검토 자료는 수강생 배포 자료에 포함하지 않습니다.
