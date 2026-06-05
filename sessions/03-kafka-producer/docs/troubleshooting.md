# 3차시 트러블슈팅

3차시는 Kafka broker, Kafka UI, producer, consumer만 봅니다. 문제가 나면 먼저 "어느 계층까지 정상인지"를 분리해서 확인합니다.

## Docker Desktop이 실행 중이 아닌 경우

증상입니다.

```text
Cannot connect to the Docker daemon
```

해결 방법입니다.

1. Docker Desktop을 실행합니다.
2. Docker Desktop 하단 또는 좌측 상태가 Running인지 확인합니다.
3. 다시 확인합니다.

```bash
docker ps
./scripts/check-env.sh
```

## Kafka port가 이미 사용 중인 경우

증상입니다.

```text
Bind for 0.0.0.0:9092 failed: port is already allocated
```

원인입니다.

다른 Kafka나 로컬 서비스가 이미 `9092` port를 사용 중입니다.

확인합니다.

```bash
lsof -nP -iTCP:9092 -sTCP:LISTEN
```

해결 방법입니다.

기존 Kafka를 종료하거나, 수업용 stack을 완전히 내린 뒤 다시 시작합니다.

```bash
docker compose -f docker-compose.lite.yml down
docker compose -f docker-compose.lite.yml up -d --build kafka kafka-init kafka-ui
```

## Kafka 컨테이너는 떴지만 topic 명령이 실패하는 경우

증상입니다.

```text
Failed to create new KafkaAdminClient
Connection to node -1 could not be established
```

해석입니다.

컨테이너가 `Up`인 것과 broker가 요청을 받을 준비가 된 것은 다릅니다. Kafka는 controller, listener, topic metadata가 준비되기까지 시간이 걸릴 수 있습니다.

확인합니다.

```bash
docker compose -f docker-compose.lite.yml ps kafka
docker compose -f docker-compose.lite.yml logs --tail=80 kafka
./scripts/smoke-test.sh
```

## `Failed to acquire idempotence PID`가 보이는 경우

증상입니다.

```text
Failed to acquire idempotence PID from broker
Coordinator load in progress
```

해석입니다.

producer가 멱등 전송을 위해 Producer ID를 받으려 했지만 Kafka coordinator가 아직 준비 중인 상황일 수 있습니다. Kafka가 막 뜬 직후에 잠깐 보일 수 있습니다.

판단 기준입니다.

```text
sent=100 topic=ux-events ...
```

처럼 최종 전송 결과가 성공이고 consumer로 메시지가 확인되면 일시적인 startup race로 봅니다. 최종 전송이 실패했다면 Kafka 로그와 topic 상태부터 확인합니다.

## Git Bash에서 Docker 경로가 이상하게 바뀌는 경우

증상입니다.

```text
kafka-topics.sh: no such file or directory
```

또는 Docker 컨테이너 내부 경로가 Windows 경로로 바뀐 것처럼 보입니다.

해결 방법입니다.

Git Bash는 `/workspace/...` 같은 경로를 Windows 경로로 자동 변환할 수 있습니다. 이 경우 아래처럼 실행합니다.

```bash
MSYS_NO_PATHCONV=1 ./scripts/produce-olist-ux-events.sh --max-events 100
```

가능하면 WSL2 Ubuntu 터미널 사용을 권장합니다.

## 스크립트가 `bash\r` 또는 `not found`로 실패하는 경우

증상입니다.

```text
/usr/bin/env: 'bash\r': No such file or directory
```

원인입니다.

Windows 줄바꿈(CRLF)이 shell script에 들어간 상황입니다.

해결 방법입니다.

```bash
find scripts -name "*.sh" -exec sed -i 's/\r$//' {} \;
chmod +x scripts/*.sh
```

## consumer lag가 비어 있거나 예상과 다른 경우

증상입니다.

```text
Consumer group ... does not exist
```

원인입니다.

lag는 topic 전체 지표가 아니라 consumer group 기준 지표입니다. `GROUP_ID`를 고정하지 않고 consume하면, 나중에 같은 group의 lag를 조회할 수 없습니다.

권장 흐름입니다.

```bash
GROUP_ID=de5-debug-consumer KAFKA_TOPIC=ux-events MAX_MESSAGES=5 ./scripts/consume-kafka.sh
GROUP_ID=de5-debug-consumer ./scripts/check-kafka-lag.sh
```

## 같은 sample을 replay했더니 offset이 다시 증가하는 경우

정상입니다.

Kafka에서 replay는 새 메시지를 다시 append하는 행위입니다. 멱등 producer는 producer 재시도 중복을 막는 것이지, 사람이 같은 파일을 다시 보내는 replay 중복까지 막지 않습니다.

확인합니다.

```bash
KAFKA_TOPIC=ux-events ./scripts/get-kafka-offsets.sh
./scripts/produce-olist-ux-events.sh --max-events 100
KAFKA_TOPIC=ux-events ./scripts/get-kafka-offsets.sh
```

offset이 증가하면 정상입니다. 4차시에서는 이 replay된 데이터를 Flink/Paimon에서 append fact와 current-state entity로 다르게 해석합니다.
