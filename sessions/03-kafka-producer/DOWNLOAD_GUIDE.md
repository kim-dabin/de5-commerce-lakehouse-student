# 3차시 다운로드/실행 가이드

## GitHub로 받은 경우

```bash
git clone https://github.com/kim-dabin/de5-commerce-lakehouse-student.git
cd de5-commerce-lakehouse-student
git pull
cd sessions/03-kafka-producer
```

이미 clone한 경우:

```bash
cd de5-commerce-lakehouse-student
git pull
cd sessions/03-kafka-producer
```

## 처음 실행 전 공통 확인

Windows 사용자는 WSL2 Ubuntu 또는 Git Bash에서 실행해주세요.

```bash
cp .env.example .env
./scripts/check-env.sh
```

## 이번 차시 대표 명령

```bash
docker compose -f docker-compose.lite.yml up -d --build kafka kafka-init kafka-ui
./scripts/smoke-test.sh
./scripts/reset-olist-kafka-topics.sh
./scripts/describe-olist-kafka-topics.sh
VERBOSE=true ./scripts/produce-olist-ux-events.sh --max-events 100
GROUP_ID=de5-debug-consumer KAFKA_TOPIC=ux-events MAX_MESSAGES=5 ./scripts/consume-kafka.sh
KAFKA_TOPIC=ux-events ./scripts/get-kafka-offsets.sh
GROUP_ID=de5-debug-consumer ./scripts/check-kafka-lag.sh
```

4차시 입력까지 미리 준비하려면 아래 명령도 실행합니다.

```bash
./scripts/reset-olist-kafka-topics.sh
./scripts/produce-olist-ux-events.sh
./scripts/produce-olist-review-events.sh
./scripts/produce-olist-order-events.sh
./scripts/describe-olist-kafka-topics.sh
```

## 확인해야 할 증거

- topic 3개가 존재하는가?
- 각 topic의 partition은 3개, RF는 1인가?
- producer 출력에서 `partition`, `offset`, `key`가 보이는가?
- consumer 출력에서 왼쪽 message key와 오른쪽 JSON payload가 보이는가?
- consumer group lag가 조회되는가?
- 같은 sample을 다시 replay했을 때 offset이 증가하는가?

실행이 실패하면 실행한 명령어와 터미널 에러 메시지를 함께 캡처해 디스코드 해당 차시 채널에 올려주세요.

첫 Docker 이미지 다운로드/빌드는 PC와 네트워크에 따라 30분 이상 걸릴 수 있습니다.
