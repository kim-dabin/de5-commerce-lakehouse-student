# 3차시 자료: Kafka UX 이벤트 수집

이 폴더는 데이터 엔지니어 부트캠프 5기 B주제 라이브 스터디의 3차시 수강생 실습 자료입니다.

## 이번 차시 범위

Olist 기반 UXLog, review, order status JSONL 샘플을 Kafka topic에 넣고, consumer와 offset으로 수집 상태를 확인합니다.

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

## 대표 실행 명령

```bash
docker compose -f docker-compose.lite.yml up -d --build
./scripts/reset-olist-kafka-topics.sh
./scripts/produce-olist-ux-events.sh --max-events 100
KAFKA_TOPIC=ux-events MAX_MESSAGES=5 ./scripts/consume-kafka.sh
KAFKA_TOPIC=ux-events ./scripts/check-kafka-lag.sh
./scripts/produce-olist-review-events.sh
./scripts/produce-olist-order-events.sh
```





## 제출/질문 기준

- 과제 제출은 디스코드의 해당 차시 채널에 올립니다.
- 실행이 실패하면 전체 화면보다 터미널의 에러 메시지와 실행한 명령어를 함께 캡처합니다.
- 로컬 PC와 네트워크에 따라 첫 Docker 이미지 다운로드/빌드는 30분 이상 걸릴 수 있습니다. 수업 직전에 처음 실행하지 말고 미리 빌드해두는 것을 권장합니다.

## 포함하지 않은 것

- 멘토 운영 문서, 출석/녹화 관리 문서, 내부 검토 자료는 수강생 배포 자료에 포함하지 않습니다.
- 참고 답안은 기본적으로 포함하지 않습니다. 멘토가 수업 이후 공유할 때만 `INCLUDE_SOLUTIONS=1`로 패키지를 다시 생성합니다.
