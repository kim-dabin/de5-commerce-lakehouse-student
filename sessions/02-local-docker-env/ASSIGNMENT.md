# 2차시 과제: 로컬 실행 환경 점검 + 운영 관찰

이번 과제는 단순히 Docker를 띄웠는지 확인하는 과제가 아닙니다. 앞으로 만들 파이프라인을 운영 가능한 시스템으로 바라보기 위해, 실행 결과와 로그를 함께 남기는 것이 목표입니다.

## 제출 마감

3차시 시작 3시간 전까지 제출합니다.

## 제출 위치

Discord `2차시` 채널

## 필수 제출 1: 실행 검증

아래 실행 결과를 캡처해서 제출합니다.

```bash
./scripts/check-env.sh
docker compose -f docker-compose.lite.yml ps
./scripts/smoke-test.sh
./scripts/produce-kafka.sh --max-events 10 --rate-per-second 2
MAX_MESSAGES=5 ./scripts/consume-kafka.sh
```

추가로 Flink UI 또는 MinIO Console 중 하나를 브라우저로 열어 캡처합니다.

```text
Flink UI: http://localhost:8081
MinIO Console: http://localhost:9001
```

## 필수 제출 2: 운영 관찰

아래 명령도 실행하고 결과를 짧게 정리합니다.

```bash
docker compose -f docker-compose.lite.yml logs --tail=80 kafka
docker stats --no-stream
```

정리할 내용:

1. `kafka` 로그에서 정상 기동으로 보이는 단서 1개
2. `docker stats --no-stream`에서 CPU 또는 memory 사용량이 가장 눈에 띄는 컨테이너 1개
3. `container up`과 `service ready`가 왜 다른지 본인 말로 2-3문장

## 필수 제출 3: 실패 또는 장애 가설

성공했더라도 아래 중 하나를 골라 3-5문장으로 작성합니다.

- Docker build가 너무 오래 걸린다면 어디부터 확인할까?
- Kafka topic은 있는데 consumer가 메시지를 못 읽는다면 어디부터 볼까?
- MinIO bucket 생성은 됐는데 Iceberg REST가 준비되지 않는다면 어디부터 볼까?
- Flink UI는 열리는데 job이 없다면 이것은 장애일까, 정상일까?

정답을 맞히는 과제가 아닙니다. 운영 상황에서 어떤 순서로 확인할지 생각하는 것이 목적입니다.

## 심화 선택: 모니터링 관점

아래는 선택 과제입니다. Python/SQL이나 기본 파이프라인 구현에 익숙한 분들은 도전해보세요.

Kafka와 Flink는 JVM 기반 서비스입니다. 실제 운영에서는 JMX 등을 통해 JVM heap, GC, thread, CPU, broker/topic 지표를 봅니다. 오늘은 JMX를 설정하지 않지만, 아래 질문에 답해봅니다.

1. Kafka 또는 Flink 중 하나를 고릅니다.
2. 운영 중 반드시 보고 싶은 지표 3개를 적습니다.
3. 각 지표가 나빠졌을 때 어떤 문제가 의심되는지 한 줄씩 적습니다.

예시:

```text
Flink heap memory 사용량 - 계속 증가하면 state 또는 object 누적을 의심한다.
Kafka consumer lag - 증가하면 producer 유입량 대비 consumer 처리 속도 부족을 의심한다.
GC time - 길어지면 JVM 메모리 압박 또는 객체 생성량 증가를 의심한다.
```

## 3차시 스탠드업 준비

다음 수업 초반에는 전원 데일리 스탠드업으로 아래 3가지를 30-45초 안에 공유합니다.

1. 어디까지 실행됐는지
2. 가장 먼저 확인한 로그 또는 지표는 무엇인지
3. 3차시 Kafka 실습에서 확인하고 싶은 것 1개
