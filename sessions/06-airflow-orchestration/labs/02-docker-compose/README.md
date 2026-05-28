# 02 Docker Compose

로컬 실습 스택을 Docker Compose로 실행하고 각 서비스가 정상적으로 준비되었는지 확인합니다.

## 목표

- `docker-compose.lite.yml`이 어떤 서비스를 띄우는지 이해합니다.
- Kafka, Flink, Spark, Iceberg, MinIO, StarRocks의 container 상태를 확인합니다.
- 이후 차시에서 반복 사용할 reset, run, query 스크립트의 위치를 익힙니다.

## 실행

```bash
cp .env.example .env
./scripts/check-env.sh
docker compose -f docker-compose.lite.yml up -d --build
./scripts/smoke-test.sh
```

첫 실행은 Docker image 다운로드 때문에 시간이 오래 걸릴 수 있습니다.

## 상태 확인

```bash
docker compose -f docker-compose.lite.yml ps
```

Flink UI와 MinIO Console도 브라우저에서 확인합니다.

```text
Flink: http://localhost:8081
MinIO Console: http://localhost:9001
```

## 중지

```bash
docker compose -f docker-compose.lite.yml down
```

## 초기화

```bash
./scripts/reset-local-state.sh
```

## 수업 중 사용할 질문

- container가 떠 있다는 것과 애플리케이션이 ready 상태라는 것은 어떻게 다를까요?
- 수업용 Docker Compose와 프로덕션 배포는 어떤 점이 다를까요?
- 실습이 실패했을 때 바로 전체 삭제를 하기보다 어떤 로그를 먼저 봐야 할까요?
