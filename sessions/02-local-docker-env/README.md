# 2차시 자료: 로컬 Docker 실습 환경

이 폴더는 데이터 엔지니어 부트캠프 5기 B주제 라이브 스터디의 2차시 수강생 실습 자료입니다.

## 이번 차시 범위

Docker Compose로 Kafka, Flink, Spark, Iceberg, MinIO, StarRocks가 포함된 로컬 실습 스택을 올리고 상태를 확인합니다.

## 먼저 확인할 것

1. Docker Desktop을 실행합니다.
2. 터미널에서 현재 차시 폴더로 이동합니다.
   - GitHub로 받은 경우: `de5-commerce-lakehouse-student/sessions/02-local-docker-env`
   - zip으로 받은 경우: 압축을 푼 2차시 자료 폴더
3. Windows 사용자는 WSL2 Ubuntu 또는 Git Bash에서 명령을 실행합니다.
4. Docker Desktop memory는 가능하면 10-12GB 정도로 설정합니다.
5. 처음 실행할 때는 아래 공통 명령을 먼저 실행합니다.

```bash
cp .env.example .env
./scripts/check-env.sh
```

## 2차시 시작 전 권장 작업

1차시 과제를 제출한 뒤, 가능하면 아래 Docker 실행까지 미리 해봅니다. 첫 Docker 이미지 다운로드/빌드는 PC와 네트워크에 따라 30분 이상 걸릴 수 있습니다.

```bash
docker compose -f docker-compose.lite.yml up -d --build
./scripts/smoke-test.sh
./scripts/produce-kafka.sh --max-events 10 --rate-per-second 2
MAX_MESSAGES=5 ./scripts/consume-kafka.sh
```

수업 시작 후에는 1차시 과제를 바탕으로 전원 데일리 스탠드업을 짧게 진행한 뒤, Docker Compose로 로컬 실습 환경을 함께 점검합니다.

환경 점검이 끝나면 과거 커머스 행동 로그 파일을 Kafka topic에 10건만 replay하고, consumer로 실제 JSON 이벤트를 읽어봅니다. Kafka의 자세한 구조는 3차시에서 다루고, 2차시에서는 "배치 로그를 스트림처럼 흘려보낸다"는 감각만 확인합니다.

## 대표 실행 명령

```bash
docker compose -f docker-compose.lite.yml up -d --build
./scripts/smoke-test.sh
./scripts/produce-kafka.sh --max-events 10 --rate-per-second 2
MAX_MESSAGES=5 ./scripts/consume-kafka.sh
```

## 제출/질문 기준

- 과제 제출은 디스코드의 해당 차시 채널에 올립니다.
- 2차시 과제 상세 기준은 `ASSIGNMENT.md`를 확인합니다.
- OpenMetadata 리니지 예시는 repo root의 `resources/openmetadata-lineage/README.md`에서 확인할 수 있습니다. OpenMetadata는 2차시 필수 설치 대상이 아닙니다.
- 실행이 실패하면 전체 화면보다 터미널의 에러 메시지와 실행한 명령어를 함께 캡처합니다.
- 로컬 PC와 네트워크에 따라 첫 Docker 이미지 다운로드/빌드는 30분 이상 걸릴 수 있습니다. 수업 직전에 처음 실행하지 말고 미리 빌드해두는 것을 권장합니다.
- 수강생 실습 PC는 RAM 16GB 이상을 기준으로 안내합니다. 기본 로컬 스택은 진행 가능하지만, 브라우저 탭/IDE/다른 Docker 프로젝트를 많이 켜두면 메모리 부족이 날 수 있습니다.

## 포함하지 않은 것

- 멘토 운영 문서, 출석/녹화 관리 문서, 내부 검토 자료는 수강생 배포 자료에 포함하지 않습니다.
- 참고 답안은 기본적으로 포함하지 않습니다. 멘토가 수업 이후 공유할 때만 `INCLUDE_SOLUTIONS=1`로 패키지를 다시 생성합니다.
