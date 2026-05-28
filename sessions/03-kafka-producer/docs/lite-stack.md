# Lite Stack

이 문서는 데이터 엔지니어 부트캠프 5기 B주제 스터디에서 사용하는 첫 로컬 실습 스택을 설명합니다.

## 목표

Lite stack은 프로덕션 배포용이 아니라 수업용 실습 스택입니다. 목적은 가장 얇은 기준 경로를 로컬에서 검증하는 것입니다.

```text
sample Commerce Events
  -> Kafka
  -> Flink
  -> Paimon
  -> Spark
  -> Iceberg REST Catalog
  -> MinIO
```

이 경로가 안정화된 뒤 StarRocks를 realtime OLAP serving layer로 붙입니다. Airflow는 같은 실행 흐름을 DAG로 묶는 오케스트레이션 계층으로 사용합니다.

## 서비스

| 서비스 | 포트 | 역할 |
|---|---:|---|
| Kafka | 9092 | 로컬 KRaft broker |
| Flink JobManager | 8081 | Flink Web UI |
| MinIO S3 API | 9000 | Object storage API |
| MinIO Console | 9001 | Object storage UI |
| Iceberg REST | 8181 | Iceberg REST catalog |
| Postgres | 15432 | Iceberg REST catalog backend |
| Spark client | 없음 | Batch job 실행 컨테이너 |
| Kafka producer | 없음 | 샘플 이벤트 producer 컨테이너 |
| StarRocks FE | 8030, 9030 | Realtime OLAP frontend |
| StarRocks CN | 없음 | Realtime OLAP compute node |
| Airflow | 8080 | 선택 오케스트레이션 UI |

## 시작

```bash
cp .env.example .env
./scripts/check-env.sh
docker compose -f docker-compose.lite.yml up -d --build
./scripts/smoke-test.sh
```

명령을 실행하기 전에 Docker Desktop을 먼저 켜야 합니다. `check-env.sh`는 Docker daemon이 준비되지 않았을 때 초기에 실패하도록 만든 진단 스크립트입니다.

Iceberg catalog용 Postgres는 기본적으로 host port `15432`를 사용합니다. 로컬에 이미 Postgres가 `5432`로 떠 있는 상황을 피하기 위한 설정입니다.

첫 빌드에서는 Kafka, Flink, Spark, Iceberg, MinIO, Postgres, StarRocks 이미지를 내려받기 때문에 시간이 걸릴 수 있습니다. 수업 전에는 아래 명령으로 미리 빌드해두는 것을 권장합니다.

```bash
docker compose -f docker-compose.lite.yml build
```

host port가 이미 사용 중이면 `.env`에서 해당 port 값을 바꾼 뒤 다시 시작합니다.

## 중지와 초기화

스택을 중지합니다.

```bash
docker compose -f docker-compose.lite.yml down
```

로컬 volume까지 포함해 실습 상태를 초기화합니다.

```bash
./scripts/reset-local-state.sh
```

멘토 전용 OpenMetadata 데모까지 함께 중지하려면 아래 옵션을 사용합니다.

```bash
./scripts/reset-local-state.sh --with-openmetadata
```

OpenMetadata의 로컬 MySQL bind data까지 삭제해야 할 때만 아래 강한 초기화 옵션을 사용합니다.

```bash
./scripts/reset-local-state.sh --purge-openmetadata-bind-data
```

## 주요 URL

- Flink: http://localhost:8081
- MinIO Console: http://localhost:9001
- Iceberg REST: http://localhost:8181
- StarRocks FE: http://localhost:8030
- Airflow: http://localhost:8080

MinIO 기본 계정입니다.

```text
user: minioadmin
password: minioadmin
```

Airflow 기본 계정입니다.

```text
user: admin
password: admin
```

## 참고

- Flink는 커스텀 로컬 이미지를 사용합니다. Kafka SQL connector, Paimon Flink bundle, Hadoop shaded jar를 `/opt/flink/lib`에 넣어두기 위해서입니다.
- Paimon은 첫 실습의 S3 의존성을 낮추기 위해 `/warehouse/paimon` Docker volume을 사용합니다.
- Iceberg REST는 table data를 MinIO에 저장하고 catalog metadata를 Postgres에 저장합니다.
- Spark는 batch job 실행용 client 컨테이너입니다. Spark 실습 스크립트는 실행 시점에 Paimon/Iceberg runtime package를 추가하고 shared Ivy volume에 cache합니다.
- StarRocks는 shared-data 형태로 실행합니다. FE와 CN이 뜨고 table data는 MinIO object storage에 저장합니다.
- Airflow는 `orchestration` Compose profile로 실행됩니다. 수업용 로컬 패턴에서는 Docker socket을 통해 이미 떠 있는 Flink/Spark 컨테이너를 호출합니다. 이 보안 모델을 프로덕션에 그대로 적용하면 안 됩니다.
