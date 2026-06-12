# 트러블슈팅

로컬 실습 중 자주 만날 수 있는 오류와 복구 방법입니다.

## Flink가 Paimon MinIO warehouse에 접근하지 못하는 경우

증상입니다.

```text
No FileSystem for scheme "s3"
Unable to access s3://paimon/warehouse
Connection refused: minio:9000
```

원인입니다.

Paimon warehouse는 MinIO의 `s3://paimon/warehouse`를 사용합니다. Flink image에 `paimon-s3` jar가 없거나, MinIO가 아직 준비되지 않았거나, catalog의 S3 endpoint/access key/path-style 설정이 맞지 않으면 접근에 실패합니다.

해결 방법입니다.

```bash
docker compose -f docker-compose.lite.yml up -d --build minio minio-init flink-jobmanager flink-taskmanager
./scripts/smoke-test.sh
./scripts/reset-olist-paimon.sh
```

`docker/flink/Dockerfile`에는 `paimon-flink`, `paimon-s3`, `flink-shaded-hadoop` jar가 포함되어 있어야 합니다. Flink SQL의 Paimon catalog는 `s3://paimon/warehouse`, `http://minio:9000`, `s3.path.style.access=true`를 사용합니다.

## Paimon catalog가 Hadoop configuration을 찾지 못하는 경우

증상입니다.

```text
java.lang.ClassNotFoundException: org.apache.hadoop.conf.Configuration
```

원인입니다.

Paimon filesystem catalog가 필요로 하는 Hadoop class가 Flink image에 없는 상황입니다.

해결 방법입니다.

```bash
docker compose -f docker-compose.lite.yml build flink-jobmanager
docker compose -f docker-compose.lite.yml up -d flink-jobmanager flink-taskmanager
```

커스텀 Flink image에는 `flink-shaded-hadoop-2-uber`가 포함되어 있습니다.

## Airflow UI port가 이미 사용 중인 경우

증상입니다.

```text
Bind for 0.0.0.0:8080 failed: port is already allocated
```

원인입니다.

다른 로컬 서비스가 이미 `8080` port를 사용하고 있습니다.

해결 방법입니다.

`.env`에서 다른 port를 지정한 뒤 Airflow를 다시 시작합니다.

```bash
AIRFLOW_HOST_PORT=18080
./scripts/start-airflow.sh
```

브라우저에서는 `http://localhost:18080`으로 접속합니다.

## Airflow가 Docker command를 실행하지 못하는 경우

증상입니다.

```text
permission denied while trying to connect to the Docker daemon socket
```

원인입니다.

이 로컬 수업용 구성에서는 Airflow scheduler가 `/var/run/docker.sock`에 접근해야 기존 Flink/Spark 컨테이너를 호출할 수 있습니다.

해결 방법입니다.

제공된 `orchestration` profile과 스크립트를 사용합니다.

```bash
./scripts/start-airflow.sh
./scripts/trigger-airflow-pipeline.sh
```

이 스택은 로컬 Docker socket 접근을 위해 Airflow를 `root`로 실행합니다. 프로덕션 환경에 그대로 가져가면 안 되는 수업용 패턴입니다.

## Airflow DAG가 보이지 않는 경우

증상입니다.

```text
de5_lite_lakehouse_pipeline
```

위 DAG가 목록에 보이지 않습니다.

해결 방법입니다.

Airflow가 실행 중인지, DAG directory가 mount되어 있는지 확인합니다.

```bash
docker compose -f docker-compose.lite.yml --profile orchestration ps
./scripts/list-airflow-dags.sh
```

scheduler가 DAG 파일을 인식하기 전에 먼저 시작된 경우 몇 초 기다린 뒤 list command를 다시 실행합니다.

## StarRocks table 생성이 오래 걸리는 경우

증상입니다.

`./scripts/reset-realtime-olap.sh` 실행 중 table 생성 단계가 몇 분 동안 멈춘 것처럼 보입니다.

원인입니다.

StarRocks FE와 CN이 처음 shared-data table을 만들 때 tablet 준비 시간이 필요합니다. Apple Silicon Docker Desktop에서는 첫 생성이 특히 느릴 수 있습니다.

해결 방법입니다.

아래 순서로 StarRocks 상태를 확인한 뒤 다시 실행합니다.

```bash
docker compose -f docker-compose.lite.yml ps starrocks-fe starrocks-cn
./scripts/start-realtime-olap.sh
./scripts/reset-realtime-olap.sh
```

처음 한 번 성공하면 이후 재실행은 더 빨라집니다.

## Streamlit BI가 batch 지표를 못 읽는 경우

증상입니다.

Streamlit 화면에서 batch chart가 비어 있거나 Iceberg query 오류가 보입니다.

원인입니다.

Iceberg analytics table이 아직 생성되지 않았거나 Spark transform이 실패한 상황입니다.

해결 방법입니다.

아래 batch 경로를 먼저 완료합니다.

```bash
./scripts/reset-kafka-topic.sh
./scripts/produce-kafka.sh
./scripts/reset-paimon-bronze.sh
./scripts/run-flink-paimon-bronze.sh
./scripts/reset-iceberg-tables.sh
./scripts/run-spark-iceberg-transform.sh
./scripts/query-iceberg-tables.sh
```
