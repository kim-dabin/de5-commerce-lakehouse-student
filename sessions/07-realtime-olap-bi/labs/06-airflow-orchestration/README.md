# 06 Airflow Orchestration

## 목표

검증된 Lite Lakehouse 경로를 Airflow DAG로 묶어 실행 순서와 task log를 관리합니다.

```text
Airflow DAG
  -> reset Kafka topic
  -> produce sample Commerce Events
  -> run Flink SQL into Paimon Bronze
  -> run Spark batch transform into Iceberg
  -> query final Iceberg tables
```

Airflow는 Kafka, Flink, Spark, Paimon, Iceberg를 대체하지 않습니다. 이 실습에서는 task 순서를 결정하고 실행 로그를 남기는 역할만 합니다.

## Airflow 시작

core stack을 먼저 실행한 뒤 Airflow를 시작합니다.

```bash
docker compose -f docker-compose.lite.yml up -d --build
./scripts/start-airflow.sh
```

Airflow UI입니다.

```text
http://localhost:8080
username: admin
password: admin
```

port `8080`이 이미 사용 중이면 `.env`에서 `AIRFLOW_HOST_PORT`를 변경합니다.

## DAG 실행

터미널에서 실행합니다.

```bash
./scripts/trigger-airflow-pipeline.sh
```

또는 Airflow UI에서 `de5_lite_lakehouse_pipeline` DAG를 열고 직접 trigger합니다.

## 기대 task 순서

1. `reset_kafka_topic`
2. `produce_sample_events`
3. `reset_paimon_bronze`
4. `run_flink_paimon_bronze`
5. `query_paimon_bronze`
6. `reset_iceberg_tables`
7. `run_spark_iceberg_transform`
8. `query_iceberg_tables`

## 기대 최종 출력

`query_iceberg_tables` task log에서 아래 값을 확인합니다.

```text
clean_row_count
240
```

일 단위 커머스 이벤트 summary도 함께 보여야 합니다.

```text
event_date   event_type          event_count  user_count  session_count  product_count  revenue
2026-05-01   cart                ...
2026-05-01   purchase            ...
2026-05-01   remove_from_cart    ...
2026-05-01   view                ...
```

## 동작 방식

- Kafka reset과 sample event production은 Airflow DAG 내부 Python task로 실행합니다.
- Flink SQL은 이미 떠 있는 `de5-flink-jobmanager` container 안에서 Docker 명령으로 실행합니다.
- Spark SQL과 Spark submit은 이미 떠 있는 `de5-spark-client` container 안에서 Docker 명령으로 실행합니다.
- Airflow는 이 수업용 로컬 오케스트레이션 패턴 때문에 Docker socket 접근 권한이 필요합니다.

## 수업 중 사용할 질문

- Airflow가 제어하는 것과 제어하지 않는 것은 무엇일까요?
- 무거운 처리는 왜 Airflow task 자체가 아니라 Flink/Spark 안에서 실행하는 편이 안전할까요?
- 이 pipeline에서 retry가 가장 도움이 되는 task는 어디일까요?
- 최종 Iceberg table이 비어 있다면 어떤 task log부터 확인해야 할까요?
