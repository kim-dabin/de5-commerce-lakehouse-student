# 5차시 다운로드/실행 가이드

## GitHub로 받은 경우

```bash
git clone https://github.com/kim-dabin/de5-commerce-lakehouse-student.git
cd de5-commerce-lakehouse-student
git pull
cd sessions/05-project-mvp
```

이미 clone한 경우에는 아래만 실행하면 됩니다.

```bash
cd de5-commerce-lakehouse-student
git pull
cd sessions/05-project-mvp
```

## 처음 실행 전 공통 확인

```bash
cp .env.example .env
./scripts/check-env.sh
```

Docker Desktop을 먼저 실행해주세요.

## 오늘 수업 핵심 명령

```bash
docker compose -f docker-compose.lite.yml up -d --build
docker compose -f docker-compose.lite.yml ps

./scripts/query-olist-paimon.sh
./scripts/reset-realtime-olap.sh
./scripts/query-realtime-olap-metrics.sh

./scripts/reset-iceberg-tables.sh
./scripts/run-spark-iceberg-transform.sh
./scripts/query-iceberg-tables.sh
./scripts/query-bi-metrics.sh
```

BI 대시보드는 아래 명령으로 실행합니다.

```bash
./scripts/start-streamlit-bi.sh
```

접속:

```text
http://127.0.0.1:8501
```

Airflow까지 확인하려면:

```bash
./scripts/start-airflow.sh
./scripts/trigger-airflow-pipeline.sh
```

접속:

```text
http://localhost:8080
admin / admin
```

## 주의

- 첫 실행은 Docker 이미지 다운로드와 Spark package 다운로드 때문에 오래 걸릴 수 있습니다.
- 이전 차시 Docker stack이 이미 떠 있으면 같은 폴더 안에서 이어서 실행해도 됩니다.
- 명령이 실패하면 실행한 명령어와 에러 메시지를 함께 캡처해주세요.
