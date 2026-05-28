# 06차시 자료: Airflow 오케스트레이션

이 폴더는 데이터 엔지니어 부트캠프 5기 B주제 라이브 스터디의 06차시 수강생 실습 자료입니다.

## 이번 차시 범위

검증된 Kafka, Flink, Paimon, Spark, Iceberg 실행 순서를 Airflow DAG로 묶고 task log 기준으로 장애 위치를 찾습니다.

## 먼저 확인할 것

1. Docker Desktop을 실행합니다.
2. 터미널에서 압축을 푼 폴더로 이동합니다.
3. 처음 실행할 때는 아래 공통 명령을 먼저 실행합니다.

```bash
cp .env.example .env
./scripts/check-env.sh
```

## 대표 실행 명령

```bash
docker compose -f docker-compose.lite.yml up -d --build
./scripts/start-airflow.sh
./scripts/list-airflow-dags.sh
./scripts/trigger-airflow-pipeline.sh
```

## 제출/질문 기준

- 과제 제출은 디스코드의 해당 차시 채널에 올립니다.
- 실행이 실패하면 전체 화면보다 터미널의 에러 메시지와 실행한 명령어를 함께 캡처합니다.
- 로컬 PC 성능에 따라 첫 Docker 이미지 다운로드와 Spark 패키지 다운로드는 시간이 걸릴 수 있습니다.

## 포함하지 않은 것

- 멘토 운영 문서, 출석/녹화 관리 문서, 내부 검토 자료는 수강생 배포 자료에 포함하지 않습니다.
- 참고 답안은 기본적으로 포함하지 않습니다. 멘토가 수업 이후 공유할 때만 `INCLUDE_SOLUTIONS=1`로 패키지를 다시 생성합니다.
