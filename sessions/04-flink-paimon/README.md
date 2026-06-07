# 4차시 자료: Flink와 Paimon Bronze

이 폴더는 데이터 엔지니어 부트캠프 5기 B주제 라이브 스터디의 4차시 수강생 실습 자료입니다.

## 이번 차시 범위

Kafka의 ux-events append 로그와 review/order 변경 이벤트를 Flink SQL로 읽어 Paimon ux_events_bronze, review_current, order_current 테이블에 저장합니다.

## 먼저 확인할 것

1. Docker Desktop을 실행합니다.
2. 터미널에서 현재 차시 폴더로 이동합니다.
   - GitHub로 받은 경우: `de5-commerce-lakehouse-student/sessions/04-flink-paimon`
   - zip으로 받은 경우: 압축을 푼 4차시 자료 폴더
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
./scripts/produce-olist-ux-events.sh
./scripts/produce-olist-review-events.sh
./scripts/produce-olist-order-events.sh
./scripts/reset-olist-paimon.sh
./scripts/run-flink-olist-paimon.sh
./scripts/query-olist-paimon.sh
```



## (선택) UI로 직접 쿼리하기

Paimon/Iceberg 결과를 코드나 SQL로 탐색하고 싶다면 query 도구를 함께 띄웁니다.

```bash
docker compose -f docker-compose.lite.yml --profile query up -d --build
```

| 도구 | URL | 용도 |
|---|---|---|
| JupyterLab (PySpark) | http://localhost:8888/lab | `notebooks/de5-spark-starter.ipynb` 실행. 커널 목록에서 **PySpark (DE5 Lakehouse)** 를 고르면 `spark` 세션이 catalog까지 연결된 채 자동 준비됩니다. |
| CloudBeaver | http://localhost:8978 | 웹 SQL IDE. MySQL 드라이버로 `starrocks-fe:9030` (user `root`, 비밀번호 없음) 연결 후 `paimon_olist`/`iceberg_olist` 카탈로그 조회. |

JupyterLab 첫 셀(SparkSession)은 JVM 기동으로 30초~1분 걸리고 이후 셀은 빠릅니다.

## 제출/질문 기준

- 과제 제출은 디스코드의 해당 차시 채널에 올립니다.
- 실행이 실패하면 전체 화면보다 터미널의 에러 메시지와 실행한 명령어를 함께 캡처합니다.
- 로컬 PC와 네트워크에 따라 첫 Docker 이미지 다운로드/빌드는 30분 이상 걸릴 수 있습니다. 수업 직전에 처음 실행하지 말고 미리 빌드해두는 것을 권장합니다.

## 포함하지 않은 것

- 멘토 운영 문서, 출석/녹화 관리 문서, 내부 검토 자료는 수강생 배포 자료에 포함하지 않습니다.
- 참고 답안은 기본적으로 포함하지 않습니다. 멘토가 수업 이후 공유할 때만 `INCLUDE_SOLUTIONS=1`로 패키지를 다시 생성합니다.
