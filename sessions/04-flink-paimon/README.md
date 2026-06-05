# 4차시 자료: Flink와 Paimon Bronze

이 폴더는 데이터 엔지니어 부트캠프 5기 B주제 라이브 스터디의 4차시 수강생 실습 자료입니다.

## 이번 차시 범위

Kafka의 ux-events append 로그와 review-events 변경 이벤트를 Flink SQL로 읽어 Paimon ux_events_bronze와 review_current 테이블에 저장합니다.

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

4차시 본 실습은 Kafka 이벤트를 Flink로 읽고 Paimon 테이블에 적재한 뒤, `row_count`와 분포를 확인하는 것까지입니다.

```bash
docker compose -f docker-compose.lite.yml up -d --build
./scripts/reset-olist-kafka-topics.sh ux-events review-events
./scripts/produce-olist-ux-events.sh
./scripts/produce-olist-review-events.sh
./scripts/reset-olist-paimon.sh
./scripts/run-flink-olist-paimon.sh
./scripts/query-olist-paimon.sh
```

## 선택: BI 화면까지 확인하기

아래 명령은 4차시 필수 과제 범위를 넘어, 최종 프로젝트 결과물을 미리 확인하기 위한 선택 실행입니다.

흐름은 다음과 같습니다.

```text
Kafka -> Flink -> Paimon
Paimon -> StarRocks external catalog -> Streamlit BI
Paimon -> Spark -> Iceberg -> StarRocks external catalog -> Streamlit BI
```

한 번에 전체 파이프라인과 BI 지표를 확인하려면 아래 명령을 실행합니다.

```bash
./scripts/run-olist-bi-pipeline.sh
./scripts/start-streamlit-bi.sh
```

대시보드 주소:

```text
http://127.0.0.1:8501
```

대시보드에는 두 개의 탭이 있습니다.

- `Lakehouse Ops · StarRocks(Paimon)`: Paimon `ux_events_bronze`, `review_current`, `order_current`를 StarRocks Paimon external catalog로 조회합니다.
- `Daily Business · Iceberg`: Spark가 만든 Iceberg Analytics table을 StarRocks Iceberg external catalog로 조회합니다.

자세한 해석은 [BI 대시보드 학생 가이드](docs/bi-dashboard-student-guide.md)를 참고합니다.

BI 화면을 과제/포트폴리오용으로 캡처할 때는 [BI 캡처 가이드](docs/bi-capture-guide.md)를 참고합니다.

샘플 캡처는 아래 경로에 포함되어 있습니다.

```text
artifacts/bi-captures/bi-lakehouse-ops-paimon-desktop-full.png
artifacts/bi-captures/bi-daily-business-iceberg-desktop-full.png
```

처음 실행할 때는 Docker 이미지 다운로드와 Spark package 다운로드 때문에 오래 걸릴 수 있습니다. 수업 중 필수 실습은 위의 `대표 실행 명령`까지만 따라와도 됩니다.


## 제출/질문 기준

- 과제 제출은 디스코드의 해당 차시 채널에 올립니다.
- 실행이 실패하면 전체 화면보다 터미널의 에러 메시지와 실행한 명령어를 함께 캡처합니다.
- 로컬 PC와 네트워크에 따라 첫 Docker 이미지 다운로드/빌드는 30분 이상 걸릴 수 있습니다. 수업 직전에 처음 실행하지 말고 미리 빌드해두는 것을 권장합니다.

## 포함하지 않은 것

- 멘토 운영 문서, 출석/녹화 관리 문서, 내부 검토 자료는 수강생 배포 자료에 포함하지 않습니다.
- 참고 답안은 기본적으로 포함하지 않습니다. 멘토가 수업 이후 공유할 때만 `INCLUDE_SOLUTIONS=1`로 패키지를 다시 생성합니다.
