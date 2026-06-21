# 7차시 명령 카드

모든 명령은 아래 위치에서 실행합니다.

```bash
cd sessions/05-project-mvp
```

## 0. 기준 상태 만들기

```bash
git pull origin main
docker compose -f docker-compose.lite.yml up -d --build     # jupyter에 great-expectations 포함
docker compose -f docker-compose.lite.yml ps

./scripts/reset-olist-kafka-topics.sh
./scripts/reset-olist-paimon.sh
./scripts/run-flink-olist-paimon-streaming.sh all
./scripts/produce-olist-ux-events.sh
./scripts/produce-olist-review-events.sh
./scripts/produce-olist-order-events.sh
./scripts/start-airflow.sh
./scripts/trigger-airflow-pipeline.sh        # Airflow UI에서 success 확인
```

---

# Part A — 데이터 품질

## A-1 / A-2. 전체 품질 검증 (헤드리스 게이트)

```bash
./scripts/run-data-quality-checks.sh             # 진행바 포함
./scripts/run-data-quality-checks.sh 2>/dev/null # 깨끗한 리포트만 (권장)
echo "exit=$?"                                   # 통과 0 / 위반 1
```

기대 출력:

```text
[PASS] olist_ux_events_clean  (6/6 expectations)
[PASS] olist_review_current  (5/5 expectations)
[PASS] olist_order_current  (4/4 expectations)
[PASS] olist_event_type_daily  (4/4 expectations)
[PASS] olist_funnel_daily  (4/4 expectations)
[PASS] olist_category_daily  (4/4 expectations)
[PASS] olist_review_sentiment_by_category  (5/5 expectations)
  ok [consistency] funnel_daily: purchase_sessions <= sessions
  ok [consistency] category_daily: event_count = 세부 카운트 합
  ok [freshness  ] ux_events_clean: 최신 event_date 존재  (max_event_date=2018-02-02)
OVERALL: PASS — 모든 품질 검증 통과
```

대화형(권장 — 차원별로 직접 작성하며 이해):

```text
JupyterLab(http://localhost:8888) → notebooks/de5-data-quality.ipynb
커널: PySpark (DE5 Lakehouse)
```

포인트:

```text
completeness / uniqueness / validity 는 Great Expectations로 "선언"한다(규칙 목록 = 문서).
consistency(퍼널 단조성·집계 합) / freshness(최신 날짜)는 GE 기본 expectation에 없어 Spark로 직접 계산한다.
종료코드 0/1 이므로 그대로 Airflow task 또는 CI 게이트로 연결할 수 있다.
```

## A-3. 의도적 실패 데모 (노트북 §3)

```text
notebooks/de5-data-quality.ipynb 의 "3. 의도적 실패 데모" 셀 실행
- 정상 category_daily(759행)를 메모리상 복사
- 한 행만 오염: category_code=NULL + event_count=0
- 같은 스위트 실행 → overall success: False (DQ가 잡아냄)
```

포인트:

```text
실제 Iceberg 테이블은 건드리지 않는다(메모리 복사본만 오염).
검증이 없으면 이 오염은 BI까지 조용히 흘러간다(실무 사례 N: 무음 드롭).
"잡이 실패하지 않음"이 곧 "데이터가 옳다"는 뜻은 아니다.
```

## A-4. OpenMetadata로 품질을 보는 관점

OpenMetadata는 별도 선택 스택입니다. Docker 메모리가 부족하면 건너뛰고 멘토 화면으로만 확인합니다.

```bash
cd ../07-quality-serving/openmetadata
./start-openmetadata.sh
./seed-openmetadata-demo.sh
./seed-openmetadata-dq-demo.sh
```

FAIL 화면까지 보여주고 싶으면 아래를 추가로 실행합니다. 다시 전부 PASS로 돌리려면 옵션 없이 `./seed-openmetadata-dq-demo.sh`를 한 번 더 실행합니다.

```bash
DQ_DEMO_INCLUDE_FAILURE=true ./seed-openmetadata-dq-demo.sh
./seed-openmetadata-dq-demo.sh
```

```text
OpenMetadata UI: http://localhost:8585
Login: admin@open-metadata.org / admin
확인 순서: Table/Profile → Data Quality/Test Case → Lineage impact
```

스크립트 역할:

```text
start-openmetadata.sh
  OpenMetadata UI/API 스택을 띄운다. 이미 떠 있으면 그대로 통과한다.

seed-openmetadata-demo.sh
  Kafka topic, Flink/Spark pipeline, table, dashboard 예시 자산을 만들고 lineage를 연결한다.
  목적: "이 테이블이 어디서 와서 어디로 가는가"를 보여준다.

seed-openmetadata-dq-demo.sh
  commerce_category_daily에 Test Suite/Test Case/Test Result를 넣는다.
  목적: "품질 규칙과 PASS/FAIL 결과가 테이블 옆에 어떻게 보이는가"를 보여준다.

DQ_DEMO_INCLUDE_FAILURE=true ./seed-openmetadata-dq-demo.sh
  실제 데이터를 망가뜨리지 않고, OpenMetadata에 실패 결과만 하나 publish한다.
  목적: 실패했을 때 Data Quality 화면이 어떻게 달라지는지 보여준다.

stop-openmetadata.sh
  OpenMetadata 컨테이너를 내린다. 기본은 데이터 볼륨을 남긴다.
```

포인트:

```text
GE/Spark = 품질 규칙을 실행하고 PASS/FAIL로 막는 자동 게이트.
OpenMetadata = 품질 결과를 table/column/owner/lineage와 같이 보는 관찰·협업 계층.
오늘은 자동 게이트는 run-data-quality-checks.sh, 설명/관찰 화면은 OpenMetadata로 나눠서 본다.
seed-openmetadata-dq-demo.sh는 대표 Test Suite/Test Case/Test Result를 넣어 Data Quality 탭을 보여주는 미니 데모다.
운영에서는 GE action, OpenMetadata Data Quality as Code, Airflow task로 품질 결과 publish를 자동화한다.
```

중지:

```bash
./stop-openmetadata.sh
cd ../../05-project-mvp
```

---

# Part B — StarRocks Serving / OLAP / BI

## B-1. StarRocks external catalog로 직접 조회

```bash
./scripts/query-realtime-olap.sh             # Paimon external catalog 대표 쿼리
```

포인트:

```text
StarRocks는 Paimon/Iceberg 데이터를 internal table로 복사하지 않고,
external catalog로 원본을 직접 조회하는 serving/query 계층이다.
"복사 후 조회" 대신 "원본을 그대로 빠르게 조회"의 트레이드오프를 본다.
```

## B-2. realtime OLAP vs batch BI (같은 데이터, 두 질문)

```bash
./scripts/query-realtime-olap-metrics.sh     # Paimon current-state → "지금 무슨 일이?"
./scripts/query-bi-metrics.sh                # Iceberg mart → "믿을 수 있는 기준은?"
```

기대 지표(realtime):

```text
total_events 16,693 · users 2,875 · sessions 2,875 · products 1,470 · revenue 265,036 · event_types 7
```

포인트:

```text
같은 Olist 데이터라도 Paimon current-state(realtime)와 Iceberg mart(batch)는 grain/시점이 다르다.
숫자가 다르면 "어느 테이블·어느 grain에서 계산된 지표인가"를 먼저 확인한다.
UXLog의 purchase는 행동 이벤트다 — 공식 매출로 쓰려면 주문/결제/환불 데이터가 더 필요하다.
```

### B-2 옵션. 수업 중 live Olist 이벤트 흘리기

품질 게이트와 baseline count 확인이 끝난 뒤, serving 파트에서만 켭니다.
`ux-events`, `review-events`, `order-status-events`를 round-robin으로 천천히 추가합니다.
UX는 append fact라 realtime row count가 움직이고, review/order는 current-state라 같은 key가 들어오면 최신 상태로 접힙니다.

```bash
./scripts/start-live-olist-events.sh          # 기본 전체 6 events/sec, 계속 replay
./scripts/live-olist-events-status.sh         # producer 상태 + 세 topic offset 확인
./scripts/query-realtime-olap-metrics.sh      # total_events 변화 확인
```

속도를 조절하고 싶으면:

```bash
LIVE_OLIST_RATE_PER_SECOND=12 ./scripts/start-live-olist-events.sh
LIVE_OLIST_MAX_EVENTS=300 ./scripts/start-live-olist-events.sh
```

중지:

```bash
./scripts/stop-live-olist-events.sh
```

포인트:

```text
live producer는 세 토픽 모두에 입력을 계속 만든다.
ux_events_bronze는 append라 row count가 계속 증가한다.
review_current/order_current는 current-state라 같은 key가 다시 들어오면 row count 변화가 작다.
품질 게이트 기준값을 다시 맞춰야 하는 구간에서는 live producer를 끈다.
```

## B-3. Streamlit BI (2-view) + metadata refresh

```bash
./scripts/start-streamlit-bi.sh              # http://localhost:8501
```

```text
탭 1) Lakehouse Ops · StarRocks(Paimon)  : Paimon Bronze/current 직접 조회
탭 2) Daily Business · Iceberg            : Iceberg Analytics mart 조회
```

조회 계층 검증(6차시 R6 재확인):

```bash
./scripts/query-bi-metrics.sh                # refresh 전
./scripts/ops-r6-refresh-starrocks-iceberg.sh
./scripts/query-bi-metrics.sh                # refresh 후
```

포인트:

```text
데이터 파일과 catalog가 정상이어도 조회 계층 metadata가 stale할 수 있다(사례 K: 뷰 의존성).
refresh 전후 숫자가 같으면 "이미 fresh"이며 실패가 아니다.
가능하면 Paimon/Iceberg native count · StarRocks count · BI metric을 3자 비교한다.
```

---

## 마지막 정리

```bash
./scripts/run-data-quality-checks.sh 2>/dev/null   # 품질 게이트 PASS 재확인
./scripts/query-bi-metrics.sh                      # serving 기준값 재확인
```
