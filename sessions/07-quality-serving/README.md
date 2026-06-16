# 7차시 - 데이터 품질과 StarRocks Serving

이번 차시의 질문은 하나입니다.

> "이 데이터를 믿고 서빙해도 되는가 — 무엇을 근거로?"

5차시에서 mart를 만들고, 6차시에서 장애를 복구했습니다. 7차시는 그 mart가 **믿을 수 있는 상태인지 품질로 증명**하고, 그 위에 **StarRocks/BI serving 계층**을 올려 "지금 무슨 일이 일어나는가"와 "믿을 수 있는 기준 결과는 무엇인가"를 나누어 봅니다.

## 오늘 사용할 위치

7차시도 5차시에 완성한 프로젝트 스택을 그대로 사용합니다.

```bash
cd sessions/05-project-mvp
```

## 시작 전 기준 상태

수업 시작 전 또는 첫 10분 동안 아래 상태를 맞춥니다. (6차시 baseline과 동일 + 이번엔 **jupyter 이미지에 Great Expectations가 포함**됩니다.)

```bash
git pull origin main
docker compose -f docker-compose.lite.yml up -d --build     # jupyter에 great-expectations 포함 재빌드
docker compose -f docker-compose.lite.yml ps

./scripts/reset-olist-kafka-topics.sh
./scripts/reset-olist-paimon.sh
./scripts/run-flink-olist-paimon-streaming.sh all
./scripts/produce-olist-ux-events.sh
./scripts/produce-olist-review-events.sh
./scripts/produce-olist-order-events.sh
./scripts/start-airflow.sh
./scripts/trigger-airflow-pipeline.sh
# Airflow UI에서 de5_olist_project_mvp_pipeline 성공 확인
```

정상 기준입니다.

| 계층 | 확인 방법 | 기준값 |
|---|---|---|
| Paimon | `./scripts/query-olist-paimon.sh` | `16,693 / 1,971 / 2,000` |
| Iceberg mart | `./scripts/query-iceberg-tables.sh` | mart 7개, `olist_category_daily` 759 |
| 데이터 품질 | `./scripts/run-data-quality-checks.sh 2>/dev/null` | **OVERALL: PASS** (32 expectation + 3 custom) |
| StarRocks serving | `./scripts/query-realtime-olap-metrics.sh` | total_events 16,693 / users 2,875 / revenue 265,036 |
| BI | `./scripts/query-bi-metrics.sh` | Iceberg external catalog로 mart 조회 |

UI: Airflow `http://localhost:8080` (admin/admin) · Streamlit BI `http://localhost:8501` · JupyterLab `http://localhost:8888`

## 2시간 진행 방식

먼저 지난주 과제 발표를 짧게 진행합니다(1인 60-90초: 증거 1개 / 헷갈리는 점 1개 / 최종 발표 포인트 1개).

| 시간 | 파트 | 핵심 질문 |
|---|---|---|
| 20:00-20:05 | Open | 오늘 목표 |
| 20:05-20:20 | 지난주 과제 발표 | 증거 1 / 질문 1 / 발표 포인트 1 |
| 20:20-20:35 | A-1 품질 5차원 + GE | "정상"을 숫자가 아니라 규칙으로 선언할 수 있는가? |
| 20:35-20:50 | A-2 전체 검증 실행 | 7개 기준 테이블이 규칙을 모두 통과하는가? |
| 20:50-21:05 | A-3 의도적 실패 데모 | 검증이 없으면 오염은 어떻게 조용히 통과하는가? |
| 21:05-21:12 | 휴식 | |
| 21:12-21:27 | B-1 StarRocks external catalog | 같은 데이터를 복사 없이 조회하는 serving layer란? |
| 21:27-21:42 | B-2 realtime OLAP vs batch BI | 같은 데이터가 두 질문에 어떻게 다르게 답하는가? |
| 21:42-21:53 | B-3 Streamlit BI + metadata refresh | 조회 계층은 원천과 별도로 검증해야 하는가? |
| 21:53-22:00 | 정리 | 최종 발표에 넣을 품질/serving 포인트 선택 |

## Part A — 데이터 품질 (Data Quality)

데이터 품질을 표준 차원으로 나누어, **규칙 목록 자체가 문서이자 게이트**가 되도록 Great Expectations로 선언합니다.

| 차원 | 질문 | 도구 |
|---|---|---|
| completeness | 빠진 게 없는가 (row count, not-null) | GE |
| uniqueness | 키가 중복되지 않는가 | GE |
| validity | 값이 허용 범위/집합 안인가 (rating 1~5 등) | GE |
| consistency | 컬럼 간 관계가 맞는가 (퍼널 단조성, 집계 합) | Spark |
| freshness | 최신 데이터인가 | Spark |

5차시 Airflow DAG의 `validate_bi_metrics()`는 같은 검증을 파이썬 `assert`로 코드에 묻어 두었습니다. 7차시는 같은 의도를 **선언형 스위트로 승격**합니다 — 통과/실패가 expectation 단위로 남고, 종료코드로 게이트가 됩니다.

핵심 장치는 **의도적 실패 데모**입니다. 정상 데이터를 메모리상 복사해 한 행만 오염시키면, 같은 스위트가 그 오염을 잡아냅니다. "검증이 없으면 오염은 조용히 통과한다"(실무 사례 N)를 직접 봅니다.

자세한 실행은 `labs/09-data-quality/README.md`와 노트북 `notebooks/de5-data-quality.ipynb`를 참고하세요.

## Part B — StarRocks Serving / Realtime OLAP / BI

StarRocks는 Paimon·Iceberg 데이터를 **복사하지 않고 external catalog로 직접 조회**하는 serving/query 계층입니다.

```text
Paimon ux/review/order (current-state)   ─StarRocks Paimon external catalog→  Realtime OLAP "지금 무슨 일이?"
Iceberg analytics mart (L1/L2)           ─StarRocks Iceberg external catalog→  Daily Business BI "믿을 수 있는 기준은?"
```

같은 Olist 데이터를 두 질문으로 나눕니다.

- **Realtime OLAP**: 지금 무슨 일이 일어나고 있는가? (Paimon current-state를 빠르게 조회)
- **Batch Lakehouse BI**: 믿을 수 있는 기준 결과는 무엇인가? (Iceberg mart를 권위 있는 결과로 조회)

Streamlit BI는 이 두 관점을 `Lakehouse Ops · StarRocks(Paimon)`과 `Daily Business · Iceberg` 탭으로 나눠 보여줍니다. 마지막으로 external catalog의 metadata refresh(6차시 R6)로 "조회 계층은 원천과 별도로 검증해야 한다"는 점을 다시 확인합니다.

## 실무 사례와 연결

| 사례 | 오늘 보는 것 | 한 문장 |
|---|---|---|
| K. 모니터링 뷰 의존성 장애 | BI/대시보드도 upstream table/view 의존성을 갖는 파이프라인 | "대시보드와 알림도 upstream 의존성을 갖는 파이프라인이다." |
| N. 무음 드롭 | 검증이 없으면 오염/누락이 실패 없이 통과 (의도적 실패 데모) | "잡이 실패하지 않음이 곧 데이터가 옳다는 뜻은 아니다." |

## 수업에서 남길 증거

```text
품질: 어떤 차원의 어떤 규칙을 봤는가 (PASS/FAIL)
실패 데모: 어떤 오염을 넣었고 어떤 expectation이 잡았는가
serving: realtime 지표 1개 + batch 지표 1개를 어떤 테이블/grain에서 읽었는가
```

## 7차시 과제 방향

이번 과제도 최종 발표 자료로 쌓아갑니다. 자세한 기준은 [ASSIGNMENT.md](./ASSIGNMENT.md)를 확인하세요.

## 빠른 명령 카드

수업 중에는 [COMMAND_CARD.md](./COMMAND_CARD.md)를 열어두세요.
