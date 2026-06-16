# 09 Data Quality (7차시)

Iceberg `analytics` 기준 테이블 7개를 **선언적 데이터 품질 검증**으로 점검한다.
StarRocks/BI가 읽는 그 기준 테이블이 "믿을 수 있는 상태인지"를 데이터 계층에서 증명하는 단계다.

```text
Spark / Iceberg analytics tables
  -> Great Expectations 선언형 스위트 (completeness / uniqueness / validity)
  -> Spark custom checks (consistency / freshness)
  -> PASS/FAIL 게이트 (종료코드 0/1)
```

## 검증 대상 (7개 기준 테이블)

| 테이블 | 성격 | 기대 row count |
|---|---|---:|
| `olist_ux_events_clean` | L1 정제 이벤트 | 16,693 |
| `olist_review_current` | L1 리뷰 current-state | 1,971 |
| `olist_order_current` | L1 주문 current-state | 2,000 |
| `olist_event_type_daily` | L2 일자×이벤트 집계 | 256 |
| `olist_funnel_daily` | L2 일자 퍼널 | 52 |
| `olist_category_daily` | L2 일자×카테고리 집계 | 759 |
| `olist_review_sentiment_by_category` | L2 카테고리 감성 | 120 |

## 품질 차원

| 차원 | 도구 | 예시 규칙 |
|---|---|---|
| completeness | GE | row count 일치, 핵심 컬럼 not-null |
| uniqueness | GE | `event_id`/`review_id`/`order_id` 유일 |
| validity | GE | `rating` 1~5, `event_type` 7종 집합, `price >= 0` |
| consistency | Spark | 퍼널 단조성(`sessions >= purchase_sessions`), `event_count = 세부 카운트 합` |
| freshness | Spark | 최신 `event_date` 존재 |

> completeness/uniqueness/validity는 Great Expectations로 **선언**하고, 컬럼 간 관계·시간 기준은 GE 기본 expectation에 없어 **Spark로 직접 계산**한다.

## 실행

### 헤드리스 게이트 (종료코드 0/1)

```bash
# 05-project-mvp 디렉터리에서
./scripts/run-data-quality-checks.sh
# 깨끗한 리포트만 보려면 (GE 진행바는 stderr로 출력됨)
./scripts/run-data-quality-checks.sh 2>/dev/null
```

### 노트북 (대화형 교육)

JupyterLab(`http://127.0.0.1:8888`)에서 `notebooks/de5-data-quality.ipynb`를 열고 **PySpark (DE5 Lakehouse)** 커널로 실행한다.

## 기대 결과

GE expectation 32개 + custom check 3개가 모두 통과한다.

```text
[PASS] olist_ux_events_clean  (6/6 expectations)
[PASS] olist_review_current  (5/5 expectations)
[PASS] olist_order_current  (4/4 expectations)
[PASS] olist_event_type_daily  (4/4 expectations)
[PASS] olist_funnel_daily  (4/4 expectations)
[PASS] olist_category_daily  (4/4 expectations)
[PASS] olist_review_sentiment_by_category  (5/5 expectations)
  ok [consistency] funnel_daily: purchase_sessions <= sessions (퍼널 단조성)  (violations=0)
  ok [consistency] category_daily: event_count = 이벤트 세부 카운트 합  (violations=0)
  ok [freshness  ] ux_events_clean: 최신 event_date 존재  (max_event_date=2018-02-02)
========================================================================
OVERALL: PASS — 모든 품질 검증 통과
```

## 의도적 실패 데모 (노트북 §3)

정상 데이터를 메모리상 복사한 뒤 한 행만 오염시켜(카테고리 `NULL` + `event_count=0`) 같은 스위트를 실행하면,
`overall success: False`가 나오며 어떤 expectation이 깨졌는지 expectation 단위로 보인다.
실제 Iceberg 테이블은 건드리지 않는다.

> 교훈(실무 사례 N): 잡이 실패하지 않는다고 안전한 게 아니다. 검증이 없으면 오염은 **조용히 통과**한다.

## 5차시 DAG와의 관계

5차시 Airflow DAG `validate_bi_metrics()`는 같은 검증을 파이썬 `assert`로 코드에 묻어 두었다.
여기서는 같은 의도를 GE 선언형 스위트로 **승격**해, 규칙 목록이 곧 문서이자 게이트가 되도록 한다.
`run-data-quality-checks.sh`는 종료코드로 PASS/FAIL을 내므로 DAG task 또는 CI 게이트로 바로 연결할 수 있다.
