#!/usr/bin/env python3
"""DE5 7차시 · Iceberg analytics 기준 테이블 데이터 품질 검증 (Great Expectations).

표준 품질 차원을 "선언적"으로 검증한다.

  completeness : row count, 핵심 컬럼 not-null
  uniqueness   : 기본키 유일성
  validity     : 값 범위 / 허용 집합 (rating 1~5, event_type 집합 ...)

GE 기본 expectation으로는 표현하기 어려운 두 차원은 Spark로 직접 계산한다.

  consistency  : 퍼널 단조성(sessions >= purchase_sessions), 집계 합 일치
  freshness    : 최신 event_date 존재

5차시 Airflow DAG의 validate_bi_metrics()는 같은 검증을 파이썬 assert로 "임베디드"
했지만, 7차시에서는 같은 의도를 Great Expectations 선언형 스위트로 "승격"한다.
- 임베디드 assert : 코드를 읽어야 어떤 규칙인지 안다.
- 선언형 스위트   : 규칙 목록이 곧 문서이고, 통과/실패가 expectation 단위로 남는다.

실행:
  # 호스트(05-project-mvp 디렉터리)에서
  ./scripts/run-data-quality-checks.sh
  # 또는 jupyter 컨테이너에서 직접 (kernel과 동일한 카탈로그 설정 필요)
  PYSPARK_SUBMIT_ARGS="$KERNEL_ARGS" python3 labs/09-data-quality/data_quality_checks.py

종료 코드: 모든 검증 통과 0, 하나라도 실패 1 (CI/Airflow 게이트로 사용 가능).
"""
from __future__ import annotations

import json
import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# GE 텔레메트리(외부 전송) 비활성화 — 수업/오프라인 환경 보호.
os.environ.setdefault("GX_ANALYTICS_ENABLED", "false")

import great_expectations as gx
from great_expectations import expectations as gxe

# GE는 Spark 메트릭 계산 진행바를 stderr로 출력한다. 리포트(stdout)와 분리되므로,
# 깨끗한 캡처가 필요하면 실행 시 stderr를 버리면 된다(`... 2>/dev/null`).

CATALOG = os.getenv("ICEBERG_CATALOG", "iceberg_lake")
NS = os.getenv("ICEBERG_NAMESPACE", "analytics")

# Olist UX 이벤트 타입(실데이터 distinct 기준 7종).
EVENT_TYPES = [
    "search_result_click",
    "product_view",
    "review_impression",
    "review_expand",
    "add_to_cart",
    "remove_from_cart",
    "purchase",
]
# sentiment / order_status 는 transform 단계 COALESCE 기본값 'unknown'을 허용집합에 포함.
SENTIMENTS = ["positive", "negative", "neutral", "unknown"]
ORDER_STATUSES = [
    "delivered",
    "created",
    "processing",
    "canceled",
    "invoiced",
    "shipped",
    "unavailable",
    "unknown",
]

# 시드 데이터(결정적 합성)에서 기대하는 row count.
# 데이터를 다시 시드하면 이 값은 갱신해야 한다 — 회귀(regression) 성격의 검증이다.
EXPECTED_ROWS = {
    "olist_ux_events_clean": 16_693,
    "olist_review_current": 1_971,
    "olist_order_current": 2_000,
    "olist_event_type_daily": 256,
    "olist_funnel_daily": 52,
    "olist_category_daily": 759,
    "olist_review_sentiment_by_category": 120,
}


def expectations_for(table: str) -> list:
    """테이블별 GE expectation 목록 (completeness / uniqueness / validity)."""
    base = [gxe.ExpectTableRowCountToEqual(value=EXPECTED_ROWS[table])]
    specs: dict[str, list] = {
        "olist_ux_events_clean": [
            gxe.ExpectColumnValuesToNotBeNull(column="event_id"),
            gxe.ExpectColumnValuesToBeUnique(column="event_id"),
            gxe.ExpectColumnValuesToNotBeNull(column="event_date"),
            gxe.ExpectColumnValuesToBeInSet(column="event_type", value_set=EVENT_TYPES),
            gxe.ExpectColumnValuesToBeBetween(column="price", min_value=0),
        ],
        "olist_review_current": [
            gxe.ExpectColumnValuesToNotBeNull(column="review_id"),
            gxe.ExpectColumnValuesToBeUnique(column="review_id"),
            gxe.ExpectColumnValuesToBeBetween(column="rating", min_value=1, max_value=5),
            gxe.ExpectColumnValuesToBeInSet(column="sentiment", value_set=SENTIMENTS),
        ],
        "olist_order_current": [
            gxe.ExpectColumnValuesToNotBeNull(column="order_id"),
            gxe.ExpectColumnValuesToBeUnique(column="order_id"),
            gxe.ExpectColumnValuesToBeInSet(column="order_status", value_set=ORDER_STATUSES),
        ],
        "olist_event_type_daily": [
            gxe.ExpectColumnValuesToNotBeNull(column="event_date"),
            gxe.ExpectColumnValuesToBeInSet(column="event_type", value_set=EVENT_TYPES),
            gxe.ExpectColumnValuesToBeBetween(column="event_count", min_value=1),
        ],
        "olist_funnel_daily": [
            gxe.ExpectColumnValuesToNotBeNull(column="event_date"),
            gxe.ExpectColumnValuesToBeUnique(column="event_date"),
            gxe.ExpectColumnValuesToBeBetween(column="sessions", min_value=1),
        ],
        "olist_category_daily": [
            gxe.ExpectColumnValuesToNotBeNull(column="event_date"),
            gxe.ExpectColumnValuesToNotBeNull(column="category_code"),
            gxe.ExpectColumnValuesToBeBetween(column="event_count", min_value=1),
        ],
        "olist_review_sentiment_by_category": [
            gxe.ExpectColumnValuesToNotBeNull(column="category_code"),
            gxe.ExpectColumnValuesToBeInSet(column="sentiment", value_set=SENTIMENTS),
            gxe.ExpectColumnValuesToBeBetween(column="avg_rating", min_value=1, max_value=5),
            gxe.ExpectColumnValuesToBeBetween(column="review_count", min_value=1),
        ],
    }
    return base + specs.get(table, [])


def run_ge_suite(context, spark, table: str):
    """단일 테이블에 GE 스위트를 실행하고 ExpectationSuiteValidationResult를 반환."""
    df = spark.table(f"{CATALOG}.{NS}.{table}")

    data_source = context.data_sources.add_spark(name=f"src_{table}")
    data_asset = data_source.add_dataframe_asset(name=table)
    batch_definition = data_asset.add_batch_definition_whole_dataframe(name="whole")

    suite = context.suites.add(gx.ExpectationSuite(name=f"{table}_suite"))
    for expectation in expectations_for(table):
        suite.add_expectation(expectation)

    batch = batch_definition.get_batch(batch_parameters={"dataframe": df})
    return batch.validate(suite)


def summarize_result(result) -> list[dict]:
    """GE 결과를 expectation 단위 dict 목록으로 평탄화."""
    rows: list[dict] = []
    for item in result.results:
        config = item.expectation_config
        kwargs = dict(getattr(config, "kwargs", {}) or {})
        observed = (item.result or {}).get("observed_value")
        unexpected = (item.result or {}).get("unexpected_count")
        rows.append(
            {
                "type": getattr(config, "type", "?"),
                "column": kwargs.get("column", kwargs.get("column_list", "-")),
                "success": bool(item.success),
                "observed": observed,
                "unexpected": unexpected,
            }
        )
    return rows


def run_custom_checks(spark) -> list[dict]:
    """GE 기본 expectation으로 표현하기 어려운 consistency / freshness 검증."""
    checks: list[dict] = []

    funnel = spark.table(f"{CATALOG}.{NS}.olist_funnel_daily")
    bad_funnel = funnel.filter("purchase_sessions > sessions").count()
    checks.append(
        {
            "dimension": "consistency",
            "rule": "funnel_daily: purchase_sessions <= sessions (퍼널 단조성)",
            "success": bad_funnel == 0,
            "detail": f"violations={bad_funnel}",
        }
    )

    category = spark.table(f"{CATALOG}.{NS}.olist_category_daily")
    subcount_sum = (
        "search_result_click_count + product_view_count + review_impression_count "
        "+ review_expand_count + add_to_cart_count + remove_from_cart_count + purchase_count"
    )
    bad_category = category.filter(f"event_count <> ({subcount_sum})").count()
    checks.append(
        {
            "dimension": "consistency",
            "rule": "category_daily: event_count = 이벤트 세부 카운트 합",
            "success": bad_category == 0,
            "detail": f"violations={bad_category}",
        }
    )

    max_event_date = (
        spark.table(f"{CATALOG}.{NS}.olist_ux_events_clean")
        .agg(F.max("event_date").alias("m"))
        .collect()[0]["m"]
    )
    checks.append(
        {
            "dimension": "freshness",
            "rule": "ux_events_clean: 최신 event_date 존재",
            "success": max_event_date is not None,
            "detail": f"max_event_date={max_event_date}",
        }
    )
    return checks


def main() -> None:
    spark = SparkSession.builder.appName("de5-data-quality-checks").getOrCreate()
    spark.sparkContext.setLogLevel(os.getenv("SPARK_LOG_LEVEL", "ERROR"))
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    context = gx.get_context(mode="ephemeral")

    overall_ok = True
    report: dict[str, object] = {"tables": {}, "custom": []}

    print("=" * 72)
    print("DE5 7차시 · 데이터 품질 검증 (Great Expectations + Spark/Iceberg)")
    print(f"  great_expectations={gx.__version__}  catalog={CATALOG}.{NS}  tables={len(EXPECTED_ROWS)}")
    print("=" * 72)

    for table in EXPECTED_ROWS:
        result = run_ge_suite(context, spark, table)
        rows = summarize_result(result)
        passed = sum(1 for r in rows if r["success"])
        table_ok = bool(result.success)
        overall_ok = overall_ok and table_ok
        report["tables"][table] = {"success": table_ok, "expectations": rows}

        flag = "PASS" if table_ok else "FAIL"
        print(f"\n[{flag}] {table}  ({passed}/{len(rows)} expectations)")
        for r in rows:
            mark = "  ok " if r["success"] else "  XX "
            extra = ""
            if not r["success"]:
                if r["unexpected"] is not None:
                    extra = f"  unexpected={r['unexpected']}"
                elif r["observed"] is not None:
                    extra = f"  observed={r['observed']}"
            print(f"{mark}{r['type']:<40} {str(r['column']):<16}{extra}")

    print("\n" + "-" * 72)
    print("custom checks (consistency / freshness)")
    print("-" * 72)
    for check in run_custom_checks(spark):
        overall_ok = overall_ok and check["success"]
        report["custom"].append(check)
        mark = "  ok " if check["success"] else "  XX "
        print(f"{mark}[{check['dimension']:<11}] {check['rule']}  ({check['detail']})")

    report["overall_success"] = overall_ok
    print("\n" + "=" * 72)
    print(f"OVERALL: {'PASS — 모든 품질 검증 통과' if overall_ok else 'FAIL — 품질 위반 발견'}")
    print("=" * 72)
    print("DQ_RESULT_JSON=" + json.dumps(report, default=str, ensure_ascii=False))

    spark.stop()
    sys.exit(0 if overall_ok else 1)


if __name__ == "__main__":
    main()
