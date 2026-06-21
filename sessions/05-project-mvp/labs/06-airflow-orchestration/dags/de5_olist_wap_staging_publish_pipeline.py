"""Airflow DAG for a staging-namespace WAP demo.

This DAG demonstrates Write-Audit-Publish without adding Iceberg branch
operations or a separate catalog-versioning service.

Write   : build the Iceberg analytics mart into analytics_wap_stage.
Audit   : validate the staging namespace through StarRocks.
Publish : replace the analytics namespace only after audit passes.

It is intentionally separate from de5_olist_project_mvp_pipeline so students can
compare "build then validate" with "stage, validate, publish".
"""

from __future__ import annotations

import json
import subprocess
import urllib.request
from typing import Any

import pendulum
from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator


FLINK_OVERVIEW_URL = "http://flink-jobmanager:8081/jobs/overview"
STAGING_NAMESPACE = "analytics_wap_stage"
PUBLISHED_NAMESPACE = "analytics"

EXPECTED_FLINK_JOB_HINT_GROUPS = (
    ("ingest-ux-events", "ux_events_bronze"),
    ("ingest-review-current", "review_current"),
    ("ingest-order-current", "order_current"),
)

STARROCKS_SESSION_PREFIX = "SET new_planner_optimize_timeout = 30000;\n"

EXPECTED_TABLE_COUNTS = {
    "olist_ux_events_clean": 16_693,
    "olist_review_current": 1_971,
    "olist_order_current": 2_000,
    "olist_event_type_daily": 256,
    "olist_funnel_daily": 52,
    "olist_category_daily": 759,
    "olist_review_sentiment_by_category": 120,
}

EXPECTED_BI_TOTALS = {
    "total_events": 16_693,
    "users": 2_875,
    "sessions": 2_875,
    "orders": 1_968,
    "revenue": 265_036.00,
}
EXPECTED_REVIEW_TOTALS = {
    "reviews": 1_971,
    "negative_reviews": 367,
}
EXPECTED_ORDER_CURRENT_ROWS = 2_000
EXPECTED_AVG_RATING = 3.93


PUBLISH_SQL = f"""
CREATE NAMESPACE IF NOT EXISTS iceberg_lake.{PUBLISHED_NAMESPACE};

CREATE OR REPLACE TABLE iceberg_lake.{PUBLISHED_NAMESPACE}.olist_ux_events_clean
USING iceberg
PARTITIONED BY (event_date)
TBLPROPERTIES ('format-version' = '2')
AS SELECT * FROM iceberg_lake.{STAGING_NAMESPACE}.olist_ux_events_clean;

CREATE OR REPLACE TABLE iceberg_lake.{PUBLISHED_NAMESPACE}.olist_review_current
USING iceberg
TBLPROPERTIES ('format-version' = '2')
AS SELECT * FROM iceberg_lake.{STAGING_NAMESPACE}.olist_review_current;

CREATE OR REPLACE TABLE iceberg_lake.{PUBLISHED_NAMESPACE}.olist_order_current
USING iceberg
TBLPROPERTIES ('format-version' = '2')
AS SELECT * FROM iceberg_lake.{STAGING_NAMESPACE}.olist_order_current;

CREATE OR REPLACE TABLE iceberg_lake.{PUBLISHED_NAMESPACE}.olist_event_type_daily
USING iceberg
PARTITIONED BY (event_date)
TBLPROPERTIES ('format-version' = '2')
AS SELECT * FROM iceberg_lake.{STAGING_NAMESPACE}.olist_event_type_daily;

CREATE OR REPLACE TABLE iceberg_lake.{PUBLISHED_NAMESPACE}.olist_funnel_daily
USING iceberg
PARTITIONED BY (event_date)
TBLPROPERTIES ('format-version' = '2')
AS SELECT * FROM iceberg_lake.{STAGING_NAMESPACE}.olist_funnel_daily;

CREATE OR REPLACE TABLE iceberg_lake.{PUBLISHED_NAMESPACE}.olist_category_daily
USING iceberg
PARTITIONED BY (event_date)
TBLPROPERTIES ('format-version' = '2')
AS SELECT * FROM iceberg_lake.{STAGING_NAMESPACE}.olist_category_daily;

CREATE OR REPLACE TABLE iceberg_lake.{PUBLISHED_NAMESPACE}.olist_review_sentiment_by_category
USING iceberg
PARTITIONED BY (category_code)
TBLPROPERTIES ('format-version' = '2')
AS SELECT * FROM iceberg_lake.{STAGING_NAMESPACE}.olist_review_sentiment_by_category;
"""


def run_shell(command: str) -> str:
    result = subprocess.run(
        ["bash", "-lc", command],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        check=False,
    )
    print(result.stdout)
    if result.returncode != 0:
        raise AirflowException(f"Command failed with exit code {result.returncode}: {command}")
    return result.stdout


def run_command(args: list[str]) -> str:
    result = subprocess.run(
        args,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        check=False,
    )
    print(result.stdout)
    if result.returncode != 0:
        raise AirflowException(
            f"Command failed with exit code {result.returncode}: {' '.join(args)}"
        )
    return result.stdout


def parse_value(value: str) -> Any:
    if value in {"", "NULL", r"\N"}:
        return None
    try:
        if "." in value:
            return float(value)
        return int(value)
    except Exception:
        return value


def run_starrocks_query(sql: str) -> list[list[Any]]:
    output = run_command(
        [
            "docker",
            "exec",
            "de5-starrocks-fe",
            "mysql",
            "-h127.0.0.1",
            "-P9030",
            "-uroot",
            "--batch",
            "--raw",
            "--skip-column-names",
            "-e",
            STARROCKS_SESSION_PREFIX + sql.strip(),
        ]
    )
    rows = []
    for line in output.splitlines():
        if line.strip():
            rows.append([parse_value(value) for value in line.split("\t")])
    return rows


def recreate_iceberg_catalog(catalog_name: str) -> None:
    run_starrocks_query(
        f"""
        DROP CATALOG IF EXISTS {catalog_name};
        CREATE EXTERNAL CATALOG {catalog_name}
        PROPERTIES
        (
          "type" = "iceberg",
          "iceberg.catalog.type" = "rest",
          "iceberg.catalog.uri" = "http://iceberg-rest:8181",
          "iceberg.catalog.warehouse" = "s3://warehouse/",
          "aws.s3.enable_ssl" = "false",
          "aws.s3.enable_path_style_access" = "true",
          "aws.s3.endpoint" = "http://minio:9000",
          "aws.s3.access_key" = "minioadmin",
          "aws.s3.secret_key" = "minioadmin",
          "aws.s3.region" = "us-east-1"
        );
        """
    )


def assert_equal(label: str, actual: Any, expected: Any) -> None:
    if actual != expected:
        raise AirflowException(f"{label} mismatch: expected={expected!r}, actual={actual!r}")
    print(f"ok {label}={actual!r}")


def assert_close(label: str, actual: float, expected: float, tolerance: float = 0.01) -> None:
    if abs(actual - expected) > tolerance:
        raise AirflowException(f"{label} mismatch: expected={expected}, actual={actual}")
    print(f"ok {label}={actual}")


def validate_flink_streaming_jobs() -> None:
    with urllib.request.urlopen(FLINK_OVERVIEW_URL, timeout=10) as response:
        payload = json.loads(response.read().decode("utf-8"))

    jobs = payload.get("jobs", [])
    running_jobs = [job for job in jobs if job.get("state") == "RUNNING"]
    print("running_flink_jobs=" + json.dumps(running_jobs, ensure_ascii=False, indent=2))

    if len(running_jobs) < 3:
        raise AirflowException(
            f"Expected at least 3 RUNNING Flink jobs, found {len(running_jobs)}"
        )

    running_job_names = [str(job.get("name", "")) for job in running_jobs]
    missing = []
    for hints in EXPECTED_FLINK_JOB_HINT_GROUPS:
        if not any(
            hint in job_name
            for hint in hints
            for job_name in running_job_names
        ):
            missing.append("/".join(hints))

    if missing:
        raise AirflowException(
            "RUNNING jobs exist, but expected Olist job names were not found: "
            + ", ".join(missing)
        )

    print("Flink streaming ingestion jobs are RUNNING.")


def validate_iceberg_namespace(catalog_name: str, namespace: str, label: str) -> None:
    """Validate table counts and BI metrics for one Iceberg namespace."""
    recreate_iceberg_catalog(catalog_name)

    for table, expected in EXPECTED_TABLE_COUNTS.items():
        actual = run_starrocks_query(
            f"SELECT COUNT(*) FROM {catalog_name}.{namespace}.{table};"
        )[0][0]
        assert_equal(f"{label}.{table}", int(actual), expected)

    totals = run_starrocks_query(
        f"""
        SELECT
          COUNT(*) AS total_events,
          COUNT(DISTINCT user_id) AS users,
          COUNT(DISTINCT session_id) AS sessions,
          COUNT(DISTINCT order_id) AS orders,
          SUM(IF(event_type = 'purchase', price, 0)) AS revenue
        FROM {catalog_name}.{namespace}.olist_ux_events_clean;
        """
    )[0]

    review_totals = run_starrocks_query(
        f"""
        SELECT
          COUNT(*) AS reviews,
          AVG(rating) AS avg_rating,
          SUM(IF(sentiment = 'negative', 1, 0)) AS negative_reviews
        FROM {catalog_name}.{namespace}.olist_review_current;
        """
    )[0]

    order_current_rows = run_starrocks_query(
        f"SELECT COUNT(*) FROM {catalog_name}.{namespace}.olist_order_current;"
    )[0][0]

    total_events, users, sessions, orders, revenue = totals
    reviews, avg_rating, negative_reviews = review_totals

    assert_equal(f"{label}.total_events", int(total_events), EXPECTED_BI_TOTALS["total_events"])
    assert_equal(f"{label}.users", int(users), EXPECTED_BI_TOTALS["users"])
    assert_equal(f"{label}.sessions", int(sessions), EXPECTED_BI_TOTALS["sessions"])
    assert_equal(f"{label}.orders", int(orders), EXPECTED_BI_TOTALS["orders"])
    assert_close(f"{label}.revenue", float(revenue), EXPECTED_BI_TOTALS["revenue"])
    assert_equal(f"{label}.reviews", int(reviews), EXPECTED_REVIEW_TOTALS["reviews"])
    assert_equal(
        f"{label}.negative_reviews",
        int(negative_reviews),
        EXPECTED_REVIEW_TOTALS["negative_reviews"],
    )
    assert_close(f"{label}.avg_rating", float(avg_rating), EXPECTED_AVG_RATING, 0.01)
    assert_equal(
        f"{label}.order_current_rows",
        int(order_current_rows),
        EXPECTED_ORDER_CURRENT_ROWS,
    )


def audit_staging_namespace() -> None:
    validate_iceberg_namespace(
        catalog_name="iceberg_wap_stage",
        namespace=STAGING_NAMESPACE,
        label="staging",
    )
    print("AUDIT PASSED: staging namespace is safe to publish.")


def validate_published_namespace() -> None:
    validate_iceberg_namespace(
        catalog_name="iceberg_olist",
        namespace=PUBLISHED_NAMESPACE,
        label="published",
    )
    print("PUBLISH VERIFIED: analytics namespace is ready for BI serving.")


with DAG(
    dag_id="de5_olist_wap_staging_publish_pipeline",
    description=(
        "Write-Audit-Publish demo: build Iceberg marts in a staging namespace, "
        "audit them, then publish to the analytics namespace."
    ),
    start_date=pendulum.datetime(2026, 6, 21, tz="Asia/Seoul"),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["de5", "olist", "wap", "iceberg", "bi"],
    doc_md=f"""
## DE5 Olist WAP Staging Publish Pipeline

이 DAG는 Write-Audit-Publish 패턴을 보여주는 별도 DAG입니다.

기존 `de5_olist_project_mvp_pipeline`은 Iceberg analytics mart를 바로 build한 뒤
검증합니다. 이 DAG는 새 결과를 바로 serving namespace에 쓰지 않고 먼저
`{STAGING_NAMESPACE}`에 씁니다.

```text
Write   : Spark -> Iceberg {STAGING_NAMESPACE}
Audit   : StarRocks external catalog -> staging count / BI metric 검증
Publish : 검증 통과 시 {PUBLISHED_NAMESPACE} namespace로 CREATE OR REPLACE
Validate: StarRocks external catalog -> published count / BI metric 재검증
```

엄밀한 Iceberg branch fast-forward WAP는 아니지만, 학생 실습 환경에서는
staging namespace 방식이 가장 안전하고 이해하기 쉽습니다. 핵심은 같습니다.

> 검증 전 데이터는 BI serving namespace에 노출하지 않는다.
""",
) as dag:
    start = EmptyOperator(task_id="start")

    validate_runtime_services = BashOperator(
        task_id="validate_runtime_services",
        bash_command="""
set -euo pipefail
docker exec de5-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:19092 --list
curl -fsS http://flink-jobmanager:8081/overview
curl -fsS http://minio:9000/minio/health/live
curl -fsS http://iceberg-rest:8181/v1/config
docker exec de5-starrocks-fe mysql -h127.0.0.1 -P9030 -uroot -e "SELECT 1 AS starrocks_ready;"
""",
    )

    validate_streaming_jobs = PythonOperator(
        task_id="validate_flink_streaming_jobs",
        python_callable=validate_flink_streaming_jobs,
    )

    validate_paimon_fresh = BashOperator(
        task_id="validate_paimon_fresh",
        bash_command="""
set -euo pipefail
docker exec de5-spark-client /workspace/scripts/spark-paimon-validate-inner.sh
""",
    )

    write_staging_iceberg_mart = BashOperator(
        task_id="write_staging_iceberg_mart",
        bash_command=f"""
set -euo pipefail
docker exec \
  -e ICEBERG_NAMESPACE={STAGING_NAMESPACE} \
  -e SPARK_SHOW_PREVIEW=0 \
  de5-spark-client \
  /workspace/scripts/spark-iceberg-transform-inner.sh
""",
    )

    audit_staging_bi_metrics = PythonOperator(
        task_id="audit_staging_bi_metrics",
        python_callable=audit_staging_namespace,
    )

    publish_iceberg_analytics_mart = BashOperator(
        task_id="publish_iceberg_analytics_mart",
        bash_command=f"""
set -euo pipefail
cat > /tmp/de5_wap_publish.sql <<'SQL'
{PUBLISH_SQL}
SQL
docker cp /tmp/de5_wap_publish.sql de5-spark-client:/tmp/de5_wap_publish.sql
docker exec de5-spark-client \
  /workspace/scripts/spark-iceberg-sql-inner.sh /tmp/de5_wap_publish.sql
""",
    )

    validate_published_bi_metrics = PythonOperator(
        task_id="validate_published_bi_metrics",
        python_callable=validate_published_namespace,
    )

    finish = EmptyOperator(task_id="finish")

    (
        start
        >> validate_runtime_services
        >> validate_streaming_jobs
        >> validate_paimon_fresh
        >> write_staging_iceberg_mart
        >> audit_staging_bi_metrics
        >> publish_iceberg_analytics_mart
        >> validate_published_bi_metrics
        >> finish
    )
