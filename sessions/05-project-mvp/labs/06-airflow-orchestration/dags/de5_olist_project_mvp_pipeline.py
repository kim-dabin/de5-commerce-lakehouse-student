"""Airflow DAG for the DE5 Olist project MVP.

This DAG does not start or stop the long-running Flink streaming jobs. The
streaming jobs are the ingestion layer and should already be RUNNING before this
DAG is triggered.

Airflow's role here is to validate the ingestion layer, rebuild the batch
analytics mart, and leave task-level evidence for operations review.
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
EXPECTED_FLINK_JOB_HINTS = ("ux_events_bronze", "review_current", "order_current")

STARROCKS_SESSION_PREFIX = "SET new_planner_optimize_timeout = 30000;\n"

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


def run_shell(command: str) -> str:
    """Run a shell command from the Airflow container and print its output."""
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
    """Run a command without shell interpolation and print its output."""
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
        if not line.strip():
            continue
        rows.append([parse_value(value) for value in line.split("\t")])
    return rows


def assert_equal(label: str, actual: Any, expected: Any) -> None:
    if actual != expected:
        raise AirflowException(f"{label} mismatch: expected={expected!r}, actual={actual!r}")
    print(f"ok {label}={actual!r}")


def assert_close(label: str, actual: float, expected: float, tolerance: float = 0.01) -> None:
    if abs(actual - expected) > tolerance:
        raise AirflowException(f"{label} mismatch: expected={expected}, actual={actual}")
    print(f"ok {label}={actual}")


def validate_flink_streaming_jobs() -> None:
    """Check that the three Olist ingestion jobs are visible as RUNNING."""
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
    missing = [
        hint
        for hint in EXPECTED_FLINK_JOB_HINTS
        if not any(hint in job_name for job_name in running_job_names)
    ]

    if missing:
        cli_output = run_shell("docker exec de5-flink-jobmanager /opt/flink/bin/flink list -r || true")
        still_missing = [hint for hint in missing if hint not in cli_output]
        if still_missing:
            raise AirflowException(
                "RUNNING jobs exist, but expected Olist job names were not found: "
                + ", ".join(still_missing)
            )

    print("Flink streaming ingestion jobs are RUNNING.")


def validate_bi_metrics() -> None:
    """Validate BI metrics through StarRocks Iceberg external catalog."""
    run_starrocks_query(
        """
        DROP CATALOG IF EXISTS iceberg_olist;
        CREATE EXTERNAL CATALOG iceberg_olist
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

    totals = run_starrocks_query(
        """
        SELECT
          COUNT(*) AS total_events,
          COUNT(DISTINCT user_id) AS users,
          COUNT(DISTINCT session_id) AS sessions,
          COUNT(DISTINCT order_id) AS orders,
          SUM(IF(event_type = 'purchase', price, 0)) AS revenue
        FROM iceberg_olist.analytics.olist_ux_events_clean;
        """
    )[0]
    review_totals = run_starrocks_query(
        """
        SELECT
          COUNT(*) AS reviews,
          AVG(rating) AS avg_rating,
          SUM(IF(sentiment = 'negative', 1, 0)) AS negative_reviews
        FROM iceberg_olist.analytics.olist_review_current;
        """
    )[0]
    order_current_rows = run_starrocks_query(
        "SELECT COUNT(*) FROM iceberg_olist.analytics.olist_order_current;"
    )[0][0]

    total_events, users, sessions, orders, revenue = totals
    reviews, avg_rating, negative_reviews = review_totals

    assert_equal("total_events", int(total_events), EXPECTED_BI_TOTALS["total_events"])
    assert_equal("users", int(users), EXPECTED_BI_TOTALS["users"])
    assert_equal("sessions", int(sessions), EXPECTED_BI_TOTALS["sessions"])
    assert_equal("orders", int(orders), EXPECTED_BI_TOTALS["orders"])
    assert_close("revenue", float(revenue), EXPECTED_BI_TOTALS["revenue"])

    assert_equal("reviews", int(reviews), EXPECTED_REVIEW_TOTALS["reviews"])
    assert_equal(
        "negative_reviews",
        int(negative_reviews),
        EXPECTED_REVIEW_TOTALS["negative_reviews"],
    )
    assert_close("avg_rating", float(avg_rating), EXPECTED_AVG_RATING, 0.01)
    assert_equal("order_current_rows", int(order_current_rows), EXPECTED_ORDER_CURRENT_ROWS)

    print(
        "note: orders=1,968 is the UX purchase/order-linked BI metric; "
        "order_current_rows=2,000 is the current-state order entity count."
    )
    print(
        "note: users=sessions=2,875 is expected for this generated UX scenario, "
        "not a general production assumption."
    )


with DAG(
    dag_id="de5_olist_project_mvp_pipeline",
    description=(
        "Validate Flink/Paimon streaming ingestion, rebuild Iceberg analytics mart, "
        "and validate BI metrics for the Olist project MVP."
    ),
    start_date=pendulum.datetime(2026, 6, 12, tz="Asia/Seoul"),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["de5", "olist", "paimon", "iceberg", "bi"],
    doc_md="""
## DE5 Olist Project MVP Pipeline

이 DAG는 `Kafka -> Flink -> Paimon` streaming ingestion job이 이미 떠 있다는 전제에서 시작합니다.

Airflow가 하는 일:

1. Runtime service와 Flink RUNNING job 상태 확인
2. Paimon current/bronze table freshness 확인
3. Iceberg analytics mart 재생성
4. StarRocks Iceberg external catalog로 BI metric count 검증

Airflow가 하지 않는 일:

- Kafka, Flink, Spark 자체를 대체하지 않습니다.
- Flink streaming job을 batch task처럼 끝내지 않습니다.
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

    reset_iceberg_tables = BashOperator(
        task_id="reset_iceberg_tables",
        bash_command="""
set -euo pipefail
docker exec de5-spark-client /workspace/scripts/spark-iceberg-reset-inner.sh
""",
    )

    build_iceberg_analytics_mart = BashOperator(
        task_id="build_iceberg_analytics_mart",
        bash_command="""
set -euo pipefail
docker exec -e SPARK_SHOW_PREVIEW=0 de5-spark-client /workspace/scripts/spark-iceberg-transform-inner.sh
""",
    )

    query_iceberg_tables = BashOperator(
        task_id="query_iceberg_tables",
        bash_command="""
set -euo pipefail
docker exec de5-spark-client /workspace/scripts/spark-iceberg-query-inner.sh
""",
    )

    validate_bi_metric_counts = PythonOperator(
        task_id="validate_bi_metric_counts",
        python_callable=validate_bi_metrics,
    )

    finish = EmptyOperator(task_id="finish")

    (
        start
        >> validate_runtime_services
        >> validate_streaming_jobs
        >> validate_paimon_fresh
        >> reset_iceberg_tables
        >> build_iceberg_analytics_mart
        >> query_iceberg_tables
        >> validate_bi_metric_counts
        >> finish
    )
