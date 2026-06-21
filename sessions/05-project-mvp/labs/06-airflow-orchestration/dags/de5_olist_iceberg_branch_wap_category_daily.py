"""Airflow DAG for one-table Iceberg branch WAP.

This DAG uses a real Iceberg audit branch for one Gold mart table:

  iceberg_lake.analytics.olist_category_daily

It complements de5_olist_wap_staging_publish_pipeline:

  - staging WAP: namespace-level staging, easier to understand
  - branch WAP : table-level Iceberg branch + fast_forward, closer to Iceberg's
                 native Write-Audit-Publish pattern
"""

from __future__ import annotations

import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator


STAGING_NAMESPACE = "analytics_branch_stage"
PUBLISHED_NAMESPACE = "analytics"
TARGET_TABLE = "olist_category_daily"


with DAG(
    dag_id="de5_olist_iceberg_branch_wap_category_daily",
    description=(
        "Iceberg branch WAP demo for one Gold mart table: "
        "analytics.olist_category_daily."
    ),
    start_date=pendulum.datetime(2026, 6, 21, tz="Asia/Seoul"),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["de5", "olist", "wap", "iceberg", "branch", "gold"],
    doc_md=f"""
## DE5 Olist Iceberg Branch WAP - One Gold Table

이 DAG는 Iceberg native branch WAP를 한 개 테이블에 적용합니다.

대상은 L1이 아니라 Gold mart입니다.

```text
target = iceberg_lake.{PUBLISHED_NAMESPACE}.{TARGET_TABLE}
```

흐름:

```text
Write   : Spark -> iceberg_lake.{STAGING_NAMESPACE}.{TARGET_TABLE}
Branch  : target table에 audit branch 생성
Audit   : audit branch count == staging count 검증
Publish : Iceberg fast_forward(main <- audit branch)
Validate: main count == branch count 검증
```

주의:

- `analytics`의 `olist_category_daily`는 Gold mart입니다.
- `olist_ux_events_clean`, `olist_review_current`, `olist_order_current`는 L1 성격의 clean/current-state 테이블입니다.
- Iceberg branch WAP는 테이블 단위입니다. 여러 mart를 한 번에 원자적으로 publish하려면 staging namespace, Nessie, LakeFS 같은 더 큰 단위의 배포 전략을 검토합니다.
""",
) as dag:
    start = EmptyOperator(task_id="start")

    validate_runtime_services = BashOperator(
        task_id="validate_runtime_services",
        bash_command="""
set -euo pipefail
docker exec de5-spark-client test -x /workspace/scripts/spark-iceberg-transform-inner.sh
docker exec de5-spark-client test -x /workspace/scripts/spark-iceberg-branch-wap-category-inner.sh
curl -fsS http://minio:9000/minio/health/live
curl -fsS http://iceberg-rest:8181/v1/config
""",
    )

    write_staging_gold_table = BashOperator(
        task_id="write_staging_gold_table",
        bash_command=f"""
set -euo pipefail
docker exec \
  -e ICEBERG_NAMESPACE={STAGING_NAMESPACE} \
  -e SPARK_SHOW_PREVIEW=0 \
  de5-spark-client \
  /workspace/scripts/spark-iceberg-transform-inner.sh
""",
    )

    branch_wap_publish_category_daily = BashOperator(
        task_id="branch_wap_publish_category_daily",
        bash_command=f"""
set -euo pipefail
BRANCH_NAME="audit_category_daily_$(date +%Y%m%d%H%M%S)"
echo "branch_name=${{BRANCH_NAME}}"
docker exec \
  -e STAGING_NAMESPACE={STAGING_NAMESPACE} \
  -e PUBLISHED_NAMESPACE={PUBLISHED_NAMESPACE} \
  -e TARGET_TABLE={TARGET_TABLE} \
  -e BRANCH_NAME="${{BRANCH_NAME}}" \
  de5-spark-client \
  /workspace/scripts/spark-iceberg-branch-wap-category-inner.sh
""",
    )

    finish = EmptyOperator(task_id="finish")

    start >> validate_runtime_services >> write_staging_gold_table >> branch_wap_publish_category_daily >> finish
