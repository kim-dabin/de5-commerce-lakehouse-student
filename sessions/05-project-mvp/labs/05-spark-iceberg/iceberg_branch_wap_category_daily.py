#!/usr/bin/env python3
"""Run an Iceberg branch WAP demo for one Gold mart table.

Target table:
  iceberg_lake.analytics.olist_category_daily

Flow:
  1. Read the already-built staging table.
  2. Create an Iceberg audit branch on the published Gold table.
  3. Write the staging result to the audit branch.
  4. Audit the branch.
  5. Fast-forward main to the audit branch.
"""

from __future__ import annotations

import os
import re
import sys

from pyspark.sql import SparkSession


def q(identifier: str) -> str:
    """Quote a dot-separated Spark identifier."""
    return ".".join(f"`{part}`" for part in identifier.split("."))


def validate_branch_name(branch_name: str) -> str:
    if not re.fullmatch(r"[A-Za-z][A-Za-z0-9_]*", branch_name):
        raise ValueError(
            "BRANCH_NAME must start with a letter and contain only letters, "
            f"numbers, and underscores: {branch_name!r}"
        )
    return branch_name


def table_exists(spark: SparkSession, table_name: str) -> bool:
    try:
        spark.table(table_name).limit(1).collect()
        return True
    except Exception as exc:
        print(f"table_exists({table_name})=false reason={type(exc).__name__}: {exc}")
        return False


def count_table(spark: SparkSession, table_name: str) -> int:
    return int(spark.sql(f"SELECT COUNT(*) AS c FROM {table_name}").collect()[0]["c"])


def main() -> None:
    spark = SparkSession.builder.appName("de5-iceberg-branch-wap-category-daily").getOrCreate()
    spark.sparkContext.setLogLevel(os.getenv("SPARK_LOG_LEVEL", "WARN"))
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    catalog = os.getenv("ICEBERG_CATALOG", "iceberg_lake")
    staging_namespace = os.getenv("STAGING_NAMESPACE", "analytics_branch_stage")
    published_namespace = os.getenv("PUBLISHED_NAMESPACE", "analytics")
    table = os.getenv("TARGET_TABLE", "olist_category_daily")
    branch_name = validate_branch_name(
        os.getenv("BRANCH_NAME", "audit_category_daily_manual")
    )

    staging_table = f"{catalog}.{staging_namespace}.{table}"
    main_table = f"{catalog}.{published_namespace}.{table}"
    branch_table = f"{main_table}.branch_{branch_name}"

    print("== Iceberg branch WAP target ==")
    print(f"staging_table={staging_table}")
    print(f"main_table={main_table}")
    print(f"branch_name={branch_name}")

    if not table_exists(spark, staging_table):
        raise RuntimeError(
            f"Staging table is missing: {staging_table}. "
            "Run spark-iceberg-transform-inner.sh with "
            f"ICEBERG_NAMESPACE={staging_namespace} first."
        )

    staging_count = count_table(spark, q(staging_table))
    if staging_count <= 0:
        raise RuntimeError(f"Staging table is empty: {staging_table}")
    print(f"staging_count={staging_count}")

    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {q(catalog + '.' + published_namespace)}")

    if not table_exists(spark, main_table):
        print("main table missing; bootstrapping once from staging before branch WAP")
        spark.sql(
            f"""
            CREATE TABLE {q(main_table)}
            USING iceberg
            PARTITIONED BY (event_date)
            TBLPROPERTIES ('format-version' = '2')
            AS SELECT * FROM {q(staging_table)}
            """
        )

    main_before_count = count_table(spark, q(main_table))
    print(f"main_before_count={main_before_count}")

    print("== Write: create audit branch and write staging result there ==")
    spark.sql(
        f"""
        ALTER TABLE {q(main_table)}
        SET TBLPROPERTIES ('write.wap.enabled' = 'true')
        """
    )
    spark.sql(
        f"""
        ALTER TABLE {q(main_table)}
        CREATE BRANCH {branch_name} RETAIN 7 DAYS
        """
    )

    spark.conf.set("spark.wap.branch", branch_name)
    spark.sql(
        f"""
        INSERT OVERWRITE {q(main_table)}
        SELECT * FROM {q(staging_table)}
        """
    )

    try:
        spark.conf.unset("spark.wap.branch")
    except Exception:
        spark.conf.set("spark.wap.branch", "")

    print("== Audit: validate branch state before main is moved ==")
    branch_count = count_table(spark, q(branch_table))
    print(f"branch_count={branch_count}")
    if branch_count != staging_count:
        raise RuntimeError(
            f"Branch audit failed: branch_count={branch_count}, staging_count={staging_count}"
        )

    # main should still be readable before publish. It may be equal to the branch
    # if the input is unchanged; the important point is that publish has not moved
    # the main ref yet.
    main_pre_publish_count = count_table(spark, q(main_table))
    print(f"main_pre_publish_count={main_pre_publish_count}")

    print("== Publish: fast-forward main to the audited branch ==")
    spark.sql(
        f"""
        CALL {q(catalog)}.system.fast_forward(
          '{published_namespace}.{table}',
          'main',
          '{branch_name}'
        )
        """
    )

    print("== Validate: main after publish ==")
    main_after_count = count_table(spark, q(main_table))
    print(f"main_after_count={main_after_count}")
    if main_after_count != branch_count:
        raise RuntimeError(
            f"Publish validation failed: main_after_count={main_after_count}, "
            f"branch_count={branch_count}"
        )

    refs = spark.sql(f"SELECT * FROM {q(main_table + '.refs')}")
    print("== Iceberg refs ==")
    refs.show(truncate=False)

    print("BRANCH_WAP_PASSED")
    spark.stop()


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"BRANCH_WAP_FAILED: {type(exc).__name__}: {exc}", file=sys.stderr)
        raise
