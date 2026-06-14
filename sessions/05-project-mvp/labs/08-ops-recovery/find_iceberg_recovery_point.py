from __future__ import annotations

import argparse

from pyspark.sql import SparkSession


def quote_table(table: str) -> str:
    parts = table.split(".")
    if len(parts) != 3:
        raise ValueError("table must be catalog.namespace.table, for example iceberg_lake.analytics.olist_category_daily")
    return ".".join(parts)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Find an Iceberg snapshot that can be used as a recovery point."
    )
    parser.add_argument(
        "--table",
        default="iceberg_lake.analytics.olist_category_daily",
        help="Iceberg table name. Default: iceberg_lake.analytics.olist_category_daily",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=20,
        help="Number of recent snapshots to inspect.",
    )
    args = parser.parse_args()

    table = quote_table(args.table)

    spark = (
        SparkSession.builder.appName("de5-iceberg-recovery-point")
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config("spark.sql.catalog.iceberg_lake", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.iceberg_lake.type", "rest")
        .config("spark.sql.catalog.iceberg_lake.uri", "http://iceberg-rest:8181")
        .config("spark.sql.catalog.iceberg_lake.warehouse", "s3://warehouse/")
        .config("spark.sql.catalog.iceberg_lake.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config("spark.sql.catalog.iceberg_lake.s3.endpoint", "http://minio:9000")
        .config("spark.sql.catalog.iceberg_lake.s3.path-style-access", "true")
        .config("spark.sql.catalog.iceberg_lake.client.region", "us-east-1")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    snapshots_table = f"{table}.snapshots"
    snapshots = spark.sql(
        f"""
        SELECT
          committed_at,
          snapshot_id,
          operation
        FROM {snapshots_table}
        ORDER BY committed_at DESC
        LIMIT {args.limit}
        """
    ).collect()

    print(f"\n== Recent Iceberg snapshots: {table} ==")
    print(f"{'committed_at':26s} {'snapshot_id':>20s} {'operation':>12s} {'row_count_at_snapshot':>22s}")
    print("-" * 86)

    candidates: list[tuple[int, int]] = []
    for row in snapshots:
        snapshot_id = int(row["snapshot_id"])
        count_row = spark.sql(
            f"SELECT COUNT(*) AS row_count FROM {table} VERSION AS OF {snapshot_id}"
        ).collect()[0]
        row_count = int(count_row["row_count"])
        print(
            f"{str(row['committed_at']):26s} {snapshot_id:20d} "
            f"{str(row['operation']):>12s} {row_count:22d}"
        )
        if row_count > 0:
            candidates.append((snapshot_id, row_count))

    current_count = int(spark.sql(f"SELECT COUNT(*) AS row_count FROM {table}").collect()[0]["row_count"])
    print(f"\ncurrent_row_count={current_count}")

    if candidates:
        snapshot_id, row_count = candidates[0]
        print(
            "\nRECOVERY_POINT_CANDIDATE "
            f"snapshot_id={snapshot_id} row_count={row_count}"
        )
        print(
            "Time travel check:\n"
            f"  SELECT COUNT(*) FROM {table} VERSION AS OF {snapshot_id};"
        )
    else:
        print("\nNo non-empty snapshot candidate found in the inspected range.")

    print(
        "\nNote: this script only finds and verifies a recovery point. "
        "It does not execute rollback. In production, rollback/cutover is a separate approval step."
    )

    spark.stop()


if __name__ == "__main__":
    main()
