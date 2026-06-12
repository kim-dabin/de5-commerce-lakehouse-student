#!/usr/bin/env python3
"""Validate Olist Paimon table counts for Airflow operational checks."""

from __future__ import annotations

import json
import os

from pyspark.sql import SparkSession


EXPECTED_COUNTS = {
    "ux_events_bronze": 16_693,
    "review_current": 1_971,
    "order_current": 2_000,
}


def main() -> None:
    spark = SparkSession.builder.appName("de5-olist-paimon-count-validation").getOrCreate()
    spark.sparkContext.setLogLevel(os.getenv("SPARK_LOG_LEVEL", "ERROR"))
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    paimon_catalog = os.getenv("PAIMON_CATALOG", "paimon_lake")
    paimon_namespace = os.getenv("PAIMON_NAMESPACE", "bronze")

    actual_counts: dict[str, int] = {}
    for table_name in EXPECTED_COUNTS:
        full_table_name = f"{paimon_catalog}.{paimon_namespace}.{table_name}"
        row_count = spark.sql(f"SELECT COUNT(*) AS row_count FROM {full_table_name}").collect()[0][
            "row_count"
        ]
        actual_counts[table_name] = int(row_count)

    print("PAIMON_COUNTS_JSON=" + json.dumps(actual_counts, sort_keys=True))

    mismatches = {
        table_name: {"expected": expected, "actual": actual_counts.get(table_name)}
        for table_name, expected in EXPECTED_COUNTS.items()
        if actual_counts.get(table_name) != expected
    }
    if mismatches:
        raise RuntimeError("Paimon count mismatch: " + json.dumps(mismatches, sort_keys=True))

    print(
        "Paimon counts validated. review_current/order_current are smaller than "
        "their Kafka event inputs because they are primary-key current-state tables."
    )

    spark.stop()


if __name__ == "__main__":
    main()
