#!/usr/bin/env python3
"""Emit small BI metrics from the Iceberg analytics tables as JSON."""

from __future__ import annotations

import json
from datetime import date, datetime
from decimal import Decimal
from typing import Any

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def json_default(value: Any) -> str | int | float:
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return float(value)
    return str(value)


def rows_to_dicts(rows: list[Any]) -> list[dict[str, Any]]:
    return [row.asDict(recursive=True) for row in rows]


def main() -> None:
    spark = SparkSession.builder.appName("de5-bi-metrics").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    clean_table = "iceberg_lake.analytics.commerce_events_clean"
    event_daily_table = "iceberg_lake.analytics.commerce_event_type_daily"
    category_daily_table = "iceberg_lake.analytics.commerce_category_daily"

    clean_df = spark.table(clean_table)
    event_daily_df = spark.table(event_daily_table)
    category_daily_df = spark.table(category_daily_table)

    totals = clean_df.agg(
        F.count("*").alias("total_events"),
        F.countDistinct("event_type").alias("event_types"),
        F.countDistinct("product_id").alias("products"),
        F.countDistinct("user_id").alias("users"),
        F.countDistinct("user_session").alias("sessions"),
        F.sum(F.when(F.col("event_type") == "purchase", F.col("price")).otherwise(F.lit(0))).alias("revenue"),
        F.min("event_date").alias("first_event_date"),
        F.max("event_date").alias("last_event_date"),
    ).collect()[0].asDict()

    event_type_daily = rows_to_dicts(
        event_daily_df.select(
            "event_date",
            "event_type",
            "event_count",
            "user_count",
            "session_count",
            "product_count",
            "revenue",
        )
        .orderBy("event_date", "event_type")
        .collect()
    )

    category_daily = rows_to_dicts(
        category_daily_df.select(
            "event_date",
            "category_code",
            "event_count",
            "view_count",
            "cart_count",
            "purchase_count",
            "user_count",
            "session_count",
            "revenue",
        )
        .orderBy("event_date", F.desc("revenue"), "category_code")
        .collect()
    )

    top_categories = rows_to_dicts(
        clean_df.groupBy("category_code")
        .agg(
            F.count("*").alias("event_count"),
            F.sum(F.when(F.col("event_type") == "purchase", F.col("price")).otherwise(F.lit(0))).alias("revenue"),
        )
        .orderBy(F.desc("revenue"), F.desc("event_count"), "category_code")
        .limit(10)
        .collect()
    )

    top_brands = rows_to_dicts(
        clean_df.groupBy("brand")
        .agg(
            F.count("*").alias("event_count"),
            F.sum(F.when(F.col("event_type") == "purchase", F.col("price")).otherwise(F.lit(0))).alias("revenue"),
        )
        .orderBy(F.desc("revenue"), F.desc("event_count"), "brand")
        .limit(10)
        .collect()
    )

    payload = {
        "totals": totals,
        "event_type_daily": event_type_daily,
        "category_daily": category_daily,
        "top_categories": top_categories,
        "top_brands": top_brands,
    }

    print("BI_METRICS_JSON=" + json.dumps(payload, ensure_ascii=False, default=json_default))
    spark.stop()


if __name__ == "__main__":
    main()
