#!/usr/bin/env python3
"""Emit Olist batch BI metrics from Iceberg analytics tables as JSON."""

from __future__ import annotations

import json
from datetime import date, datetime
from decimal import Decimal
from typing import Any

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


JSON_PREFIX = "BI_METRICS_JSON="


def json_default(value: Any) -> str | int | float:
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return float(value)
    return str(value)


def rows_to_dicts(rows: list[Any]) -> list[dict[str, Any]]:
    return [row.asDict(recursive=True) for row in rows]


def main() -> None:
    spark = SparkSession.builder.appName("de5-olist-bi-metrics").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    ux_clean_table = "iceberg_lake.analytics.olist_ux_events_clean"
    review_current_table = "iceberg_lake.analytics.olist_review_current"
    order_current_table = "iceberg_lake.analytics.olist_order_current"
    event_daily_table = "iceberg_lake.analytics.olist_event_type_daily"
    funnel_daily_table = "iceberg_lake.analytics.olist_funnel_daily"
    category_daily_table = "iceberg_lake.analytics.olist_category_daily"
    review_sentiment_table = "iceberg_lake.analytics.olist_review_sentiment_by_category"

    ux_df = spark.table(ux_clean_table)
    review_df = spark.table(review_current_table)
    order_df = spark.table(order_current_table)
    event_daily_df = spark.table(event_daily_table)
    funnel_daily_df = spark.table(funnel_daily_table)
    category_daily_df = spark.table(category_daily_table)
    review_sentiment_df = spark.table(review_sentiment_table)

    totals = ux_df.agg(
        F.count("*").alias("total_events"),
        F.countDistinct("event_type").alias("event_types"),
        F.countDistinct("product_id").alias("products"),
        F.countDistinct("user_id").alias("users"),
        F.countDistinct("session_id").alias("sessions"),
        F.countDistinct("order_id").alias("orders"),
        F.sum(F.when(F.col("event_type") == "purchase", F.col("price")).otherwise(F.lit(0))).alias("revenue"),
        F.min("event_date").alias("first_event_date"),
        F.max("event_date").alias("last_event_date"),
    ).collect()[0].asDict()

    review_totals = review_df.agg(
        F.count("*").alias("reviews"),
        F.countDistinct("product_id").alias("reviewed_products"),
        F.avg(F.col("rating").cast("double")).alias("avg_rating"),
        F.sum(F.when(F.col("sentiment") == "negative", 1).otherwise(0)).alias("negative_reviews"),
        F.sum(F.when(F.col("sentiment") == "neutral", 1).otherwise(0)).alias("neutral_reviews"),
        F.sum(F.when(F.col("sentiment") == "positive", 1).otherwise(0)).alias("positive_reviews"),
    ).collect()[0].asDict()

    order_totals = order_df.groupBy("last_event_type").count().orderBy("last_event_type")

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

    funnel_daily = rows_to_dicts(
        funnel_daily_df.orderBy("event_date").collect()
    )

    category_daily = rows_to_dicts(
        category_daily_df.select(
            "event_date",
            "category_code",
            "event_count",
            "search_result_click_count",
            "product_view_count",
            "review_impression_count",
            "review_expand_count",
            "add_to_cart_count",
            "remove_from_cart_count",
            "purchase_count",
            "user_count",
            "session_count",
            "revenue",
        )
        .orderBy("event_date", F.desc("revenue"), "category_code")
        .collect()
    )

    top_categories = rows_to_dicts(
        ux_df.groupBy("category_code")
        .agg(
            F.count("*").alias("event_count"),
            F.sum(F.when(F.col("event_type") == "product_view", 1).otherwise(0)).alias("product_view_count"),
            F.sum(F.when(F.col("event_type") == "add_to_cart", 1).otherwise(0)).alias("add_to_cart_count"),
            F.sum(F.when(F.col("event_type") == "purchase", 1).otherwise(0)).alias("purchase_count"),
            F.sum(F.when(F.col("event_type") == "purchase", F.col("price")).otherwise(F.lit(0))).alias("revenue"),
        )
        .orderBy(F.desc("revenue"), F.desc("event_count"), "category_code")
        .limit(10)
        .collect()
    )

    top_brands = rows_to_dicts(
        ux_df.groupBy("brand")
        .agg(
            F.count("*").alias("event_count"),
            F.sum(F.when(F.col("event_type") == "purchase", F.col("price")).otherwise(F.lit(0))).alias("revenue"),
        )
        .orderBy(F.desc("revenue"), F.desc("event_count"), "brand")
        .limit(10)
        .collect()
    )

    review_sentiment = rows_to_dicts(
        review_sentiment_df.orderBy("category_code", "sentiment").collect()
    )

    top_negative_review_categories = rows_to_dicts(
        review_df.groupBy("category_code")
        .agg(
            F.count("*").alias("review_count"),
            F.avg(F.col("rating").cast("double")).alias("avg_rating"),
            F.sum(F.when(F.col("sentiment") == "negative", 1).otherwise(0)).alias("negative_reviews"),
        )
        .withColumn(
            "negative_review_ratio",
            F.when(F.col("review_count") > 0, F.col("negative_reviews") / F.col("review_count")).otherwise(F.lit(0.0)),
        )
        .orderBy(F.desc("negative_review_ratio"), F.desc("negative_reviews"), "category_code")
        .limit(10)
        .collect()
    )

    session_product_flow_df = ux_df.groupBy("session_id", "product_id").agg(
        F.first("category_code", ignorenulls=True).alias("category_code"),
        F.first("brand", ignorenulls=True).alias("brand"),
        F.min(F.when(F.col("event_type") == "product_view", F.col("event_time_ts"))).alias("product_view_at"),
        F.min(
            F.when(F.col("event_type").isin("review_impression", "review_expand"), F.col("event_time_ts"))
        ).alias("review_seen_at"),
        F.min(F.when(F.col("event_type") == "review_expand", F.col("event_time_ts"))).alias("review_expand_at"),
        F.min(F.when(F.col("event_type") == "add_to_cart", F.col("event_time_ts"))).alias("add_to_cart_at"),
        F.min(F.when(F.col("event_type") == "purchase", F.col("event_time_ts"))).alias("purchase_at"),
        F.sum(F.when(F.col("event_type") == "purchase", F.col("price")).otherwise(F.lit(0))).alias("revenue"),
    )

    behavior_df = (
        session_product_flow_df.withColumn("has_product_view", F.col("product_view_at").isNotNull())
        .withColumn("has_review_seen", F.col("review_seen_at").isNotNull())
        .withColumn("has_add_to_cart", F.col("add_to_cart_at").isNotNull())
        .withColumn("has_purchase", F.col("purchase_at").isNotNull())
        .withColumn(
            "cart_after_review",
            F.col("has_review_seen") & F.col("has_add_to_cart") & (F.col("add_to_cart_at") >= F.col("review_seen_at")),
        )
        .withColumn(
            "purchase_after_review",
            F.col("has_review_seen") & F.col("has_purchase") & (F.col("purchase_at") >= F.col("review_seen_at")),
        )
        .withColumn("pdp_exit", F.col("has_product_view") & ~F.col("has_add_to_cart") & ~F.col("has_purchase"))
    )

    review_impact_summary = behavior_df.agg(
        F.count("*").alias("session_product_pairs"),
        F.sum(F.when(F.col("has_product_view"), 1).otherwise(0)).alias("product_view_pairs"),
        F.sum(F.when(F.col("has_review_seen"), 1).otherwise(0)).alias("review_seen_pairs"),
        F.sum(F.when(F.col("has_add_to_cart"), 1).otherwise(0)).alias("add_to_cart_pairs"),
        F.sum(F.when(F.col("has_purchase"), 1).otherwise(0)).alias("purchase_pairs"),
        F.sum(F.when(F.col("cart_after_review"), 1).otherwise(0)).alias("cart_after_review_pairs"),
        F.sum(F.when(F.col("purchase_after_review"), 1).otherwise(0)).alias("purchase_after_review_pairs"),
        F.sum(F.when(F.col("pdp_exit"), 1).otherwise(0)).alias("pdp_exit_pairs"),
    ).collect()[0].asDict()

    product_view_pairs = review_impact_summary.get("product_view_pairs") or 0
    review_seen_pairs = review_impact_summary.get("review_seen_pairs") or 0
    review_impact_summary["view_to_cart_rate"] = (
        (review_impact_summary.get("add_to_cart_pairs") or 0) * 100.0 / product_view_pairs
        if product_view_pairs
        else 0.0
    )
    review_impact_summary["view_to_purchase_rate"] = (
        (review_impact_summary.get("purchase_pairs") or 0) * 100.0 / product_view_pairs
        if product_view_pairs
        else 0.0
    )
    review_impact_summary["cart_after_review_rate"] = (
        (review_impact_summary.get("cart_after_review_pairs") or 0) * 100.0 / review_seen_pairs
        if review_seen_pairs
        else 0.0
    )
    review_impact_summary["purchase_after_review_rate"] = (
        (review_impact_summary.get("purchase_after_review_pairs") or 0) * 100.0 / review_seen_pairs
        if review_seen_pairs
        else 0.0
    )
    review_impact_summary["pdp_exit_rate"] = (
        (review_impact_summary.get("pdp_exit_pairs") or 0) * 100.0 / product_view_pairs
        if product_view_pairs
        else 0.0
    )

    behavior_by_product_df = behavior_df.groupBy("product_id").agg(
        F.first("category_code", ignorenulls=True).alias("category_code"),
        F.first("brand", ignorenulls=True).alias("brand"),
        F.sum(F.when(F.col("has_product_view"), 1).otherwise(0)).alias("product_view_sessions"),
        F.sum(F.when(F.col("has_review_seen"), 1).otherwise(0)).alias("review_seen_sessions"),
        F.sum(F.when(F.col("has_add_to_cart"), 1).otherwise(0)).alias("add_to_cart_sessions"),
        F.sum(F.when(F.col("has_purchase"), 1).otherwise(0)).alias("purchase_sessions"),
        F.sum(F.when(F.col("pdp_exit"), 1).otherwise(0)).alias("pdp_exit_sessions"),
        F.sum("revenue").alias("revenue"),
    )

    review_by_product_df = review_df.groupBy("product_id").agg(
        F.first("category_code", ignorenulls=True).alias("review_category_code"),
        F.count("*").alias("review_count"),
        F.sum(F.when(F.col("sentiment") == "negative", 1).otherwise(0)).alias("negative_review_count"),
        F.avg(F.col("rating").cast("double")).alias("avg_rating"),
    )

    review_risk_product_df = (
        behavior_by_product_df.join(review_by_product_df, "product_id", "inner")
        .withColumn("category_code", F.coalesce(F.col("category_code"), F.col("review_category_code"), F.lit("unknown")))
        .withColumn(
            "negative_review_ratio",
            F.when(F.col("review_count") > 0, F.col("negative_review_count") * 100.0 / F.col("review_count")).otherwise(
                F.lit(0.0)
            ),
        )
        .withColumn(
            "pdp_exit_rate",
            F.when(F.col("product_view_sessions") > 0, F.col("pdp_exit_sessions") * 100.0 / F.col("product_view_sessions")).otherwise(
                F.lit(0.0)
            ),
        )
        .withColumn(
            "purchase_rate",
            F.when(F.col("product_view_sessions") > 0, F.col("purchase_sessions") * 100.0 / F.col("product_view_sessions")).otherwise(
                F.lit(0.0)
            ),
        )
    )

    review_risk_category = rows_to_dicts(
        review_risk_product_df.groupBy("category_code")
        .agg(
            F.countDistinct("product_id").alias("product_count"),
            F.sum("review_count").alias("review_count"),
            F.sum("negative_review_count").alias("negative_review_count"),
            F.avg("avg_rating").alias("avg_rating"),
            F.sum("product_view_sessions").alias("product_view_sessions"),
            F.sum("review_seen_sessions").alias("review_seen_sessions"),
            F.sum("add_to_cart_sessions").alias("add_to_cart_sessions"),
            F.sum("purchase_sessions").alias("purchase_sessions"),
            F.sum("pdp_exit_sessions").alias("pdp_exit_sessions"),
            F.sum("revenue").alias("revenue"),
        )
        .withColumn(
            "negative_review_ratio",
            F.when(F.col("review_count") > 0, F.col("negative_review_count") * 100.0 / F.col("review_count")).otherwise(
                F.lit(0.0)
            ),
        )
        .withColumn(
            "pdp_exit_rate",
            F.when(F.col("product_view_sessions") > 0, F.col("pdp_exit_sessions") * 100.0 / F.col("product_view_sessions")).otherwise(
                F.lit(0.0)
            ),
        )
        .withColumn(
            "purchase_rate",
            F.when(F.col("product_view_sessions") > 0, F.col("purchase_sessions") * 100.0 / F.col("product_view_sessions")).otherwise(
                F.lit(0.0)
            ),
        )
        .orderBy(F.desc("revenue"), F.desc("negative_review_ratio"), "category_code")
        .limit(20)
        .collect()
    )

    review_risk_products = rows_to_dicts(
        review_risk_product_df.filter(F.col("product_view_sessions") >= 2)
        .orderBy(F.desc("negative_review_ratio"), F.desc("pdp_exit_rate"), F.desc("product_view_sessions"), "product_id")
        .limit(20)
        .collect()
    )

    payload = {
        "totals": totals,
        "review_totals": review_totals,
        "order_status_current": rows_to_dicts(order_totals.collect()),
        "event_type_daily": event_type_daily,
        "funnel_daily": funnel_daily,
        "category_daily": category_daily,
        "top_categories": top_categories,
        "top_brands": top_brands,
        "review_sentiment": review_sentiment,
        "top_negative_review_categories": top_negative_review_categories,
        "review_impact_summary": review_impact_summary,
        "review_risk_category": review_risk_category,
        "review_risk_products": review_risk_products,
    }

    print(JSON_PREFIX + json.dumps(payload, ensure_ascii=False, default=json_default))
    spark.stop()


if __name__ == "__main__":
    main()
