#!/usr/bin/env python3
"""Transform Olist Paimon Bronze/current tables into Iceberg analytics tables."""

from __future__ import annotations

import os

from pyspark.sql import SparkSession


def main() -> None:
    spark = SparkSession.builder.appName("de5-olist-spark-iceberg-transform").getOrCreate()
    spark.sparkContext.setLogLevel(os.getenv("SPARK_LOG_LEVEL", "WARN"))
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    iceberg_catalog = os.getenv("ICEBERG_CATALOG", "iceberg_lake")
    iceberg_namespace = os.getenv("ICEBERG_NAMESPACE", "analytics")

    ux_path = os.getenv(
        "PAIMON_UX_PATH",
        "file:/warehouse/paimon/bronze.db/ux_events_bronze",
    )
    review_path = os.getenv(
        "PAIMON_REVIEW_PATH",
        "file:/warehouse/paimon/bronze.db/review_current",
    )
    order_path = os.getenv(
        "PAIMON_ORDER_PATH",
        "file:/warehouse/paimon/bronze.db/order_current",
    )

    ux_clean_table = f"{iceberg_catalog}.{iceberg_namespace}.olist_ux_events_clean"
    review_current_table = f"{iceberg_catalog}.{iceberg_namespace}.olist_review_current"
    order_current_table = f"{iceberg_catalog}.{iceberg_namespace}.olist_order_current"
    event_daily_table = f"{iceberg_catalog}.{iceberg_namespace}.olist_event_type_daily"
    funnel_daily_table = f"{iceberg_catalog}.{iceberg_namespace}.olist_funnel_daily"
    category_daily_table = f"{iceberg_catalog}.{iceberg_namespace}.olist_category_daily"
    review_sentiment_table = (
        f"{iceberg_catalog}.{iceberg_namespace}.olist_review_sentiment_by_category"
    )

    spark.read.format("paimon").load(ux_path).createOrReplaceTempView("ux_events_bronze")
    spark.read.format("paimon").load(review_path).createOrReplaceTempView("review_current")
    spark.read.format("paimon").load(order_path).createOrReplaceTempView("order_current")

    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {iceberg_catalog}.{iceberg_namespace}")

    for table in [
        review_sentiment_table,
        category_daily_table,
        event_daily_table,
        funnel_daily_table,
        order_current_table,
        review_current_table,
        ux_clean_table,
    ]:
        spark.sql(f"DROP TABLE IF EXISTS {table}")

    spark.sql(
        f"""
        CREATE TABLE {ux_clean_table}
        USING iceberg
        PARTITIONED BY (event_date)
        TBLPROPERTIES ('format-version' = '2')
        AS
        SELECT
          event_id,
          event_type,
          to_timestamp(event_time_text, "yyyy-MM-dd'T'HH:mm:ssX") AS event_time_ts,
          to_date(to_timestamp(event_time_text, "yyyy-MM-dd'T'HH:mm:ssX")) AS event_date,
          order_id,
          product_id,
          source_product_id,
          catalog_id,
          category_id,
          COALESCE(category_code, 'unknown') AS category_code,
          COALESCE(brand, 'unknown') AS brand,
          COALESCE(price, CAST(0 AS DECIMAL(12, 2))) AS price,
          user_id,
          source_customer_id,
          session_id,
          is_synthetic_ux,
          raw_json,
          ingested_at
        FROM ux_events_bronze
        WHERE event_id IS NOT NULL
        """
    )

    spark.sql(
        f"""
        CREATE TABLE {review_current_table}
        USING iceberg
        PARTITIONED BY (category_code)
        TBLPROPERTIES ('format-version' = '2')
        AS
        SELECT
          review_id,
          last_event_type,
          to_timestamp(updated_at_text, "yyyy-MM-dd'T'HH:mm:ssX") AS updated_at_ts,
          order_id,
          product_id,
          source_product_id,
          catalog_id,
          category_id,
          COALESCE(category_code, 'unknown') AS category_code,
          rating,
          COALESCE(sentiment, 'unknown') AS sentiment,
          is_used,
          matched_product_id,
          review_title,
          review_text,
          raw_json,
          ingested_at
        FROM review_current
        WHERE review_id IS NOT NULL
        """
    )

    spark.sql(
        f"""
        CREATE TABLE {order_current_table}
        USING iceberg
        TBLPROPERTIES ('format-version' = '2')
        AS
        SELECT
          order_id,
          last_event_type,
          to_timestamp(updated_at_text, "yyyy-MM-dd'T'HH:mm:ssX") AS updated_at_ts,
          COALESCE(order_status, 'unknown') AS order_status,
          user_id,
          source_customer_id,
          session_id,
          raw_json,
          ingested_at
        FROM order_current
        WHERE order_id IS NOT NULL
        """
    )

    spark.sql(
        f"""
        CREATE TABLE {event_daily_table}
        USING iceberg
        PARTITIONED BY (event_date)
        TBLPROPERTIES ('format-version' = '2')
        AS
        SELECT
          event_date,
          event_type,
          COUNT(*) AS event_count,
          COUNT(DISTINCT user_id) AS user_count,
          COUNT(DISTINCT session_id) AS session_count,
          COUNT(DISTINCT product_id) AS product_count,
          SUM(CASE WHEN event_type = 'purchase' THEN price ELSE CAST(0 AS DECIMAL(12, 2)) END) AS revenue,
          MIN(event_time_ts) AS first_event_at,
          MAX(event_time_ts) AS last_event_at
        FROM {ux_clean_table}
        GROUP BY event_date, event_type
        """
    )

    spark.sql(
        f"""
        CREATE TABLE {funnel_daily_table}
        USING iceberg
        PARTITIONED BY (event_date)
        TBLPROPERTIES ('format-version' = '2')
        AS
        SELECT
          event_date,
          COUNT(DISTINCT session_id) AS sessions,
          COUNT(DISTINCT CASE WHEN event_type = 'search_result_click' THEN session_id END) AS search_click_sessions,
          COUNT(DISTINCT CASE WHEN event_type = 'product_view' THEN session_id END) AS product_view_sessions,
          COUNT(DISTINCT CASE WHEN event_type = 'review_impression' THEN session_id END) AS review_impression_sessions,
          COUNT(DISTINCT CASE WHEN event_type = 'review_expand' THEN session_id END) AS review_expand_sessions,
          COUNT(DISTINCT CASE WHEN event_type = 'add_to_cart' THEN session_id END) AS add_to_cart_sessions,
          COUNT(DISTINCT CASE WHEN event_type = 'purchase' THEN session_id END) AS purchase_sessions,
          SUM(CASE WHEN event_type = 'purchase' THEN price ELSE CAST(0 AS DECIMAL(12, 2)) END) AS revenue
        FROM {ux_clean_table}
        GROUP BY event_date
        """
    )

    spark.sql(
        f"""
        CREATE TABLE {category_daily_table}
        USING iceberg
        PARTITIONED BY (event_date)
        TBLPROPERTIES ('format-version' = '2')
        AS
        SELECT
          event_date,
          category_code,
          COUNT(*) AS event_count,
          SUM(CASE WHEN event_type = 'search_result_click' THEN 1 ELSE 0 END) AS search_result_click_count,
          SUM(CASE WHEN event_type = 'product_view' THEN 1 ELSE 0 END) AS product_view_count,
          SUM(CASE WHEN event_type = 'review_impression' THEN 1 ELSE 0 END) AS review_impression_count,
          SUM(CASE WHEN event_type = 'review_expand' THEN 1 ELSE 0 END) AS review_expand_count,
          SUM(CASE WHEN event_type = 'add_to_cart' THEN 1 ELSE 0 END) AS add_to_cart_count,
          SUM(CASE WHEN event_type = 'remove_from_cart' THEN 1 ELSE 0 END) AS remove_from_cart_count,
          SUM(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END) AS purchase_count,
          COUNT(DISTINCT user_id) AS user_count,
          COUNT(DISTINCT session_id) AS session_count,
          SUM(CASE WHEN event_type = 'purchase' THEN price ELSE CAST(0 AS DECIMAL(12, 2)) END) AS revenue
        FROM {ux_clean_table}
        GROUP BY event_date, category_code
        """
    )

    spark.sql(
        f"""
        CREATE TABLE {review_sentiment_table}
        USING iceberg
        PARTITIONED BY (category_code)
        TBLPROPERTIES ('format-version' = '2')
        AS
        SELECT
          category_code,
          sentiment,
          COUNT(*) AS review_count,
          AVG(CAST(rating AS DOUBLE)) AS avg_rating,
          COUNT(DISTINCT product_id) AS reviewed_product_count
        FROM {review_current_table}
        GROUP BY category_code, sentiment
        """
    )

    tables = [
        ux_clean_table,
        review_current_table,
        order_current_table,
        event_daily_table,
        funnel_daily_table,
        category_daily_table,
        review_sentiment_table,
    ]
    for table in tables:
        print(f"created={table} rows={spark.table(table).count()}")

    spark.table(event_daily_table).orderBy("event_date", "event_type").show(50, truncate=False)
    spark.table(category_daily_table).orderBy("event_date", "category_code").show(50, truncate=False)
    spark.table(review_sentiment_table).orderBy("category_code", "sentiment").show(50, truncate=False)
    spark.stop()


if __name__ == "__main__":
    main()
