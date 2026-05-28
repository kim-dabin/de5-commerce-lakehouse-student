#!/usr/bin/env python3
"""Transform Paimon Bronze commerce events into Iceberg analytics tables."""

import os

from pyspark.sql import SparkSession


def main() -> None:
    spark = SparkSession.builder.appName("de5-spark-iceberg-transform").getOrCreate()
    spark.sparkContext.setLogLevel(os.getenv("SPARK_LOG_LEVEL", "WARN"))
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    paimon_bronze_path = os.getenv(
        "PAIMON_BRONZE_PATH",
        "file:/warehouse/paimon/bronze.db/commerce_events_bronze",
    )
    iceberg_catalog = os.getenv("ICEBERG_CATALOG", "iceberg_lake")
    iceberg_namespace = os.getenv("ICEBERG_NAMESPACE", "analytics")

    clean_table = f"{iceberg_catalog}.{iceberg_namespace}.commerce_events_clean"
    event_daily_table = f"{iceberg_catalog}.{iceberg_namespace}.commerce_event_type_daily"
    category_daily_table = f"{iceberg_catalog}.{iceberg_namespace}.commerce_category_daily"

    bronze_df = spark.read.format("paimon").load(paimon_bronze_path)
    bronze_df.createOrReplaceTempView("commerce_events_bronze")

    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {iceberg_catalog}.{iceberg_namespace}")

    spark.sql(f"DROP TABLE IF EXISTS {clean_table}")
    spark.sql(
        f"""
        CREATE TABLE {clean_table}
        USING iceberg
        PARTITIONED BY (event_date)
        TBLPROPERTIES ('format-version' = '2')
        AS
        SELECT
          event_id,
          event_type,
          to_timestamp(event_time_text, "yyyy-MM-dd'T'HH:mm:ssX") AS event_time_ts,
          to_date(to_timestamp(event_time_text, "yyyy-MM-dd'T'HH:mm:ssX")) AS event_date,
          product_id,
          category_id,
          COALESCE(category_code, 'unknown') AS category_code,
          COALESCE(brand, 'unknown') AS brand,
          price,
          user_id,
          user_session,
          raw_json,
          ingested_at
        FROM commerce_events_bronze
        WHERE event_id IS NOT NULL
        """
    )

    spark.sql(f"DROP TABLE IF EXISTS {event_daily_table}")
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
          COUNT(DISTINCT user_session) AS session_count,
          COUNT(DISTINCT product_id) AS product_count,
          SUM(CASE WHEN event_type = 'purchase' THEN price ELSE CAST(0 AS DECIMAL(12, 2)) END) AS revenue,
          MIN(event_time_ts) AS first_event_at,
          MAX(event_time_ts) AS last_event_at
        FROM {clean_table}
        GROUP BY event_date, event_type
        """
    )

    spark.sql(f"DROP TABLE IF EXISTS {category_daily_table}")
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
          SUM(CASE WHEN event_type = 'view' THEN 1 ELSE 0 END) AS view_count,
          SUM(CASE WHEN event_type = 'cart' THEN 1 ELSE 0 END) AS cart_count,
          SUM(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END) AS purchase_count,
          COUNT(DISTINCT user_id) AS user_count,
          COUNT(DISTINCT user_session) AS session_count,
          SUM(CASE WHEN event_type = 'purchase' THEN price ELSE CAST(0 AS DECIMAL(12, 2)) END) AS revenue
        FROM {clean_table}
        GROUP BY event_date, category_code
        """
    )

    clean_count = spark.table(clean_table).count()
    event_daily_count = spark.table(event_daily_table).count()
    category_daily_count = spark.table(category_daily_table).count()

    print(f"created={clean_table} rows={clean_count}")
    print(f"created={event_daily_table} rows={event_daily_count}")
    print(f"created={category_daily_table} rows={category_daily_count}")

    spark.table(event_daily_table).orderBy("event_date", "event_type").show(truncate=False)
    spark.table(category_daily_table).orderBy("event_date", "category_code").show(truncate=False)
    spark.stop()


if __name__ == "__main__":
    main()
