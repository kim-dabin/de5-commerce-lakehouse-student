#!/usr/bin/env python3
"""Emit batch BI metrics by querying Iceberg tables through StarRocks."""

from __future__ import annotations

import json
import os
import subprocess
from decimal import Decimal
from pathlib import Path
from typing import Any, Callable


ROOT = Path(__file__).resolve().parents[1]
COMPOSE_FILE = os.getenv("COMPOSE_FILE", "docker-compose.lite.yml")
JSON_PREFIX = "BI_METRICS_JSON="
DEFAULT_QUERY_TIMEOUT = int(os.getenv("STARROCKS_QUERY_TIMEOUT", "300"))


def parse_value(value: str) -> Any:
    if value in {"", "NULL", r"\N"}:
        return None
    try:
        if "." in value:
            return float(Decimal(value))
        return int(value)
    except Exception:
        return value


def run_starrocks_query(sql: str, timeout: int | None = None) -> list[list[str]]:
    if timeout is None:
        timeout = DEFAULT_QUERY_TIMEOUT
    result = subprocess.run(
        [
            "docker",
            "compose",
            "-f",
            COMPOSE_FILE,
            "exec",
            "-T",
            "starrocks-fe",
            "mysql",
            "-h127.0.0.1",
            "-P9030",
            "-uroot",
            "--batch",
            "--raw",
            "--skip-column-names",
            "-e",
            sql,
        ],
        cwd=ROOT,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        timeout=timeout,
        check=False,
    )
    if result.returncode != 0:
        raise RuntimeError(result.stdout[-8000:])
    return [line.split("\t") for line in result.stdout.splitlines() if line.strip()]


def execute_starrocks(sql: str, timeout: int | None = None) -> None:
    run_starrocks_query(sql, timeout=timeout)


def rows_to_dicts(
    sql: str,
    columns: list[str],
    converters: dict[str, Callable[[str], Any]] | None = None,
    timeout: int | None = None,
) -> list[dict[str, Any]]:
    rows = []
    for values in run_starrocks_query(sql, timeout=timeout):
        row: dict[str, Any] = {}
        for column, value in zip(columns, values):
            if converters and column in converters:
                row[column] = converters[column](value)
            else:
                row[column] = parse_value(value)
        rows.append(row)
    return rows


def first_row(sql: str, columns: list[str], timeout: int | None = None) -> dict[str, Any]:
    rows = rows_to_dicts(sql, columns, timeout=timeout)
    return rows[0] if rows else {}


def reset_iceberg_catalog() -> None:
    execute_starrocks(
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
        """,
        timeout=60,
    )


def main() -> None:
    reset_iceberg_catalog()

    ux_table = "iceberg_olist.analytics.olist_ux_events_clean"
    review_table = "iceberg_olist.analytics.olist_review_current"
    order_table = "iceberg_olist.analytics.olist_order_current"
    event_daily_table = "iceberg_olist.analytics.olist_event_type_daily"
    funnel_daily_table = "iceberg_olist.analytics.olist_funnel_daily"
    category_daily_table = "iceberg_olist.analytics.olist_category_daily"
    review_sentiment_table = "iceberg_olist.analytics.olist_review_sentiment_by_category"

    totals = first_row(
        f"""
        SELECT
          COUNT(*) AS total_events,
          COUNT(DISTINCT event_type) AS event_types,
          COUNT(DISTINCT product_id) AS products,
          COUNT(DISTINCT user_id) AS users,
          COUNT(DISTINCT session_id) AS sessions,
          COUNT(DISTINCT order_id) AS orders,
          SUM(IF(event_type = 'purchase', price, 0)) AS revenue,
          MIN(event_date) AS first_event_date,
          MAX(event_date) AS last_event_date
        FROM {ux_table}
        """,
        [
            "total_events",
            "event_types",
            "products",
            "users",
            "sessions",
            "orders",
            "revenue",
            "first_event_date",
            "last_event_date",
        ],
    )

    review_totals = first_row(
        f"""
        SELECT
          COUNT(*) AS reviews,
          COUNT(DISTINCT product_id) AS reviewed_products,
          AVG(rating) AS avg_rating,
          SUM(IF(sentiment = 'negative', 1, 0)) AS negative_reviews,
          SUM(IF(sentiment = 'neutral', 1, 0)) AS neutral_reviews,
          SUM(IF(sentiment = 'positive', 1, 0)) AS positive_reviews
        FROM {review_table}
        """,
        [
            "reviews",
            "reviewed_products",
            "avg_rating",
            "negative_reviews",
            "neutral_reviews",
            "positive_reviews",
        ],
    )

    order_status_current = rows_to_dicts(
        f"""
        SELECT
          last_event_type,
          COUNT(*) AS count
        FROM {order_table}
        GROUP BY last_event_type
        ORDER BY last_event_type
        """,
        ["last_event_type", "count"],
    )

    event_type_daily = rows_to_dicts(
        f"""
        SELECT
          event_date,
          event_type,
          event_count,
          user_count,
          session_count,
          product_count,
          revenue
        FROM {event_daily_table}
        ORDER BY event_date, event_type
        """,
        [
            "event_date",
            "event_type",
            "event_count",
            "user_count",
            "session_count",
            "product_count",
            "revenue",
        ],
    )

    funnel_daily = rows_to_dicts(
        f"""
        SELECT
          event_date,
          sessions,
          search_click_sessions,
          product_view_sessions,
          review_impression_sessions,
          review_expand_sessions,
          add_to_cart_sessions,
          purchase_sessions,
          revenue
        FROM {funnel_daily_table}
        ORDER BY event_date
        """,
        [
            "event_date",
            "sessions",
            "search_click_sessions",
            "product_view_sessions",
            "review_impression_sessions",
            "review_expand_sessions",
            "add_to_cart_sessions",
            "purchase_sessions",
            "revenue",
        ],
    )

    category_daily = rows_to_dicts(
        f"""
        SELECT
          event_date,
          category_code,
          event_count,
          search_result_click_count,
          product_view_count,
          review_impression_count,
          review_expand_count,
          add_to_cart_count,
          remove_from_cart_count,
          purchase_count,
          user_count,
          session_count,
          revenue
        FROM {category_daily_table}
        ORDER BY event_date, revenue DESC, category_code
        """,
        [
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
        ],
    )

    top_categories = rows_to_dicts(
        f"""
        SELECT
          category_code,
          COUNT(*) AS event_count,
          SUM(IF(event_type = 'product_view', 1, 0)) AS product_view_count,
          SUM(IF(event_type = 'add_to_cart', 1, 0)) AS add_to_cart_count,
          SUM(IF(event_type = 'purchase', 1, 0)) AS purchase_count,
          SUM(IF(event_type = 'purchase', price, 0)) AS revenue
        FROM {ux_table}
        GROUP BY category_code
        ORDER BY revenue DESC, event_count DESC, category_code
        LIMIT 10
        """,
        [
            "category_code",
            "event_count",
            "product_view_count",
            "add_to_cart_count",
            "purchase_count",
            "revenue",
        ],
    )

    top_brands = rows_to_dicts(
        f"""
        SELECT
          brand,
          COUNT(*) AS event_count,
          SUM(IF(event_type = 'purchase', price, 0)) AS revenue
        FROM {ux_table}
        GROUP BY brand
        ORDER BY revenue DESC, event_count DESC, brand
        LIMIT 10
        """,
        ["brand", "event_count", "revenue"],
    )

    review_sentiment = rows_to_dicts(
        f"""
        SELECT
          category_code,
          sentiment,
          review_count,
          avg_rating,
          reviewed_product_count
        FROM {review_sentiment_table}
        ORDER BY category_code, sentiment
        """,
        ["category_code", "sentiment", "review_count", "avg_rating", "reviewed_product_count"],
    )

    top_negative_review_categories = rows_to_dicts(
        f"""
        SELECT
          category_code,
          COUNT(*) AS review_count,
          AVG(rating) AS avg_rating,
          SUM(IF(sentiment = 'negative', 1, 0)) AS negative_reviews,
          IF(COUNT(*) = 0, 0, SUM(IF(sentiment = 'negative', 1, 0)) / COUNT(*)) AS negative_review_ratio
        FROM {review_table}
        GROUP BY category_code
        ORDER BY negative_review_ratio DESC, negative_reviews DESC, category_code
        LIMIT 10
        """,
        ["category_code", "review_count", "avg_rating", "negative_reviews", "negative_review_ratio"],
    )

    review_impact_summary = first_row(
        f"""
        WITH session_product AS (
          SELECT
            session_id,
            product_id,
            MIN(IF(event_type = 'product_view', event_time_ts, NULL)) AS product_view_at,
            MIN(IF(event_type IN ('review_impression', 'review_expand'), event_time_ts, NULL)) AS review_seen_at,
            MIN(IF(event_type = 'review_expand', event_time_ts, NULL)) AS review_expand_at,
            MIN(IF(event_type = 'add_to_cart', event_time_ts, NULL)) AS add_to_cart_at,
            MIN(IF(event_type = 'purchase', event_time_ts, NULL)) AS purchase_at
          FROM {ux_table}
          WHERE product_id IS NOT NULL
          GROUP BY session_id, product_id
        ),
        counts AS (
          SELECT
            COUNT(*) AS session_product_pairs,
            SUM(IF(product_view_at IS NOT NULL, 1, 0)) AS product_view_pairs,
            SUM(IF(review_seen_at IS NOT NULL, 1, 0)) AS review_seen_pairs,
            SUM(IF(add_to_cart_at IS NOT NULL, 1, 0)) AS add_to_cart_pairs,
            SUM(IF(purchase_at IS NOT NULL, 1, 0)) AS purchase_pairs,
            SUM(IF(
              review_seen_at IS NOT NULL
              AND add_to_cart_at IS NOT NULL
              AND add_to_cart_at >= review_seen_at,
              1,
              0
            )) AS cart_after_review_pairs,
            SUM(IF(
              review_seen_at IS NOT NULL
              AND purchase_at IS NOT NULL
              AND purchase_at >= review_seen_at,
              1,
              0
            )) AS purchase_after_review_pairs,
            SUM(IF(
              product_view_at IS NOT NULL
              AND add_to_cart_at IS NULL
              AND purchase_at IS NULL,
              1,
              0
            )) AS pdp_exit_pairs
          FROM session_product
        )
        SELECT
          session_product_pairs,
          product_view_pairs,
          review_seen_pairs,
          add_to_cart_pairs,
          purchase_pairs,
          cart_after_review_pairs,
          purchase_after_review_pairs,
          pdp_exit_pairs,
          IF(product_view_pairs = 0, 0, ROUND(add_to_cart_pairs * 100.0 / product_view_pairs, 2)) AS view_to_cart_rate,
          IF(product_view_pairs = 0, 0, ROUND(purchase_pairs * 100.0 / product_view_pairs, 2)) AS view_to_purchase_rate,
          IF(review_seen_pairs = 0, 0, ROUND(cart_after_review_pairs * 100.0 / review_seen_pairs, 2)) AS cart_after_review_rate,
          IF(review_seen_pairs = 0, 0, ROUND(purchase_after_review_pairs * 100.0 / review_seen_pairs, 2)) AS purchase_after_review_rate,
          IF(product_view_pairs = 0, 0, ROUND(pdp_exit_pairs * 100.0 / product_view_pairs, 2)) AS pdp_exit_rate
        FROM counts
        """,
        [
            "session_product_pairs",
            "product_view_pairs",
            "review_seen_pairs",
            "add_to_cart_pairs",
            "purchase_pairs",
            "cart_after_review_pairs",
            "purchase_after_review_pairs",
            "pdp_exit_pairs",
            "view_to_cart_rate",
            "view_to_purchase_rate",
            "cart_after_review_rate",
            "purchase_after_review_rate",
            "pdp_exit_rate",
        ],
    )

    review_risk_category = rows_to_dicts(
        f"""
        WITH session_product AS (
          SELECT
            product_id,
            MAX(category_code) AS category_code,
            MAX(brand) AS brand,
            session_id,
            MIN(IF(event_type = 'product_view', event_time_ts, NULL)) AS product_view_at,
            MIN(IF(event_type IN ('review_impression', 'review_expand'), event_time_ts, NULL)) AS review_seen_at,
            MIN(IF(event_type = 'add_to_cart', event_time_ts, NULL)) AS add_to_cart_at,
            MIN(IF(event_type = 'purchase', event_time_ts, NULL)) AS purchase_at,
            SUM(IF(event_type = 'purchase', price, 0)) AS revenue
          FROM {ux_table}
          WHERE product_id IS NOT NULL
          GROUP BY product_id, session_id
        ),
        product_behavior AS (
          SELECT
            product_id,
            MAX(category_code) AS category_code,
            SUM(IF(product_view_at IS NOT NULL, 1, 0)) AS product_view_sessions,
            SUM(IF(review_seen_at IS NOT NULL, 1, 0)) AS review_seen_sessions,
            SUM(IF(add_to_cart_at IS NOT NULL, 1, 0)) AS add_to_cart_sessions,
            SUM(IF(purchase_at IS NOT NULL, 1, 0)) AS purchase_sessions,
            SUM(IF(
              product_view_at IS NOT NULL
              AND add_to_cart_at IS NULL
              AND purchase_at IS NULL,
              1,
              0
            )) AS pdp_exit_sessions,
            SUM(revenue) AS revenue
          FROM session_product
          GROUP BY product_id
        ),
        product_reviews AS (
          SELECT
            product_id,
            MAX(COALESCE(category_code, 'unknown')) AS review_category_code,
            COUNT(*) AS review_count,
            SUM(IF(sentiment = 'negative', 1, 0)) AS negative_review_count,
            AVG(rating) AS avg_rating
          FROM {review_table}
          WHERE product_id IS NOT NULL
          GROUP BY product_id
        ),
        product_risk AS (
          SELECT
            behavior.product_id,
            COALESCE(behavior.category_code, reviews.review_category_code, 'unknown') AS category_code,
            reviews.review_count,
            reviews.negative_review_count,
            reviews.avg_rating,
            behavior.product_view_sessions,
            behavior.review_seen_sessions,
            behavior.add_to_cart_sessions,
            behavior.purchase_sessions,
            behavior.pdp_exit_sessions,
            behavior.revenue
          FROM product_behavior behavior
          JOIN product_reviews reviews
            ON behavior.product_id = reviews.product_id
        )
        SELECT
          category_code,
          COUNT(DISTINCT product_id) AS product_count,
          SUM(review_count) AS review_count,
          SUM(negative_review_count) AS negative_review_count,
          AVG(avg_rating) AS avg_rating,
          SUM(product_view_sessions) AS product_view_sessions,
          SUM(review_seen_sessions) AS review_seen_sessions,
          SUM(add_to_cart_sessions) AS add_to_cart_sessions,
          SUM(purchase_sessions) AS purchase_sessions,
          SUM(pdp_exit_sessions) AS pdp_exit_sessions,
          SUM(revenue) AS revenue,
          IF(SUM(review_count) = 0, 0, ROUND(SUM(negative_review_count) * 100.0 / SUM(review_count), 2)) AS negative_review_ratio,
          IF(SUM(product_view_sessions) = 0, 0, ROUND(SUM(pdp_exit_sessions) * 100.0 / SUM(product_view_sessions), 2)) AS pdp_exit_rate,
          IF(SUM(product_view_sessions) = 0, 0, ROUND(SUM(purchase_sessions) * 100.0 / SUM(product_view_sessions), 2)) AS purchase_rate
        FROM product_risk
        GROUP BY category_code
        ORDER BY revenue DESC, negative_review_ratio DESC, category_code
        LIMIT 20
        """,
        [
            "category_code",
            "product_count",
            "review_count",
            "negative_review_count",
            "avg_rating",
            "product_view_sessions",
            "review_seen_sessions",
            "add_to_cart_sessions",
            "purchase_sessions",
            "pdp_exit_sessions",
            "revenue",
            "negative_review_ratio",
            "pdp_exit_rate",
            "purchase_rate",
        ],
        timeout=120,
    )

    review_risk_products = rows_to_dicts(
        f"""
        WITH session_product AS (
          SELECT
            product_id,
            MAX(category_code) AS category_code,
            MAX(brand) AS brand,
            session_id,
            MIN(IF(event_type = 'product_view', event_time_ts, NULL)) AS product_view_at,
            MIN(IF(event_type IN ('review_impression', 'review_expand'), event_time_ts, NULL)) AS review_seen_at,
            MIN(IF(event_type = 'add_to_cart', event_time_ts, NULL)) AS add_to_cart_at,
            MIN(IF(event_type = 'purchase', event_time_ts, NULL)) AS purchase_at,
            SUM(IF(event_type = 'purchase', price, 0)) AS revenue
          FROM {ux_table}
          WHERE product_id IS NOT NULL
          GROUP BY product_id, session_id
        ),
        product_behavior AS (
          SELECT
            product_id,
            MAX(category_code) AS category_code,
            MAX(brand) AS brand,
            SUM(IF(product_view_at IS NOT NULL, 1, 0)) AS product_view_sessions,
            SUM(IF(review_seen_at IS NOT NULL, 1, 0)) AS review_seen_sessions,
            SUM(IF(add_to_cart_at IS NOT NULL, 1, 0)) AS add_to_cart_sessions,
            SUM(IF(purchase_at IS NOT NULL, 1, 0)) AS purchase_sessions,
            SUM(IF(
              product_view_at IS NOT NULL
              AND add_to_cart_at IS NULL
              AND purchase_at IS NULL,
              1,
              0
            )) AS pdp_exit_sessions,
            SUM(revenue) AS revenue
          FROM session_product
          GROUP BY product_id
        ),
        product_reviews AS (
          SELECT
            product_id,
            MAX(COALESCE(category_code, 'unknown')) AS review_category_code,
            COUNT(*) AS review_count,
            SUM(IF(sentiment = 'negative', 1, 0)) AS negative_review_count,
            AVG(rating) AS avg_rating
          FROM {review_table}
          WHERE product_id IS NOT NULL
          GROUP BY product_id
        )
        SELECT
          behavior.product_id,
          COALESCE(behavior.category_code, reviews.review_category_code, 'unknown') AS category_code,
          behavior.brand,
          reviews.review_count,
          reviews.negative_review_count,
          IF(reviews.review_count = 0, 0, ROUND(reviews.negative_review_count * 100.0 / reviews.review_count, 2)) AS negative_review_ratio,
          reviews.avg_rating,
          behavior.product_view_sessions,
          behavior.review_seen_sessions,
          behavior.add_to_cart_sessions,
          behavior.purchase_sessions,
          behavior.pdp_exit_sessions,
          IF(behavior.product_view_sessions = 0, 0, ROUND(behavior.pdp_exit_sessions * 100.0 / behavior.product_view_sessions, 2)) AS pdp_exit_rate,
          IF(behavior.product_view_sessions = 0, 0, ROUND(behavior.purchase_sessions * 100.0 / behavior.product_view_sessions, 2)) AS purchase_rate,
          behavior.revenue
        FROM product_behavior behavior
        JOIN product_reviews reviews
          ON behavior.product_id = reviews.product_id
        WHERE behavior.product_view_sessions >= 2
        ORDER BY negative_review_ratio DESC, pdp_exit_rate DESC, product_view_sessions DESC, behavior.product_id
        LIMIT 20
        """,
        [
            "product_id",
            "category_code",
            "brand",
            "review_count",
            "negative_review_count",
            "negative_review_ratio",
            "avg_rating",
            "product_view_sessions",
            "review_seen_sessions",
            "add_to_cart_sessions",
            "purchase_sessions",
            "pdp_exit_sessions",
            "pdp_exit_rate",
            "purchase_rate",
            "revenue",
        ],
        timeout=120,
    )

    payload = {
        "source": {
            "engine": "StarRocks",
            "catalog": "iceberg_olist",
            "database": "analytics",
            "mode": "iceberg_external_catalog",
        },
        "totals": totals,
        "review_totals": review_totals,
        "order_status_current": order_status_current,
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

    print(JSON_PREFIX + json.dumps(payload, ensure_ascii=False))


if __name__ == "__main__":
    main()
