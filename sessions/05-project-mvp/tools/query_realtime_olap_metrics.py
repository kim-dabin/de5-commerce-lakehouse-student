#!/usr/bin/env python3
"""Emit small realtime OLAP metrics from StarRocks as JSON."""

from __future__ import annotations

import json
import os
import subprocess
from decimal import Decimal
from pathlib import Path
from typing import Any, Callable


ROOT = Path(__file__).resolve().parents[1]
COMPOSE_FILE = os.getenv("COMPOSE_FILE", "docker-compose.lite.yml")
JSON_PREFIX = "REALTIME_OLAP_JSON="
DEFAULT_QUERY_TIMEOUT = int(os.getenv("STARROCKS_QUERY_TIMEOUT", "300"))
STARROCKS_SESSION_PREFIX = "SET new_planner_optimize_timeout = 30000;\n"


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
            STARROCKS_SESSION_PREFIX + sql,
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


def rows_to_dicts(
    sql: str,
    columns: list[str],
    converters: dict[str, Callable[[str], Any]] | None = None,
) -> list[dict[str, Any]]:
    rows = []
    for values in run_starrocks_query(sql):
        row: dict[str, Any] = {}
        for column, value in zip(columns, values):
            if converters and column in converters:
                row[column] = converters[column](value)
            else:
                row[column] = parse_value(value)
        rows.append(row)
    return rows


def first_row(sql: str, columns: list[str]) -> dict[str, Any]:
    rows = rows_to_dicts(sql, columns)
    return rows[0] if rows else {}


def main() -> None:
    totals = first_row(
        """
        SELECT
          COUNT(*) AS total_events,
          COUNT(DISTINCT event_type) AS event_types,
          COUNT(DISTINCT product_id) AS products,
          COUNT(DISTINCT user_id) AS users,
          COUNT(DISTINCT user_session) AS sessions,
          SUM(IF(event_type = 'purchase', price, 0)) AS revenue,
          DATE_FORMAT(MIN(event_time_ts), '%Y-%m-%d %H:%i:%s') AS first_event_at,
          DATE_FORMAT(MAX(event_time_ts), '%Y-%m-%d %H:%i:%s') AS last_event_at
        FROM de5_realtime_olap.commerce_events_rt_typed
        """,
        [
            "total_events",
            "event_types",
            "products",
            "users",
            "sessions",
            "revenue",
            "first_event_at",
            "last_event_at",
        ],
    )

    event_type_realtime = rows_to_dicts(
        """
        SELECT
          event_type,
          event_count,
          user_count,
          session_count,
          product_count,
          revenue,
          DATE_FORMAT(first_event_at, '%Y-%m-%d %H:%i:%s') AS first_event_at,
          DATE_FORMAT(last_event_at, '%Y-%m-%d %H:%i:%s') AS last_event_at
        FROM de5_realtime_olap.commerce_event_type_realtime
        ORDER BY event_type
        """,
        [
            "event_type",
            "event_count",
            "user_count",
            "session_count",
            "product_count",
            "revenue",
            "first_event_at",
            "last_event_at",
        ],
    )

    category_realtime = rows_to_dicts(
        """
        SELECT
          category_code,
          event_count,
          view_count,
          cart_count,
          purchase_count,
          user_count,
          session_count,
          revenue,
          DATE_FORMAT(last_event_at, '%Y-%m-%d %H:%i:%s') AS last_event_at
        FROM de5_realtime_olap.commerce_category_realtime
        ORDER BY revenue DESC, event_count DESC, category_code
        LIMIT 20
        """,
        [
            "category_code",
            "event_count",
            "view_count",
            "cart_count",
            "purchase_count",
            "user_count",
            "session_count",
            "revenue",
            "last_event_at",
        ],
    )

    minute_realtime = rows_to_dicts(
        """
        SELECT
          DATE_FORMAT(event_minute, '%Y-%m-%d %H:%i:%s') AS event_minute,
          event_type,
          event_count,
          user_count,
          revenue
        FROM de5_realtime_olap.commerce_minute_event_type_realtime
        ORDER BY event_minute, event_type
        LIMIT 120
        """,
        ["event_minute", "event_type", "event_count", "user_count", "revenue"],
    )

    recent_events = rows_to_dicts(
        """
        SELECT
          event_id,
          DATE_FORMAT(event_time_ts, '%Y-%m-%d %H:%i:%s') AS event_time,
          event_type,
          category_code,
          brand,
          price,
          user_id,
          user_session
        FROM de5_realtime_olap.commerce_events_rt_typed
        ORDER BY event_time_ts DESC, event_id DESC
        LIMIT 20
        """,
        [
            "event_id",
            "event_time",
            "event_type",
            "category_code",
            "brand",
            "price",
            "user_id",
            "user_session",
        ],
    )

    review_sentiment = rows_to_dicts(
        """
        SELECT
          sentiment,
          review_count,
          product_count,
          avg_rating
        FROM de5_realtime_olap.review_sentiment_realtime
        ORDER BY sentiment
        """,
        ["sentiment", "review_count", "product_count", "avg_rating"],
    )

    order_status = rows_to_dicts(
        """
        SELECT
          order_status,
          order_count,
          user_count
        FROM de5_realtime_olap.order_status_realtime
        ORDER BY order_count DESC, order_status
        """,
        ["order_status", "order_count", "user_count"],
    )

    review_impact_summary = first_row(
        """
        SELECT
          session_product_pairs,
          product_view_pairs,
          review_seen_pairs,
          review_expand_pairs,
          cart_click_after_review_pairs,
          purchase_after_review_pairs,
          pdp_exit_pairs,
          cart_click_after_review_rate,
          purchase_after_review_rate,
          pdp_exit_rate
        FROM de5_realtime_olap.review_impact_summary_realtime
        """,
        [
            "session_product_pairs",
            "product_view_pairs",
            "review_seen_pairs",
            "review_expand_pairs",
            "cart_click_after_review_pairs",
            "purchase_after_review_pairs",
            "pdp_exit_pairs",
            "cart_click_after_review_rate",
            "purchase_after_review_rate",
            "pdp_exit_rate",
        ],
    )

    review_risk_category = rows_to_dicts(
        """
        SELECT
          category_code,
          product_count,
          review_count,
          negative_review_count,
          negative_review_ratio,
          product_view_sessions,
          pdp_exit_sessions,
          pdp_exit_rate,
          purchase_sessions,
          purchase_rate
        FROM de5_realtime_olap.review_risk_category_realtime
        WHERE product_view_sessions > 0
        ORDER BY negative_review_ratio DESC, product_view_sessions DESC, category_code
        LIMIT 20
        """,
        [
            "category_code",
            "product_count",
            "review_count",
            "negative_review_count",
            "negative_review_ratio",
            "product_view_sessions",
            "pdp_exit_sessions",
            "pdp_exit_rate",
            "purchase_sessions",
            "purchase_rate",
        ],
    )

    review_risk_products = rows_to_dicts(
        """
        SELECT
          product_id,
          category_code,
          review_count,
          negative_review_count,
          negative_review_ratio,
          avg_rating,
          product_view_sessions,
          pdp_exit_sessions,
          pdp_exit_rate,
          purchase_sessions,
          purchase_rate
        FROM de5_realtime_olap.review_risk_product_realtime
        WHERE product_view_sessions > 0
        ORDER BY negative_review_ratio DESC, product_view_sessions DESC, review_count DESC, product_id
        LIMIT 20
        """,
        [
            "product_id",
            "category_code",
            "review_count",
            "negative_review_count",
            "negative_review_ratio",
            "avg_rating",
            "product_view_sessions",
            "pdp_exit_sessions",
            "pdp_exit_rate",
            "purchase_sessions",
            "purchase_rate",
        ],
    )

    payload = {
        "source": {
            "engine": "StarRocks",
            "catalog": "paimon_olist",
            "database": "bronze",
            "ux_table": "ux_events_bronze",
            "review_table": "review_current",
            "order_table": "order_current",
            "mode": "external_catalog",
        },
        "totals": totals,
        "event_type_realtime": event_type_realtime,
        "category_realtime": category_realtime,
        "minute_realtime": minute_realtime,
        "recent_events": recent_events,
        "review_sentiment": review_sentiment,
        "order_status": order_status,
        "review_impact_summary": review_impact_summary,
        "review_risk_category": review_risk_category,
        "review_risk_products": review_risk_products,
    }
    print(JSON_PREFIX + json.dumps(payload, ensure_ascii=False))


if __name__ == "__main__":
    main()
