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


def parse_value(value: str) -> Any:
    if value in {"", "NULL", r"\N"}:
        return None
    try:
        if "." in value:
            return float(Decimal(value))
        return int(value)
    except Exception:
        return value


def run_starrocks_query(sql: str) -> list[list[str]]:
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
        timeout=60,
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

    payload = {
        "totals": totals,
        "event_type_realtime": event_type_realtime,
        "category_realtime": category_realtime,
        "minute_realtime": minute_realtime,
        "recent_events": recent_events,
    }
    print(JSON_PREFIX + json.dumps(payload, ensure_ascii=False))


if __name__ == "__main__":
    main()
