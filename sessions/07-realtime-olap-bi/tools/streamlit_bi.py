#!/usr/bin/env python3
from __future__ import annotations

import json
import subprocess
from pathlib import Path
from typing import Any

import altair as alt
import pandas as pd
import streamlit as st


ROOT = Path(__file__).resolve().parents[1]
BATCH_JSON_PREFIX = "BI_METRICS_JSON="
REALTIME_JSON_PREFIX = "REALTIME_OLAP_JSON="


st.set_page_config(
    page_title="DE5 Commerce Realtime OLAP + Batch Lakehouse BI",
    page_icon="🛒",
    layout="wide",
)


def run_metric_command(command: list[str], prefix: str, timeout: int = 180) -> dict[str, Any]:
    result = subprocess.run(
        command,
        cwd=ROOT,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        timeout=timeout,
        check=False,
    )
    if result.returncode != 0:
        raise RuntimeError(result.stdout[-8000:])

    payload_line = ""
    for line in result.stdout.splitlines():
        if line.startswith(prefix):
            payload_line = line

    if not payload_line:
        raise RuntimeError(f"{prefix.rstrip('=')} payload was not found in command output.")

    return json.loads(payload_line.removeprefix(prefix))


@st.cache_data(ttl=30, show_spinner=False)
def load_realtime_metrics() -> dict[str, Any]:
    return run_metric_command(["./scripts/query-realtime-olap-metrics.sh"], REALTIME_JSON_PREFIX, 90)


@st.cache_data(ttl=60, show_spinner=False)
def load_batch_metrics() -> dict[str, Any]:
    return run_metric_command(["./scripts/query-bi-metrics.sh"], BATCH_JSON_PREFIX, 180)


def metric_number(payload: dict[str, Any], key: str) -> int:
    return int(payload.get("totals", {}).get(key, 0) or 0)


def metric_money(payload: dict[str, Any], key: str = "revenue") -> float:
    return float(payload.get("totals", {}).get(key, 0) or 0)


def empty_commands(kind: str) -> None:
    if kind == "realtime":
        st.info("StarRocks Realtime OLAP 테이블이 아직 준비되지 않았습니다. 아래 순서로 실행한 뒤 새로고침하세요.")
        commands = [
            "./scripts/reset-kafka-topic.sh",
            "./scripts/produce-kafka.sh",
            "./scripts/start-realtime-olap.sh",
            "./scripts/reset-realtime-olap.sh",
            "./scripts/load-realtime-olap-from-kafka.sh",
        ]
    else:
        st.info("Iceberg Analytics 테이블이 아직 준비되지 않았습니다. 아래 순서로 실행한 뒤 새로고침하세요.")
        commands = [
            "./scripts/reset-kafka-topic.sh",
            "./scripts/produce-kafka.sh",
            "./scripts/reset-paimon-bronze.sh",
            "./scripts/run-flink-paimon-bronze.sh",
            "./scripts/reset-iceberg-tables.sh",
            "./scripts/run-spark-iceberg-transform.sh",
        ]
    st.code("\n".join(commands), language="bash")


def metric_strip(payload: dict[str, Any]) -> None:
    cols = st.columns(4)
    cols[0].metric("Events", f"{metric_number(payload, 'total_events'):,}")
    cols[1].metric("Users", f"{metric_number(payload, 'users'):,}")
    cols[2].metric("Sessions", f"{metric_number(payload, 'sessions'):,}")
    cols[3].metric("Revenue", f"{metric_money(payload):,.0f}")


def bar_chart(df: pd.DataFrame, x: str, y: str, color: str, title: str) -> alt.Chart:
    return (
        alt.Chart(df, title=title)
        .mark_bar(cornerRadiusTopLeft=4, cornerRadiusTopRight=4)
        .encode(
            x=alt.X(f"{x}:N", title=None, sort="-y", axis=alt.Axis(labelAngle=-25)),
            y=alt.Y(f"{y}:Q", title="events"),
            color=alt.Color(f"{color}:N", title=None, legend=None),
            tooltip=list(df.columns),
        )
        .properties(height=320)
    )


def horizontal_bar_chart(df: pd.DataFrame, label: str, value: str, title: str) -> alt.Chart:
    return (
        alt.Chart(df, title=title)
        .mark_bar(cornerRadiusTopRight=4, cornerRadiusBottomRight=4)
        .encode(
            x=alt.X(f"{value}:Q", title=value),
            y=alt.Y(f"{label}:N", title=None, sort="-x"),
            color=alt.Color(f"{label}:N", title=None, legend=None),
            tooltip=list(df.columns),
        )
        .properties(height=320)
    )


def realtime_tab() -> None:
    button_col, note_col = st.columns([0.18, 0.82], vertical_alignment="center")
    with button_col:
        if st.button("Realtime 새로고침", type="primary", use_container_width=True):
            load_realtime_metrics.clear()
    with note_col:
        st.caption("Kafka 이벤트를 StarRocks에 바로 적재해 현재 운영 상태를 빠르게 보는 화면")

    try:
        with st.spinner("StarRocks Realtime OLAP를 조회하는 중입니다..."):
            payload = load_realtime_metrics()
    except Exception as exc:
        empty_commands("realtime")
        with st.expander("오류 로그 보기"):
            st.code(str(exc), language="text")
        return

    metric_strip(payload)
    st.caption(
        f"Realtime window: {payload.get('totals', {}).get('first_event_at')} "
        f"to {payload.get('totals', {}).get('last_event_at')}"
    )

    event_type = pd.DataFrame(payload.get("event_type_realtime", []))
    category = pd.DataFrame(payload.get("category_realtime", []))
    minute = pd.DataFrame(payload.get("minute_realtime", []))
    recent = pd.DataFrame(payload.get("recent_events", []))

    left, right = st.columns([0.58, 0.42])
    with left:
        if not minute.empty:
            minute["event_minute"] = pd.to_datetime(minute["event_minute"])
            st.altair_chart(
                alt.Chart(minute, title="Minute-level Event Flow")
                .mark_line(point=True)
                .encode(
                    x=alt.X("event_minute:T", title=None),
                    y=alt.Y("event_count:Q", title="events"),
                    color=alt.Color("event_type:N", title="event type"),
                    tooltip=list(minute.columns),
                )
                .properties(height=320),
                use_container_width=True,
            )
        elif not event_type.empty:
            st.altair_chart(
                bar_chart(event_type, "event_type", "event_count", "event_type", "Realtime Events by Type"),
                use_container_width=True,
            )

    with right:
        if not category.empty:
            st.altair_chart(
                horizontal_bar_chart(category.head(10), "category_code", "revenue", "Realtime Revenue by Category"),
                use_container_width=True,
            )

    st.subheader("Recent Events")
    st.dataframe(recent, use_container_width=True, hide_index=True)


def batch_tab() -> None:
    button_col, note_col = st.columns([0.18, 0.82], vertical_alignment="center")
    with button_col:
        if st.button("Batch 새로고침", type="primary", use_container_width=True):
            load_batch_metrics.clear()
    with note_col:
        st.caption("Paimon Bronze를 Spark로 정제/집계해 Iceberg Analytics에 만든 기준 BI 화면")

    try:
        with st.spinner("Iceberg Analytics 테이블을 조회하는 중입니다..."):
            payload = load_batch_metrics()
    except Exception as exc:
        empty_commands("batch")
        with st.expander("오류 로그 보기"):
            st.code(str(exc), language="text")
        return

    metric_strip(payload)
    st.caption(
        f"Analytics window: {payload.get('totals', {}).get('first_event_date')} "
        f"to {payload.get('totals', {}).get('last_event_date')}"
    )

    event_daily = pd.DataFrame(payload.get("event_type_daily", []))
    category_daily = pd.DataFrame(payload.get("category_daily", []))
    top_categories = pd.DataFrame(payload.get("top_categories", []))
    top_brands = pd.DataFrame(payload.get("top_brands", []))

    if event_daily.empty:
        empty_commands("batch")
        return

    left, right = st.columns([0.58, 0.42])
    with left:
        st.altair_chart(
            bar_chart(event_daily, "event_type", "event_count", "event_type", "Daily Events by Type"),
            use_container_width=True,
        )

    with right:
        category_tab, brand_tab = st.tabs(["Categories", "Brands"])
        with category_tab:
            st.altair_chart(
                horizontal_bar_chart(top_categories, "category_code", "revenue", "Batch Revenue by Category"),
                use_container_width=True,
            )
        with brand_tab:
            st.altair_chart(
                horizontal_bar_chart(top_brands, "brand", "revenue", "Batch Revenue by Brand"),
                use_container_width=True,
            )

    st.subheader("Analytics Table")
    st.dataframe(
        category_daily[
            [
                "event_date",
                "category_code",
                "view_count",
                "cart_count",
                "purchase_count",
                "revenue",
            ]
        ],
        use_container_width=True,
        hide_index=True,
    )


st.markdown(
    """
    <style>
      .block-container { padding-top: 2rem; padding-bottom: 3rem; }
      div[data-testid="stMetric"] {
        background: #ffffff;
        border: 1px solid #e2e8f0;
        border-radius: 8px;
        padding: 14px 16px;
        box-shadow: 0 1px 2px rgba(15, 23, 42, 0.06);
      }
      div[data-testid="stMetricLabel"],
      div[data-testid="stMetricLabel"] p {
        color: #475569;
        font-weight: 700;
      }
      div[data-testid="stMetricValue"],
      div[data-testid="stMetricValue"] div {
        color: #0f172a;
      }
    </style>
    """,
    unsafe_allow_html=True,
)

st.title("DE5 Commerce Realtime OLAP + Batch Lakehouse BI")
st.caption("Realtime Ops는 StarRocks, Daily Business는 Iceberg로 나누어 같은 이벤트의 두 가지 사용 방식을 보여줍니다.")

realtime, batch = st.tabs(["Realtime Ops · StarRocks", "Daily Business · Iceberg"])
with realtime:
    realtime_tab()
with batch:
    batch_tab()
