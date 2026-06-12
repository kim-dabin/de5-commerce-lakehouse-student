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
    page_title="DE5 Olist UXLog + Review Lakehouse BI",
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
    return run_metric_command(["./scripts/query-realtime-olap-metrics.sh"], REALTIME_JSON_PREFIX, 300)


@st.cache_data(ttl=60, show_spinner=False)
def load_batch_metrics() -> dict[str, Any]:
    return run_metric_command(["./scripts/query-bi-metrics.sh"], BATCH_JSON_PREFIX, 180)


def metric_number(payload: dict[str, Any], key: str) -> int:
    return int(payload.get("totals", {}).get(key, 0) or 0)


def metric_money(payload: dict[str, Any], key: str = "revenue") -> float:
    return float(payload.get("totals", {}).get(key, 0) or 0)


def metric_rate(value: Any) -> str:
    return f"{float(value or 0):.1f}%"


def empty_commands(kind: str) -> None:
    if kind == "realtime":
        st.info("StarRocks Paimon external catalog와 Paimon table이 아직 준비되지 않았습니다. 아래 순서로 실행한 뒤 새로고침하세요.")
        commands = [
            "./scripts/reset-olist-kafka-topics.sh",
            "./scripts/produce-olist-ux-events.sh",
            "./scripts/produce-olist-review-events.sh",
            "./scripts/produce-olist-order-events.sh",
            "./scripts/reset-olist-paimon.sh",
            "./scripts/run-flink-olist-paimon.sh",
            "./scripts/start-realtime-olap.sh",
            "./scripts/reset-realtime-olap.sh",
        ]
    else:
        st.info("Iceberg Analytics 테이블이 아직 준비되지 않았습니다. 아래 순서로 실행한 뒤 새로고침하세요.")
        commands = [
            "./scripts/reset-olist-kafka-topics.sh",
            "./scripts/produce-olist-ux-events.sh",
            "./scripts/produce-olist-review-events.sh",
            "./scripts/produce-olist-order-events.sh",
            "./scripts/reset-olist-paimon.sh",
            "./scripts/run-flink-olist-paimon.sh",
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


def scatter_chart(df: pd.DataFrame, x: str, y: str, size: str, color: str, title: str) -> alt.Chart:
    return (
        alt.Chart(df, title=title)
        .mark_circle(opacity=0.82)
        .encode(
            x=alt.X(f"{x}:Q", title=x),
            y=alt.Y(f"{y}:Q", title=y),
            size=alt.Size(f"{size}:Q", title=size, scale=alt.Scale(range=[80, 900])),
            color=alt.Color(f"{color}:N", title=None, legend=None),
            tooltip=list(df.columns),
        )
        .properties(height=340)
    )


def line_chart(df: pd.DataFrame, x: str, y: str, color: str, title: str) -> alt.Chart:
    return (
        alt.Chart(df, title=title)
        .mark_line(point=True)
        .encode(
            x=alt.X(f"{x}:T", title=None),
            y=alt.Y(f"{y}:Q", title=y),
            color=alt.Color(f"{color}:N", title=None),
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
        st.caption("StarRocks Paimon external catalog로 Paimon Bronze/current table을 직접 조회하는 OLAP 화면")

    try:
        with st.spinner("StarRocks Paimon catalog를 조회하는 중입니다..."):
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
    review_sentiment = pd.DataFrame(payload.get("review_sentiment", []))
    order_status = pd.DataFrame(payload.get("order_status", []))
    review_impact = payload.get("review_impact_summary", {})
    review_risk_category = pd.DataFrame(payload.get("review_risk_category", []))
    review_risk_products = pd.DataFrame(payload.get("review_risk_products", []))

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

    st.subheader("Paimon Current-state Tables via StarRocks")
    current_left, current_right = st.columns(2)
    with current_left:
        if not review_sentiment.empty:
            st.altair_chart(
                bar_chart(review_sentiment, "sentiment", "review_count", "sentiment", "Review Current by Sentiment"),
                use_container_width=True,
            )
    with current_right:
        if not order_status.empty:
            st.altair_chart(
                horizontal_bar_chart(order_status, "order_status", "order_count", "Order Current by Status"),
                use_container_width=True,
            )

    st.subheader("Review Impact · 리뷰 노출 이후 전환과 이탈")
    st.caption(
        "UXLog append fact와 review_current 상태 테이블을 product/session 기준으로 결합해 "
        "리뷰를 본 세션이 장바구니/구매로 이어지는지, 부정 리뷰 비율이 높은 상품에서 PDP 이탈이 커지는지 봅니다."
    )

    impact_cols = st.columns(4)
    impact_cols[0].metric("Review Seen Pairs", f"{int(review_impact.get('review_seen_pairs', 0) or 0):,}")
    impact_cols[1].metric("Cart Click After Review", metric_rate(review_impact.get("cart_click_after_review_rate")))
    impact_cols[2].metric("Purchase After Review", metric_rate(review_impact.get("purchase_after_review_rate")))
    impact_cols[3].metric("PDP Exit Rate", metric_rate(review_impact.get("pdp_exit_rate")))

    impact_left, impact_right = st.columns([0.42, 0.58])
    with impact_left:
        review_funnel = pd.DataFrame(
            [
                {"stage": "review_seen", "session_product_pairs": review_impact.get("review_seen_pairs", 0) or 0},
                {
                    "stage": "cart_click_after_review",
                    "session_product_pairs": review_impact.get("cart_click_after_review_pairs", 0) or 0,
                },
                {
                    "stage": "purchase_after_review",
                    "session_product_pairs": review_impact.get("purchase_after_review_pairs", 0) or 0,
                },
            ]
        )
        st.altair_chart(
            horizontal_bar_chart(
                review_funnel,
                "stage",
                "session_product_pairs",
                "Review Seen -> Cart/Purchase",
            ),
            use_container_width=True,
        )
    with impact_right:
        if not review_risk_category.empty:
            st.altair_chart(
                scatter_chart(
                    review_risk_category,
                    "negative_review_ratio",
                    "pdp_exit_rate",
                    "product_view_sessions",
                    "category_code",
                    "Negative Review Ratio vs PDP Exit by Category",
                ),
                use_container_width=True,
            )

    if not review_risk_products.empty:
        st.dataframe(
            review_risk_products[
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
                    "purchase_rate",
                ]
            ],
            use_container_width=True,
            hide_index=True,
        )

    st.subheader("Recent Events")
    st.dataframe(recent, use_container_width=True, hide_index=True)


def batch_tab() -> None:
    button_col, note_col = st.columns([0.18, 0.82], vertical_alignment="center")
    with button_col:
        if st.button("Batch 새로고침", type="primary", use_container_width=True):
            load_batch_metrics.clear()
    with note_col:
        st.caption("Spark가 만든 Iceberg Analytics table을 StarRocks Iceberg external catalog로 조회하는 기준 BI 화면")

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

    review_totals = payload.get("review_totals", {})
    order_status_current = pd.DataFrame(payload.get("order_status_current", []))
    review_impact = payload.get("review_impact_summary", {})
    review_cols = st.columns(4)
    review_cols[0].metric("Reviews", f"{int(review_totals.get('reviews', 0) or 0):,}")
    review_cols[1].metric("Reviewed Products", f"{int(review_totals.get('reviewed_products', 0) or 0):,}")
    review_cols[2].metric("Avg Rating", f"{float(review_totals.get('avg_rating', 0) or 0):.2f}")
    review_cols[3].metric("Negative Reviews", f"{int(review_totals.get('negative_reviews', 0) or 0):,}")

    event_daily = pd.DataFrame(payload.get("event_type_daily", []))
    category_daily = pd.DataFrame(payload.get("category_daily", []))
    top_categories = pd.DataFrame(payload.get("top_categories", []))
    top_brands = pd.DataFrame(payload.get("top_brands", []))
    funnel_daily = pd.DataFrame(payload.get("funnel_daily", []))
    top_negative_reviews = pd.DataFrame(payload.get("top_negative_review_categories", []))
    review_risk_category = pd.DataFrame(payload.get("review_risk_category", []))
    review_risk_products = pd.DataFrame(payload.get("review_risk_products", []))

    if event_daily.empty:
        empty_commands("batch")
        return

    st.subheader("Batch Decision KPIs")
    st.caption(
        "Spark가 Paimon Bronze/current table을 정리해 Iceberg에 남기고, "
        "StarRocks가 Iceberg external catalog로 조회한 기준 결과입니다. "
        "운영 화면의 빠른 숫자가 아니라 다음날 회고와 의사결정에 쓰는 확정 집계로 해석합니다."
    )
    decision_cols = st.columns(4)
    decision_cols[0].metric("View -> Cart", metric_rate(review_impact.get("view_to_cart_rate")))
    decision_cols[1].metric("View -> Purchase", metric_rate(review_impact.get("view_to_purchase_rate")))
    decision_cols[2].metric("Review Seen -> Purchase", metric_rate(review_impact.get("purchase_after_review_rate")))
    decision_cols[3].metric("PDP Exit", metric_rate(review_impact.get("pdp_exit_rate")))

    if not funnel_daily.empty:
        funnel_daily["event_date"] = pd.to_datetime(funnel_daily["event_date"])
        funnel_daily["view_to_purchase_rate"] = (
            funnel_daily["purchase_sessions"] * 100 / funnel_daily["product_view_sessions"].replace(0, pd.NA)
        ).fillna(0)
        funnel_daily["view_to_cart_rate"] = (
            funnel_daily["add_to_cart_sessions"] * 100 / funnel_daily["product_view_sessions"].replace(0, pd.NA)
        ).fillna(0)
        funnel_daily["review_expand_rate"] = (
            funnel_daily["review_expand_sessions"] * 100 / funnel_daily["review_impression_sessions"].replace(0, pd.NA)
        ).fillna(0)

    trend_col, funnel_col = st.columns([0.58, 0.42])
    with trend_col:
        if not funnel_daily.empty:
            trend = funnel_daily[
                ["event_date", "revenue", "view_to_purchase_rate", "view_to_cart_rate", "review_expand_rate"]
            ].melt("event_date", var_name="metric", value_name="value")
            st.altair_chart(
                line_chart(trend, "event_date", "value", "metric", "Daily Revenue and Conversion Trend"),
                use_container_width=True,
            )
    with funnel_col:
        if not funnel_daily.empty:
            funnel_columns = [
                "product_view_sessions",
                "review_impression_sessions",
                "review_expand_sessions",
                "add_to_cart_sessions",
                "purchase_sessions",
            ]
            funnel_melted = funnel_daily[funnel_columns].sum().reset_index()
            funnel_melted.columns = ["stage", "sessions"]
            st.altair_chart(
                horizontal_bar_chart(funnel_melted, "stage", "sessions", "Batch Funnel Sessions"),
                use_container_width=True,
            )

    st.subheader("Trusted Review Impact")
    st.caption(
        "같은 UXLog와 review_current를 batch 기준으로 다시 계산한 결과입니다. "
        "실시간 화면에서 본 지표가 하루 결산 기준으로도 설명 가능한지 확인합니다."
    )
    impact_left, impact_right = st.columns([0.42, 0.58])
    with impact_left:
        review_funnel = pd.DataFrame(
            [
                {"stage": "review_seen", "session_product_pairs": review_impact.get("review_seen_pairs", 0) or 0},
                {
                    "stage": "cart_after_review",
                    "session_product_pairs": review_impact.get("cart_after_review_pairs", 0) or 0,
                },
                {
                    "stage": "purchase_after_review",
                    "session_product_pairs": review_impact.get("purchase_after_review_pairs", 0) or 0,
                },
            ]
        )
        st.altair_chart(
            horizontal_bar_chart(review_funnel, "stage", "session_product_pairs", "Review Seen -> Cart/Purchase"),
            use_container_width=True,
        )
    with impact_right:
        if not review_risk_category.empty:
            st.altair_chart(
                scatter_chart(
                    review_risk_category,
                    "negative_review_ratio",
                    "pdp_exit_rate",
                    "revenue",
                    "category_code",
                    "Category Risk Map: Negative Reviews vs PDP Exit",
                ),
                use_container_width=True,
            )

    st.subheader("Category Business Health")
    health_left, health_right = st.columns([0.5, 0.5])
    with health_left:
        if not review_risk_category.empty:
            st.altair_chart(
                horizontal_bar_chart(
                    review_risk_category.head(10),
                    "category_code",
                    "revenue",
                    "Revenue by Category with Review Coverage",
                ),
                use_container_width=True,
            )
    with health_right:
        if not top_negative_reviews.empty:
            negative_df = top_negative_reviews.copy()
            negative_df["negative_review_ratio"] = negative_df["negative_review_ratio"] * 100
            st.altair_chart(
                horizontal_bar_chart(
                    negative_df,
                    "category_code",
                    "negative_review_ratio",
                    "Negative Review Ratio by Category",
                ),
                use_container_width=True,
            )

    if not review_risk_products.empty:
        st.subheader("Products to Inspect")
        st.caption("부정 리뷰 비율과 PDP 이탈률이 함께 높은 상품 후보입니다. 실무에서는 여기서 상품 상세, 리뷰 품질, 재고/배송 이슈를 같이 확인합니다.")
        st.dataframe(
            review_risk_products[
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
                    "purchase_rate",
                    "revenue",
                ]
            ],
            use_container_width=True,
            hide_index=True,
        )

    with st.expander("검증용 원천 집계 보기"):
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

    if not order_status_current.empty:
        st.subheader("Current Order State")
        st.dataframe(order_status_current, use_container_width=True, hide_index=True)

    st.subheader("Analytics Table")
    st.dataframe(
        category_daily[
            [
                "event_date",
                "category_code",
                "product_view_count",
                "review_impression_count",
                "review_expand_count",
                "add_to_cart_count",
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

st.title("DE5 Olist UXLog + Review Lakehouse BI")
st.caption("Olist 기반 UXLog는 append fact로, 리뷰/주문은 current-state로 관리하고 StarRocks는 Paimon external catalog로 Lakehouse 데이터를 조회합니다.")

realtime, batch = st.tabs(["Lakehouse Ops · StarRocks(Paimon)", "Daily Business · Iceberg"])
with realtime:
    realtime_tab()
with batch:
    batch_tab()
