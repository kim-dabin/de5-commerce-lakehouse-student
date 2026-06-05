#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import hashlib
import json
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Iterable


UTC = timezone.utc


@dataclass(frozen=True)
class ItemContext:
    order_id: str
    order_item_id: int
    source_product_id: str
    product_id: int
    catalog_id: str
    category_id: int
    category_code: str
    seller_id: str
    brand: str
    price: float
    freight_value: float


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Generate classroom UXLog append events and order/review state events "
            "from the Olist Brazilian E-Commerce public dataset."
        )
    )
    parser.add_argument("--input-dir", default="data/raw/olist")
    parser.add_argument("--output-dir", default="data/derived/olist")
    parser.add_argument(
        "--max-orders",
        type=int,
        default=0,
        help="Maximum orders to generate from. 0 means all orders.",
    )
    parser.add_argument(
        "--order-offset",
        type=int,
        default=0,
        help="Skip this many orders after sorting by purchase timestamp.",
    )
    parser.add_argument("--encoding", default="utf-8")
    return parser.parse_args()


def stable_int(value: str, modulo: int = 10**12) -> int:
    digest = hashlib.sha1(value.encode("utf-8")).hexdigest()
    return int(digest[:15], 16) % modulo


def stable_id(prefix: str, *parts: object, length: int = 20) -> str:
    raw = "|".join(str(part) for part in parts)
    digest = hashlib.sha1(raw.encode("utf-8")).hexdigest()[:length]
    return f"{prefix}-{digest}"


def parse_ts(value: str | None) -> datetime | None:
    if not value:
        return None
    value = value.strip()
    if not value:
        return None
    return datetime.strptime(value, "%Y-%m-%d %H:%M:%S").replace(tzinfo=UTC)


def format_ts(value: datetime) -> str:
    return value.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def read_csv(path: Path, encoding: str) -> list[dict[str, str]]:
    with path.open("r", newline="", encoding=encoding) as file:
        return list(csv.DictReader(file))


def write_jsonl(path: Path, rows: Iterable[dict]) -> Counter:
    counter: Counter = Counter()
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as file:
        for row in rows:
            event_type = row.get("event_type", "unknown")
            counter[event_type] += 1
            file.write(json.dumps(row, ensure_ascii=False, separators=(",", ":")) + "\n")
    return counter


def write_csv(path: Path, rows: Iterable[dict], fieldnames: list[str]) -> int:
    path.parent.mkdir(parents=True, exist_ok=True)
    count = 0
    with path.open("w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field, "") for field in fieldnames})
            count += 1
    return count


def build_category_translation(rows: list[dict[str, str]]) -> dict[str, str]:
    return {
        row["product_category_name"]: row["product_category_name_english"]
        for row in rows
        if row.get("product_category_name")
    }


def build_product_maps(
    products: list[dict[str, str]],
    translations: dict[str, str],
) -> tuple[dict[str, dict], list[dict]]:
    product_map: dict[str, dict] = {}
    product_xref: list[dict] = []

    for row in products:
        source_product_id = row["product_id"]
        source_category = row.get("product_category_name") or "unknown"
        category_code = translations.get(source_category, source_category)
        product_id = stable_int(source_product_id)
        catalog_id = f"cat-{stable_int(category_code, 10**8):08d}"
        category_id = stable_int(category_code, 10**10)
        product = {
            "source_product_id": source_product_id,
            "product_id": product_id,
            "catalog_id": catalog_id,
            "category_id": category_id,
            "category_code": category_code,
            "category_code_source": source_category,
            "product_name_length": row.get("product_name_lenght", ""),
            "product_description_length": row.get("product_description_lenght", ""),
            "product_photos_qty": row.get("product_photos_qty", ""),
            "product_weight_g": row.get("product_weight_g", ""),
        }
        product_map[source_product_id] = product
        product_xref.append(product)

    return product_map, product_xref


def build_order_items(
    order_items: list[dict[str, str]],
    product_map: dict[str, dict],
) -> dict[str, list[ItemContext]]:
    by_order: dict[str, list[ItemContext]] = defaultdict(list)

    for row in order_items:
        source_product_id = row["product_id"]
        product = product_map[source_product_id]
        seller_id = row.get("seller_id", "")
        brand = f"seller_{seller_id[:8]}" if seller_id else "unknown"
        item = ItemContext(
            order_id=row["order_id"],
            order_item_id=int(row.get("order_item_id") or 1),
            source_product_id=source_product_id,
            product_id=product["product_id"],
            catalog_id=product["catalog_id"],
            category_id=product["category_id"],
            category_code=product["category_code"],
            seller_id=seller_id,
            brand=brand,
            price=float(row.get("price") or 0),
            freight_value=float(row.get("freight_value") or 0),
        )
        by_order[item.order_id].append(item)

    for items in by_order.values():
        items.sort(key=lambda item: item.order_item_id)

    return by_order


def build_reviews_by_order(reviews: list[dict[str, str]]) -> dict[str, list[dict[str, str]]]:
    by_order: dict[str, list[dict[str, str]]] = defaultdict(list)
    for row in reviews:
        by_order[row["order_id"]].append(row)
    return by_order


def sentiment_from_score(score_text: str) -> str:
    try:
        score = int(score_text)
    except ValueError:
        return "unknown"
    if score <= 2:
        return "negative"
    if score == 3:
        return "neutral"
    return "positive"


def select_orders(orders: list[dict[str, str]], max_orders: int, offset: int) -> list[dict[str, str]]:
    sortable: list[tuple[datetime, dict[str, str]]] = []
    for row in orders:
        ts = parse_ts(row.get("order_purchase_timestamp"))
        if ts is None:
            continue
        sortable.append((ts, row))
    sortable.sort(key=lambda pair: (pair[0], pair[1]["order_id"]))
    selected = [row for _, row in sortable[offset:]]
    if max_orders:
        selected = selected[:max_orders]
    return selected


def make_base_event(
    *,
    event_id: str,
    event_time: datetime,
    event_type: str,
    customer_id: str,
    item: ItemContext | None,
    session_id: str,
    order_id: str,
) -> dict:
    product_id = item.product_id if item else None
    category_id = item.category_id if item else None
    category_code = item.category_code if item else None
    brand = item.brand if item else None
    price = item.price if item else 0.0

    return {
        "id": event_id,
        "event_id": event_id,
        "event_time": format_ts(event_time),
        "event_type": event_type,
        "user_id": stable_int(customer_id),
        "source_customer_id": customer_id,
        "user_session": session_id,
        "session_id": session_id,
        "order_id": order_id,
        "product_id": product_id,
        "source_product_id": item.source_product_id if item else None,
        "catalog_id": item.catalog_id if item else None,
        "category_id": category_id,
        "category_code": category_code,
        "brand": brand,
        "price": round(price, 2),
    }


def generate_ux_events(
    selected_orders: list[dict[str, str]],
    items_by_order: dict[str, list[ItemContext]],
    reviews_by_order: dict[str, list[dict[str, str]]],
) -> Iterable[dict]:
    for order in selected_orders:
        order_id = order["order_id"]
        customer_id = order["customer_id"]
        purchase_ts = parse_ts(order.get("order_purchase_timestamp"))
        if purchase_ts is None:
            continue
        order_status = order.get("order_status") or "unknown"
        session_id = stable_id("sess", customer_id, order_id, length=24)
        reviews = reviews_by_order.get(order_id, [])
        has_review = bool(reviews)
        is_canceled = order_status == "canceled"

        for item in items_by_order.get(order_id, []):
            item_key = f"{order_id}-{item.order_item_id}"
            timeline = [
                ("search_result_click", purchase_ts - timedelta(minutes=38, seconds=item.order_item_id)),
                ("product_view", purchase_ts - timedelta(minutes=30, seconds=item.order_item_id)),
            ]
            if has_review:
                timeline.append(("review_impression", purchase_ts - timedelta(minutes=24, seconds=item.order_item_id)))
                if item.order_item_id == 1:
                    timeline.append(("review_expand", purchase_ts - timedelta(minutes=22, seconds=item.order_item_id)))
            timeline.append(("add_to_cart", purchase_ts - timedelta(minutes=12, seconds=item.order_item_id)))
            if is_canceled:
                timeline.append(("remove_from_cart", purchase_ts - timedelta(minutes=3, seconds=item.order_item_id)))
            else:
                timeline.append(("purchase", purchase_ts + timedelta(seconds=item.order_item_id)))

            for event_type, event_time in timeline:
                event_id = stable_id("ux", item_key, event_type, event_time.isoformat())
                event = make_base_event(
                    event_id=event_id,
                    event_time=event_time,
                    event_type=event_type,
                    customer_id=customer_id,
                    item=item,
                    session_id=session_id,
                    order_id=order_id,
                )
                event["order_status_at_source"] = order_status
                event["generated_from"] = "olist_order_item"
                event["is_synthetic_ux"] = True
                yield event


def generate_order_state_events(selected_orders: list[dict[str, str]]) -> Iterable[dict]:
    for order in selected_orders:
        order_id = order["order_id"]
        customer_id = order["customer_id"]
        status = order.get("order_status") or "unknown"
        session_id = stable_id("sess", customer_id, order_id, length=24)
        steps = [
            ("order_created", parse_ts(order.get("order_purchase_timestamp"))),
            ("order_approved", parse_ts(order.get("order_approved_at"))),
            ("order_shipped", parse_ts(order.get("order_delivered_carrier_date"))),
            ("order_delivered", parse_ts(order.get("order_delivered_customer_date"))),
        ]
        if status == "canceled":
            purchase_ts = parse_ts(order.get("order_purchase_timestamp"))
            steps.append(("order_canceled", purchase_ts + timedelta(hours=1) if purchase_ts else None))

        for event_type, event_time in steps:
            if event_time is None:
                continue
            event_id = stable_id("order", order_id, event_type)
            yield {
                "event_id": event_id,
                "event_time": format_ts(event_time),
                "event_type": event_type,
                "order_id": order_id,
                "order_status": status,
                "user_id": stable_int(customer_id),
                "source_customer_id": customer_id,
                "session_id": session_id,
                "user_session": session_id,
                "generated_from": "olist_orders_dataset",
            }


def generate_review_events(
    selected_orders: list[dict[str, str]],
    items_by_order: dict[str, list[ItemContext]],
    reviews_by_order: dict[str, list[dict[str, str]]],
) -> Iterable[dict]:
    selected_order_ids = {order["order_id"] for order in selected_orders}
    order_customer = {order["order_id"]: order["customer_id"] for order in selected_orders}

    for order_id in selected_order_ids:
        items = items_by_order.get(order_id, [])
        first_item = items[0] if items else None
        customer_id = order_customer[order_id]
        for review in reviews_by_order.get(order_id, []):
            review_id = review["review_id"]
            created_at = parse_ts(review.get("review_creation_date"))
            answered_at = parse_ts(review.get("review_answer_timestamp"))
            if created_at is None:
                continue
            score = int(review.get("review_score") or 0)
            sentiment = sentiment_from_score(review.get("review_score") or "")
            base = {
                "review_id": review_id,
                "order_id": order_id,
                "user_id": stable_int(customer_id),
                "source_customer_id": customer_id,
                "product_id": first_item.product_id if first_item else None,
                "source_product_id": first_item.source_product_id if first_item else None,
                "catalog_id": first_item.catalog_id if first_item else None,
                "category_id": first_item.category_id if first_item else None,
                "category_code": first_item.category_code if first_item else None,
                "rating": score,
                "review_title": review.get("review_comment_title") or "",
                "review_text": review.get("review_comment_message") or "",
                "is_used": True,
                "matched_product_id": first_item.product_id if first_item else None,
                "is_order_level_review": True,
                "generated_from": "olist_order_reviews_dataset",
            }

            created_event_id = stable_id("review", review_id, "review_created")
            yield {
                **base,
                "review_event_id": created_event_id,
                "event_id": created_event_id,
                "event_time": format_ts(created_at),
                "event_type": "review_created",
                "sentiment": None,
                "created_at": format_ts(created_at),
                "updated_at": format_ts(created_at),
            }

            scored_at = created_at + timedelta(minutes=5)
            scored_event_id = stable_id("review", review_id, "sentiment_scored")
            yield {
                **base,
                "review_event_id": scored_event_id,
                "event_id": scored_event_id,
                "event_time": format_ts(scored_at),
                "event_type": "sentiment_scored",
                "sentiment": sentiment,
                "analysis_model_version": "score-rule-v1",
                "created_at": format_ts(created_at),
                "updated_at": format_ts(scored_at),
            }

            if answered_at is not None:
                answered_event_id = stable_id("review", review_id, "review_answered")
                yield {
                    **base,
                    "review_event_id": answered_event_id,
                    "event_id": answered_event_id,
                    "event_time": format_ts(answered_at),
                    "event_type": "review_answered",
                    "sentiment": sentiment,
                    "analysis_model_version": "score-rule-v1",
                    "created_at": format_ts(created_at),
                    "updated_at": format_ts(answered_at),
                }


def main() -> None:
    args = parse_args()
    input_dir = Path(args.input_dir)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    customers = read_csv(input_dir / "olist_customers_dataset.csv", args.encoding)
    orders = read_csv(input_dir / "olist_orders_dataset.csv", args.encoding)
    order_items = read_csv(input_dir / "olist_order_items_dataset.csv", args.encoding)
    reviews = read_csv(input_dir / "olist_order_reviews_dataset.csv", args.encoding)
    products = read_csv(input_dir / "olist_products_dataset.csv", args.encoding)
    translations = read_csv(input_dir / "product_category_name_translation.csv", args.encoding)

    translation_map = build_category_translation(translations)
    product_map, product_xref = build_product_maps(products, translation_map)
    items_by_order = build_order_items(order_items, product_map)
    reviews_by_order = build_reviews_by_order(reviews)
    selected_orders = select_orders(orders, args.max_orders, args.order_offset)

    ux_counts = write_jsonl(
        output_dir / "ux_events.jsonl",
        generate_ux_events(selected_orders, items_by_order, reviews_by_order),
    )
    order_counts = write_jsonl(
        output_dir / "order_status_events.jsonl",
        generate_order_state_events(selected_orders),
    )
    review_counts = write_jsonl(
        output_dir / "review_events.jsonl",
        generate_review_events(selected_orders, items_by_order, reviews_by_order),
    )
    xref_count = write_csv(
        output_dir / "product_xref.csv",
        product_xref,
        [
            "product_id",
            "source_product_id",
            "catalog_id",
            "category_id",
            "category_code",
            "category_code_source",
            "product_name_length",
            "product_description_length",
            "product_photos_qty",
            "product_weight_g",
        ],
    )

    summary = {
        "source": "olistbr/brazilian-ecommerce",
        "input_dir": str(input_dir),
        "output_dir": str(output_dir),
        "selected_orders": len(selected_orders),
        "source_rows": {
            "customers": len(customers),
            "orders": len(orders),
            "order_items": len(order_items),
            "order_reviews": len(reviews),
            "products": len(products),
        },
        "generated_rows": {
            "ux_events": sum(ux_counts.values()),
            "order_status_events": sum(order_counts.values()),
            "review_events": sum(review_counts.values()),
            "product_xref": xref_count,
        },
        "event_type_counts": {
            "ux_events": dict(sorted(ux_counts.items())),
            "order_status_events": dict(sorted(order_counts.items())),
            "review_events": dict(sorted(review_counts.items())),
        },
        "notes": [
            "UX events are simulated from real Olist order/order_item/product/review relationships.",
            "Order and review state events preserve real Olist keys and timestamps where available.",
            "product_id is a deterministic numeric surrogate for classroom systems; source_product_id keeps the original Olist key.",
        ],
    }
    (output_dir / "dataset_summary.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )

    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
