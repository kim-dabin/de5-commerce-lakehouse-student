#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import random
from datetime import datetime, timedelta, timezone
from pathlib import Path


PRODUCTS = [
    (1001001, 21000001, "electronics.keyboard", "logitech", 79.00),
    (1001002, 21000002, "electronics.audio.headphone", "sony", 129.00),
    (1001003, 21000003, "electronics.smartphone", "samsung", 699.00),
    (2001001, 22000001, "apparel.hoodie", "musinsa", 59.00),
    (2001002, 22000002, "apparel.shoes.running", "nike", 119.00),
    (3001001, 23000001, "home.kitchen.coffee", "hario", 42.00),
    (3001002, 23000002, "home.office.organizer", "monami", 18.00),
    (4001001, 24000001, "books.data_engineering", "oreilly", 36.00),
    (4001002, 24000002, "books.sql", "hanbit", 28.00),
    (5001001, 25000001, "grocery.snack.protein_bar", "quest", 24.00),
    (5001002, 25000002, "grocery.drink.sparkling_water", "trevi", 19.00),
]

EVENT_TYPES = [
    ("view", 64),
    ("cart", 18),
    ("remove_from_cart", 7),
    ("purchase", 11),
]


def weighted_choice(rng: random.Random) -> str:
    total = sum(weight for _, weight in EVENT_TYPES)
    point = rng.uniform(0, total)
    upto = 0.0
    for event_type, weight in EVENT_TYPES:
        upto += weight
        if upto >= point:
            return event_type
    return EVENT_TYPES[-1][0]


def make_event(rng: random.Random, index: int, base_time: datetime) -> dict:
    event_type = weighted_choice(rng)
    product_id, category_id, category_code, brand, price = rng.choice(PRODUCTS)
    occurred_at = base_time + timedelta(seconds=index * rng.randint(20, 180))
    user_id = rng.randint(1_000_000, 1_000_080)
    session_id = f"{rng.getrandbits(64):016x}-{rng.getrandbits(32):08x}"

    return {
        "id": f"evt-202605-{index:06d}",
        "event_time": occurred_at.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "event_type": event_type,
        "product_id": product_id,
        "category_id": category_id,
        "category_code": category_code,
        "brand": brand,
        "price": price,
        "user_id": user_id,
        "user_session": session_id,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate deterministic commerce event JSONL.")
    parser.add_argument("--output", default="data/sample/commerce_events_sample.jsonl")
    parser.add_argument("--count", type=int, default=240)
    parser.add_argument("--seed", type=int, default=20260529)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    rng = random.Random(args.seed)
    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    base_time = datetime(2026, 5, 1, 0, 0, 0, tzinfo=timezone.utc)

    with output.open("w", encoding="utf-8") as file:
        for index in range(1, args.count + 1):
            event = make_event(rng, index, base_time)
            file.write(json.dumps(event, ensure_ascii=False, separators=(",", ":")) + "\n")

    print(f"wrote={args.count} output={output}")


if __name__ == "__main__":
    main()
