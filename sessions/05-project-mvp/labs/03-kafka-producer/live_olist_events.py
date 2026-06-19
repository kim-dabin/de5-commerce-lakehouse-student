#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import signal
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from confluent_kafka import Producer


STOP_REQUESTED = False


@dataclass(frozen=True)
class TopicSource:
    name: str
    topic: str
    input_path: Path
    key_field: str
    events: list[dict[str, Any]]


def request_stop(_signum: int, _frame: Any) -> None:
    global STOP_REQUESTED
    STOP_REQUESTED = True


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Continuously replay Olist UX, review, and order events into Kafka."
    )
    parser.add_argument(
        "--bootstrap-server",
        default=os.getenv("KAFKA_BOOTSTRAP_SERVER", "kafka:19092"),
        help="Kafka bootstrap server inside Docker network.",
    )
    parser.add_argument(
        "--ux-input",
        default=os.getenv("OLIST_UX_INPUT", "/workspace/data/sample/olist/ux_events.jsonl"),
        help="UX event JSONL path.",
    )
    parser.add_argument(
        "--review-input",
        default=os.getenv("OLIST_REVIEW_INPUT", "/workspace/data/sample/olist/review_events.jsonl"),
        help="Review event JSONL path.",
    )
    parser.add_argument(
        "--order-input",
        default=os.getenv("OLIST_ORDER_INPUT", "/workspace/data/sample/olist/order_status_events.jsonl"),
        help="Order status event JSONL path.",
    )
    parser.add_argument(
        "--rate-per-second",
        type=float,
        default=float(os.getenv("LIVE_OLIST_RATE_PER_SECOND", "6")),
        help="Total send rate across all topics. 0 means send as fast as possible.",
    )
    parser.add_argument(
        "--max-events",
        type=int,
        default=int(os.getenv("LIVE_OLIST_MAX_EVENTS", "0")),
        help="Total events to send across all topics. 0 means run until stopped.",
    )
    parser.add_argument(
        "--progress-interval",
        type=int,
        default=int(os.getenv("LIVE_OLIST_PROGRESS_INTERVAL", "30")),
        help="Print progress every N delivered events. 0 disables progress logs.",
    )
    return parser.parse_args()


def get_field(event: dict[str, Any], field_path: str) -> Any:
    current: Any = event
    for part in field_path.split("."):
        if not isinstance(current, dict) or part not in current:
            return None
        current = current[part]
    return current


def load_events(path: Path) -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as source:
        for line_number, raw_line in enumerate(source, start=1):
            line = raw_line.strip()
            if not line:
                continue
            try:
                event = json.loads(line)
            except json.JSONDecodeError as exc:
                raise ValueError(f"{path}:{line_number}: invalid JSON: {exc}") from exc
            if not isinstance(event, dict):
                raise ValueError(f"{path}:{line_number}: JSON value must be an object")
            events.append(event)
    return events


def make_sources(args: argparse.Namespace) -> list[TopicSource]:
    config = [
        ("ux", "ux-events", Path(args.ux_input), "session_id"),
        ("review", "review-events", Path(args.review_input), "review_id"),
        ("order", "order-status-events", Path(args.order_input), "order_id"),
    ]
    sources: list[TopicSource] = []
    for name, topic, input_path, key_field in config:
        events = load_events(input_path)
        if not events:
            raise ValueError(f"no events found for {name}: {input_path}")
        sources.append(TopicSource(name, topic, input_path, key_field, events))
    return sources


def delivery_report(err: Any, msg: Any) -> None:
    if err is not None:
        print(f"delivery failed topic={msg.topic() if msg else '?'}: {err}", file=sys.stderr)


def run(args: argparse.Namespace) -> int:
    if args.rate_per_second < 0:
        print("--rate-per-second must be >= 0", file=sys.stderr)
        return 1
    if args.max_events < 0:
        print("--max-events must be >= 0", file=sys.stderr)
        return 1

    signal.signal(signal.SIGTERM, request_stop)
    signal.signal(signal.SIGINT, request_stop)

    sources = make_sources(args)
    producer = Producer(
        {
            "bootstrap.servers": args.bootstrap_server,
            "client.id": "de5-live-olist-events-producer",
            "acks": "all",
            "enable.idempotence": True,
            "retries": 5,
            "compression.type": "snappy",
        }
    )

    print("live olist producer started")
    print(f"bootstrap={args.bootstrap_server}")
    print(f"rate={args.rate_per_second}/sec max_events={args.max_events or 'infinite'}")
    for source in sources:
        print(f"source={source.name} topic={source.topic} rows={len(source.events)} input={source.input_path}")

    indexes = {source.name: 0 for source in sources}
    counts = {source.topic: 0 for source in sources}
    sent_total = 0
    delay_seconds = 1 / args.rate_per_second if args.rate_per_second > 0 else 0

    while not STOP_REQUESTED:
        for source in sources:
            if STOP_REQUESTED:
                break
            if args.max_events and sent_total >= args.max_events:
                break

            idx = indexes[source.name]
            event = source.events[idx]
            indexes[source.name] = (idx + 1) % len(source.events)

            key_value = get_field(event, source.key_field)
            key = str(key_value) if key_value is not None else None
            value = json.dumps(event, ensure_ascii=False, separators=(",", ":"))

            while True:
                try:
                    producer.produce(
                        source.topic,
                        key=key,
                        value=value.encode("utf-8"),
                        callback=delivery_report,
                    )
                    break
                except BufferError:
                    producer.poll(1)
            producer.poll(0)

            sent_total += 1
            counts[source.topic] += 1

            if args.progress_interval and sent_total % args.progress_interval == 0:
                summary = " ".join(f"{topic}={count}" for topic, count in counts.items())
                print(f"sent_total={sent_total} {summary}", flush=True)

            if delay_seconds:
                time.sleep(delay_seconds)

        if args.max_events and sent_total >= args.max_events:
            break

    remaining = producer.flush(30)
    if remaining:
        print(f"producer stopped with {remaining} message(s) not flushed", file=sys.stderr)
        return 1

    summary = " ".join(f"{topic}={count}" for topic, count in counts.items())
    print(f"live olist producer stopped sent_total={sent_total} {summary}")
    return 0


def main() -> int:
    try:
        return run(parse_args())
    except Exception as exc:
        print(f"live olist producer failed: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
