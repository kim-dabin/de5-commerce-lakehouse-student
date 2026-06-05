#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import sys
import time
from pathlib import Path
from typing import Any

from confluent_kafka import Producer


class ProduceStats:
    def __init__(self, quiet: bool = False) -> None:
        self.delivered = 0
        self.failed = 0
        self.quiet = quiet

    def delivery_report(self, err: Any, msg: Any) -> None:
        if err is not None:
            self.failed += 1
            print(f"delivery failed: {err}", file=sys.stderr)
            return

        self.delivered += 1
        if self.quiet:
            return
        key = msg.key().decode("utf-8") if msg.key() else ""
        print(
            f"delivered topic={msg.topic()} partition={msg.partition()} "
            f"offset={msg.offset()} key={key}"
        )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Produce JSONL commerce events into a Kafka topic."
    )
    parser.add_argument(
        "--bootstrap-server",
        default=os.getenv("KAFKA_BOOTSTRAP_SERVER", "localhost:9092"),
        help="Kafka bootstrap server. Use localhost:9092 on host, kafka:19092 in Docker.",
    )
    parser.add_argument(
        "--topic",
        default=os.getenv("KAFKA_TOPIC", "commerce-events"),
        help="Kafka topic name.",
    )
    parser.add_argument(
        "--input",
        default=os.getenv("KAFKA_INPUT", "data/sample/commerce_events_sample.jsonl"),
        help="Input JSONL file path.",
    )
    parser.add_argument(
        "--key-field",
        default=os.getenv("KAFKA_KEY_FIELD", "user_session"),
        help="Dot-path field used as Kafka message key. Use empty string for no key.",
    )
    parser.add_argument(
        "--repeat",
        type=int,
        default=1,
        help="Number of times to send the input file.",
    )
    parser.add_argument(
        "--rate-per-second",
        type=float,
        default=0,
        help="Maximum send rate. 0 means send as fast as possible.",
    )
    parser.add_argument(
        "--max-events",
        type=int,
        default=0,
        help="Maximum number of events to send. 0 means no limit.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Parse and print events without producing to Kafka.",
    )
    parser.add_argument(
        "--quiet",
        action="store_true",
        help="Suppress per-message delivery logs and print only the final summary.",
    )
    return parser.parse_args()


def get_field(event: dict[str, Any], field_path: str) -> Any:
    if not field_path:
        return None

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


def produce(args: argparse.Namespace) -> int:
    input_path = Path(args.input)
    events = load_events(input_path)
    if not events:
        print(f"no events found in {input_path}", file=sys.stderr)
        return 1

    producer = None
    stats = ProduceStats(quiet=args.quiet)
    if not args.dry_run:
        producer = Producer(
            {
                "bootstrap.servers": args.bootstrap_server,
                "client.id": "de5-commerce-events-producer",
                "acks": "all",
                "enable.idempotence": True,
                "retries": 5,
                "compression.type": "snappy",
            }
        )

    sent = 0
    delay_seconds = 1 / args.rate_per_second if args.rate_per_second > 0 else 0

    for _ in range(args.repeat):
        for event in events:
            if args.max_events and sent >= args.max_events:
                break

            key_value = get_field(event, args.key_field)
            key = str(key_value) if key_value is not None else None
            value = json.dumps(event, ensure_ascii=False, separators=(",", ":"))

            if args.dry_run:
                print(f"dry-run key={key or ''} value={value}")
            else:
                assert producer is not None
                while True:
                    try:
                        producer.produce(
                            args.topic,
                            key=key,
                            value=value.encode("utf-8"),
                            callback=stats.delivery_report,
                        )
                        break
                    except BufferError:
                        producer.poll(1)
                producer.poll(0)

            sent += 1
            if delay_seconds:
                time.sleep(delay_seconds)

        if args.max_events and sent >= args.max_events:
            break

    if producer is not None:
        remaining = producer.flush(30)
        if remaining:
            print(f"producer failed: {remaining} message(s) not flushed", file=sys.stderr)
            return 1
        if stats.failed:
            print(f"producer failed: {stats.failed} delivery failure(s)", file=sys.stderr)
            return 1

    print(f"sent={sent} topic={args.topic} input={input_path}")
    return 0


def main() -> int:
    args = parse_args()
    if args.repeat < 1:
        print("--repeat must be >= 1", file=sys.stderr)
        return 1
    if args.rate_per_second < 0:
        print("--rate-per-second must be >= 0", file=sys.stderr)
        return 1
    if args.max_events < 0:
        print("--max-events must be >= 0", file=sys.stderr)
        return 1

    try:
        return produce(args)
    except KeyboardInterrupt:
        print("interrupted", file=sys.stderr)
        return 130
    except Exception as exc:
        print(f"producer failed: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
