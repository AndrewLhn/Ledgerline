"""Generate synthetic order/payment/refund load for benchmarking.

    python3 scripts/loadgen.py --orders 5000

Each order emits one order_created and one payment_captured with the same
amount, and every tenth order also a partial refund, so the daily
reconciliation keeps passing while volume grows. Events validate against the
contract and are keyed by event_id before publishing.
"""

import argparse
import json
import os
import random
import sys
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from uuid import uuid4

from confluent_kafka import Producer

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT / "dags") not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT / "dags"))

from ledgerline_contract import validate_event  # noqa: E402

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "ledgerline_events")
CHANNELS = ("web", "store", "marketplace")


def build_chain(index: int, occurred_at: str, rng: random.Random) -> list[dict[str, Any]]:
    order_id = f"ord_load_{index:08d}"
    payment_id = f"pay_load_{index:08d}"
    amount_minor = rng.randint(500, 50_000)

    events: list[dict[str, Any]] = [
        {
            "event_id": str(uuid4()),
            "event_type": "order_created",
            "schema_version": 1,
            "occurred_at": occurred_at,
            "source_system": "storefront",
            "correlation_id": order_id,
            "data": {
                "order_id": order_id,
                "customer_id": f"cust_{rng.randint(1, 500)}",
                "channel": CHANNELS[index % len(CHANNELS)],
                "currency": "EUR",
                "amount_minor": amount_minor,
            },
        },
        {
            "event_id": str(uuid4()),
            "event_type": "payment_captured",
            "schema_version": 1,
            "occurred_at": occurred_at,
            "source_system": "payment_gateway",
            "correlation_id": order_id,
            "data": {
                "payment_id": payment_id,
                "order_id": order_id,
                "currency": "EUR",
                "amount_minor": amount_minor,
            },
        },
    ]

    if index % 10 == 0:
        events.append(
            {
                "event_id": str(uuid4()),
                "event_type": "refund_issued",
                "schema_version": 1,
                "occurred_at": occurred_at,
                "source_system": "payment_gateway",
                "correlation_id": order_id,
                "data": {
                    "refund_id": f"ref_load_{index:08d}",
                    "payment_id": payment_id,
                    "order_id": order_id,
                    "currency": "EUR",
                    "amount_minor": amount_minor // 4,
                },
            }
        )

    return events


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Publish synthetic Ledgerline load.")
    parser.add_argument("--orders", type=int, default=5000, help="Order chains to publish.")
    parser.add_argument("--seed", type=int, default=7, help="RNG seed for reproducibility.")
    args = parser.parse_args(argv)

    rng = random.Random(args.seed)
    occurred_at = datetime.now(UTC).isoformat()
    producer = Producer({"bootstrap.servers": BOOTSTRAP_SERVERS})

    published = 0
    started = time.monotonic()
    for index in range(args.orders):
        for event in build_chain(index, occurred_at, rng):
            validated = validate_event(event)
            producer.produce(
                TOPIC,
                json.dumps(validated, separators=(",", ":")).encode("utf-8"),
                key=validated["event_id"].encode("utf-8"),
            )
            published += 1
        if index % 500 == 0:
            producer.poll(0)

    pending = producer.flush(30)
    if pending:
        raise RuntimeError(f"Timed out delivering {pending} message(s)")

    elapsed = time.monotonic() - started
    print(f"published {published} events in {elapsed:.1f}s ({published / elapsed:.0f}/s)")


if __name__ == "__main__":
    main()
