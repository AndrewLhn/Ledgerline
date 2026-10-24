import argparse
import json
import os
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from confluent_kafka import Producer

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT / "dags") not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT / "dags"))

from ledgerline_contract import validate_event  # noqa: E402

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "ledgerline_events")


def delivery_report(error, message) -> None:
    if error is not None:
        raise RuntimeError(f"Kafka delivery failed: {error}")
    print(
        f"Message delivered to {message.topic()} "
        f"[partition={message.partition()}, offset={message.offset()}]"
    )


def build_ledgerline_events(occurred_at: str | None = None) -> list[dict[str, Any]]:
    """Return a small, reconciled synthetic retail scenario."""

    timestamp = occurred_at or (datetime.now(UTC) - timedelta(minutes=5)).isoformat()
    order_id = "ord_1001"
    payment_id = "pay_1001"

    return [
        {
            "event_id": "11111111-1111-4111-8111-111111111111",
            "event_type": "order_created",
            "schema_version": 1,
            "occurred_at": timestamp,
            "source_system": "storefront",
            "correlation_id": order_id,
            "data": {
                "order_id": order_id,
                "customer_id": "cust_42",
                "channel": "web",
                "currency": "EUR",
                "amount_minor": 10000
            }
        },
        {
            "event_id": "22222222-2222-4222-8222-222222222222",
            "event_type": "payment_captured",
            "schema_version": 1,
            "occurred_at": timestamp,
            "source_system": "payment_gateway",
            "correlation_id": order_id,
            "data": {
                "payment_id": payment_id,
                "order_id": order_id,
                "currency": "EUR",
                "amount_minor": 10000
            }
        },
        {
            "event_id": "33333333-3333-4333-8333-333333333333",
            "event_type": "refund_issued",
            "schema_version": 1,
            "occurred_at": timestamp,
            "source_system": "payment_gateway",
            "correlation_id": order_id,
            "data": {
                "refund_id": "ref_1001",
                "payment_id": payment_id,
                "order_id": order_id,
                "currency": "EUR",
                "amount_minor": 2000
            }
        },
        {
            "event_id": "44444444-4444-4444-8444-444444444444",
            "event_type": "inventory_adjusted",
            "schema_version": 1,
            "occurred_at": timestamp,
            "source_system": "erp",
            "correlation_id": order_id,
            "data": {
                "adjustment_id": "adj_1001",
                "product_id": "sku_espresso",
                "location_id": "wh_kyiv",
                "quantity_delta": -1
            }
        }
    ]


def build_v2_payment(occurred_at: str | None = None) -> dict[str, Any]:
    """A version-2 payment: identical to v1 plus the required method field."""

    timestamp = occurred_at or (datetime.now(UTC) - timedelta(minutes=5)).isoformat()
    return {
        "event_id": "55555555-5555-4555-8555-555555555555",
        "event_type": "payment_captured",
        "schema_version": 2,
        "occurred_at": timestamp,
        "source_system": "payment_gateway",
        "correlation_id": "ord_1002",
        "data": {
            "payment_id": "pay_1002",
            "order_id": "ord_1002",
            "currency": "EUR",
            "amount_minor": 5000,
            "method": "card",
        },
    }


def build_messages(scenario: str) -> list[tuple[str | None, bytes]]:
    """Return (Kafka key, payload) pairs; the key keeps per-entity ordering."""

    events = build_ledgerline_events()

    if scenario == "ledgerline":
        return [
            (event["event_id"], json.dumps(validate_event(event)).encode("utf-8"))
            for event in events
        ]
    if scenario == "duplicate":
        event = events[1]
        return [(event["event_id"], json.dumps(validate_event(event)).encode("utf-8"))]
    if scenario == "v2":
        event = build_v2_payment()
        return [(event["event_id"], json.dumps(validate_event(event)).encode("utf-8"))]
    if scenario == "invalid":
        return [(None, b'{"event_id": "not-a-valid-ledgerline-event"}')]

    raise ValueError(f"Unsupported scenario: {scenario}")


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Publish synthetic Ledgerline events to Kafka.")
    parser.add_argument(
        "--scenario",
        choices=["ledgerline", "duplicate", "v2", "invalid"],
        default="ledgerline",
        help="Synthetic message set to publish."
    )
    args = parser.parse_args(argv)

    producer = Producer({"bootstrap.servers": BOOTSTRAP_SERVERS})
    for key, message in build_messages(args.scenario):
        producer.produce(
            TOPIC,
            message,
            key=key.encode("utf-8") if key else None,
            on_delivery=delivery_report,
        )

    pending_messages = producer.flush(10)
    if pending_messages:
        raise RuntimeError(f"Timed out delivering {pending_messages} Kafka message(s)")


if __name__ == "__main__":
    main()
