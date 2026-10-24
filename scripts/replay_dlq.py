"""Replay recoverable events from the DLQ back onto the main topic.

Reads the DLQ from the beginning with a dedicated consumer group and never
commits offsets, so a replay can be repeated safely. Re-ingestion is idempotent
downstream: dbt deduplicates facts by source event ID.

    python3 scripts/replay_dlq.py           # dry run, prints what would happen
    python3 scripts/replay_dlq.py --apply   # republish events that now validate

Events that still fail the contract are counted and left for manual triage
(see docs/runbooks/data-quality-incident.md).
"""

import argparse
import json
import os
import sys
from pathlib import Path

from confluent_kafka import Consumer, Producer

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT / "dags") not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT / "dags"))

from ledgerline_contract import ContractValidationError, validate_event  # noqa: E402

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "ledgerline_events")
DLQ_TOPIC = os.getenv("KAFKA_DLQ_TOPIC", f"{TOPIC}_dlq")
REPLAY_GROUP = os.getenv("KAFKA_REPLAY_GROUP", "ledgerline-dlq-replay")


def classify(record: bytes) -> tuple[str, str, bytes | None]:
    """Return (status, detail, payload) for one DLQ record.

    status is "replayable" when the inner event now passes the contract,
    otherwise "still_invalid". payload is the canonical re-encoded event.
    """

    try:
        envelope = json.loads(record.decode("utf-8"))
        event = validate_event(json.loads(envelope["value"]))
    except (
        KeyError,
        TypeError,
        UnicodeDecodeError,
        json.JSONDecodeError,
        ContractValidationError,
    ) as error:
        return "still_invalid", str(error), None

    payload = json.dumps(event, separators=(",", ":"), sort_keys=True).encode("utf-8")
    return "replayable", event["event_id"], payload


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Replay recoverable DLQ events.")
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Republish replayable events (default is a dry run).",
    )
    parser.add_argument(
        "--idle-polls",
        type=int,
        default=3,
        help="Empty polls before the DLQ is considered fully read.",
    )
    args = parser.parse_args(argv)

    consumer = Consumer(
        {
            "bootstrap.servers": BOOTSTRAP_SERVERS,
            "group.id": REPLAY_GROUP,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe([DLQ_TOPIC])
    producer = Producer({"bootstrap.servers": BOOTSTRAP_SERVERS}) if args.apply else None

    counts = {"replayable": 0, "still_invalid": 0}
    idle_polls = 0
    try:
        while idle_polls < args.idle_polls:
            message = consumer.poll(1.0)
            if message is None:
                idle_polls += 1
                continue
            if message.error():
                raise RuntimeError(f"Kafka error: {message.error()}")
            idle_polls = 0

            status, detail, payload = classify(message.value())
            counts[status] += 1
            if status == "replayable":
                print(f"replayable: event_id={detail}")
                if producer:
                    producer.produce(TOPIC, payload, key=detail.encode("utf-8"))
            else:
                print(f"still invalid: {detail}")
    finally:
        consumer.close()

    if producer and producer.flush(10):
        raise RuntimeError("Timed out delivering replayed events")

    mode = "applied" if args.apply else "dry run"
    print(
        f"{mode}: replayable={counts['replayable']} "
        f"still_invalid={counts['still_invalid']}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
