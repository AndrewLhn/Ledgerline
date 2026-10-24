import json
import logging
import os
import time
from datetime import UTC, datetime
from uuid import uuid4

import pyarrow as pa
from confluent_kafka import Consumer, Producer
from ledgerline_contract import ContractValidationError, validate_event

TABLE_IDENTIFIER = "ledgerline_raw.events"
RUNS_TABLE_IDENTIFIER = "ledgerline_raw.ingestion_runs"
TOPIC = os.getenv("KAFKA_TOPIC", "ledgerline_events")
DLQ_TOPIC = os.getenv("KAFKA_DLQ_TOPIC", f"{TOPIC}_dlq")
BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
CONSUMER_GROUP = os.getenv("KAFKA_CONSUMER_GROUP", "ledgerline-iceberg-consumer")
PUSHGATEWAY_URL = os.getenv("PUSHGATEWAY_URL", "")
LOGGER = logging.getLogger(__name__)

INGESTED_AT_FIELD_ID = 9
INGESTED_AT_PARTITION_FIELD_ID = 1000


def get_catalog():
    # Imported lazily: the host test suite runs without pyiceberg installed.
    from pyiceberg.catalog import load_catalog

    return load_catalog(
        "default",
        **{
            "type": "sql",
            "uri": os.environ["ICEBERG_CATALOG_URI"],
            "warehouse": os.environ["ICEBERG_WAREHOUSE"],
            "py-io-impl": "pyiceberg.io.pyarrow.PyArrowFileIO",
            "s3.endpoint": os.environ["MINIO_ENDPOINT"],
            "s3.access-key-id": os.environ["MINIO_ROOT_USER"],
            "s3.secret-access-key": os.environ["MINIO_ROOT_PASSWORD"],
            "s3.region": "us-east-1",
            "s3.path-style-access": "true",
        },
    )


def event_table_schema():
    from pyiceberg.schema import Schema
    from pyiceberg.types import (
        IntegerType,
        LongType,
        NestedField,
        StringType,
        TimestamptzType,
    )

    return Schema(
        NestedField(1, "event_id", StringType()),
        NestedField(2, "event_type", StringType()),
        NestedField(3, "schema_version", IntegerType()),
        NestedField(4, "occurred_at", StringType()),
        NestedField(5, "source_system", StringType()),
        NestedField(6, "correlation_id", StringType()),
        NestedField(7, "kafka_partition", IntegerType()),
        NestedField(8, "kafka_offset", LongType()),
        NestedField(INGESTED_AT_FIELD_ID, "ingested_at", TimestamptzType()),
    )


def event_arrow_schema():
    from pyiceberg.io.pyarrow import schema_to_pyarrow

    return schema_to_pyarrow(event_table_schema())


def runs_table_schema():
    from pyiceberg.schema import Schema
    from pyiceberg.types import LongType, NestedField, StringType, TimestamptzType

    return Schema(
        NestedField(1, "run_id", StringType()),
        NestedField(2, "started_at", TimestamptzType()),
        NestedField(3, "finished_at", TimestamptzType()),
        NestedField(4, "records_read", LongType()),
        NestedField(5, "records_written", LongType()),
        NestedField(6, "records_rejected", LongType()),
    )


def runs_arrow_schema():
    from pyiceberg.io.pyarrow import schema_to_pyarrow

    return schema_to_pyarrow(runs_table_schema())


def get_table(catalog):
    from pyiceberg.exceptions import NoSuchTableError
    from pyiceberg.partitioning import PartitionField, PartitionSpec
    from pyiceberg.transforms import DayTransform

    if not catalog.namespace_exists("ledgerline_raw"):
        catalog.create_namespace("ledgerline_raw")

    schema = event_table_schema()
    try:
        table = catalog.load_table(TABLE_IDENTIFIER)
    except NoSuchTableError:
        # Partition by ingestion day so maintenance can expire old snapshots
        # and batches stay small; the raw layer is scanned by day, not by event.
        spec = PartitionSpec(
            PartitionField(
                source_id=INGESTED_AT_FIELD_ID,
                field_id=INGESTED_AT_PARTITION_FIELD_ID,
                transform=DayTransform(),
                name="ingested_at_day",
            )
        )
        return catalog.create_table(TABLE_IDENTIFIER, schema=schema, partition_spec=spec)

    expected_columns = {field.name for field in schema.fields}
    existing_columns = set(table.schema().column_names)
    missing_columns = expected_columns - existing_columns
    if missing_columns:
        with table.update_schema() as update:
            for field in schema.fields:
                if field.name in missing_columns:
                    update.add_column(field.name, field.field_type)
    return catalog.load_table(TABLE_IDENTIFIER)


def get_runs_table(catalog):
    from pyiceberg.exceptions import NoSuchTableError

    if not catalog.namespace_exists("ledgerline_raw"):
        catalog.create_namespace("ledgerline_raw")
    try:
        return catalog.load_table(RUNS_TABLE_IDENTIFIER)
    except NoSuchTableError:
        return catalog.create_table(RUNS_TABLE_IDENTIFIER, schema=runs_table_schema())


def normalize_message(message) -> dict[str, object]:
    try:
        payload = json.loads(message.value().decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ContractValidationError("message value must be UTF-8 JSON") from error

    validated_event = validate_event(payload)
    return {
        "event_id": validated_event["event_id"],
        "event_type": validated_event["event_type"],
        "schema_version": validated_event["schema_version"],
        "occurred_at": validated_event["occurred_at"],
        "source_system": validated_event["source_system"],
        "correlation_id": validated_event["correlation_id"],
        "kafka_partition": message.partition(),
        "kafka_offset": message.offset(),
        "ingested_at": datetime.now(UTC),
    }


def send_to_dlq(producer: Producer, message, error: Exception) -> None:
    producer.produce(
        DLQ_TOPIC,
        json.dumps(
            {
                "error": str(error),
                "topic": message.topic(),
                "partition": message.partition(),
                "offset": message.offset(),
                "value": message.value().decode("utf-8", errors="replace"),
            }
        ).encode("utf-8"),
    )
    producer.poll(0)


def record_run(catalog, started_at, finished_at, records_read, records_written, rejected):
    """Append one audit row per non-empty batch. Best effort: ingestion must not
    fail because the audit trail is unavailable."""

    try:
        runs_table = get_runs_table(catalog)
        runs_table.append(
            pa.Table.from_pylist(
                [
                    {
                        "run_id": str(uuid4()),
                        "started_at": started_at,
                        "finished_at": finished_at,
                        "records_read": records_read,
                        "records_written": records_written,
                        "records_rejected": rejected,
                    }
                ],
                schema=runs_arrow_schema(),
            )
        )
    except Exception:
        LOGGER.warning("ingestion_run_audit_failed", exc_info=True)


def push_batch_metrics(records_read, records_written, rejected, duration_seconds):
    if not PUSHGATEWAY_URL:
        return
    try:
        from prometheus_client import CollectorRegistry, Gauge, push_to_gateway

        registry = CollectorRegistry()
        for name, value, doc in (
            ("records_read", records_read, "Messages polled in the batch"),
            ("records_written", records_written, "Messages appended to Iceberg"),
            ("records_rejected", rejected, "Messages sent to the DLQ"),
            ("duration_seconds", duration_seconds, "Batch wall time"),
        ):
            Gauge(f"ledgerline_batch_{name}", doc, registry=registry).set(value)
        push_to_gateway(
            PUSHGATEWAY_URL, job="ledgerline_ingest", registry=registry, timeout=5
        )
    except Exception:
        LOGGER.warning("metrics_push_failed", exc_info=True)


def run_sync(
    max_messages: int = 100_000,
    poll_timeout: float = 1.0,
    idle_polls: int = 3,
    time_budget_seconds: float = 240.0,
) -> int:
    """Drain the topic until it goes idle or the time budget runs out.

    A fixed per-run message cap silently bounds throughput (100 messages an
    hour is ~2.4k events/day), so the batch keeps reading while messages
    arrive and stops after `idle_polls` consecutive empty polls. The budget
    leaves headroom under the Airflow execution timeout for the Iceberg
    append, offset commit, and audit write.
    """

    started_at = datetime.now(UTC)
    deadline = time.monotonic() + time_budget_seconds
    catalog = get_catalog()
    table = get_table(catalog)
    consumer = Consumer(
        {
            "bootstrap.servers": BOOTSTRAP_SERVERS,
            "group.id": CONSUMER_GROUP,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    dlq_producer = Producer({"bootstrap.servers": BOOTSTRAP_SERVERS})
    consumer.subscribe([TOPIC])

    batch: list[dict[str, object]] = []
    records_read = 0
    rejected_count = 0
    idle = 0
    try:
        while records_read < max_messages and time.monotonic() < deadline:
            message = consumer.poll(poll_timeout)
            if message is None:
                idle += 1
                if idle >= idle_polls:
                    break
                continue
            if message.error():
                raise RuntimeError(f"Kafka error: {message.error()}")
            idle = 0
            records_read += 1
            try:
                batch.append(normalize_message(message))
            except ContractValidationError as error:
                rejected_count += 1
                send_to_dlq(dlq_producer, message, error)
                LOGGER.warning(
                    "invalid_event_sent_to_dlq",
                    extra={"offset": message.offset(), "reason": str(error)},
                )

        pending_dlq_messages = dlq_producer.flush(10)
        if pending_dlq_messages:
            raise RuntimeError(f"Timed out delivering {pending_dlq_messages} DLQ message(s)")

        if batch:
            table.append(pa.Table.from_pylist(batch, schema=event_arrow_schema()))
        consumer.commit(asynchronous=False)
        finished_at = datetime.now(UTC)
        LOGGER.info(
            "kafka_batch_committed",
            extra={"records_written": len(batch), "records_rejected": rejected_count},
        )
        if records_read:
            record_run(
                catalog, started_at, finished_at, records_read, len(batch), rejected_count
            )
            push_batch_metrics(
                records_read,
                len(batch),
                rejected_count,
                (finished_at - started_at).total_seconds(),
            )
        return len(batch)
    finally:
        consumer.close()


if __name__ == "__main__":
    run_sync()
