import json
import logging
import os
from datetime import UTC, datetime

import pyarrow as pa
from confluent_kafka import Consumer, Producer
from ledgerline_contract import ContractValidationError, parse_message_value, validate_event
from pyiceberg.catalog import load_catalog
from pyiceberg.exceptions import NoSuchTableError
from pyiceberg.types import IntegerType, LongType, StringType

TABLE_IDENTIFIER = "ledgerline_raw.events"
TOPIC = os.getenv("KAFKA_TOPIC", "ledgerline_events")
DLQ_TOPIC = os.getenv("KAFKA_DLQ_TOPIC", f"{TOPIC}_dlq")
BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
CONSUMER_GROUP = os.getenv("KAFKA_CONSUMER_GROUP", "ledgerline-iceberg-consumer")
LOGGER = logging.getLogger(__name__)
EVENT_SCHEMA = pa.schema([
    ("id", pa.int64()),
    ("data", pa.string()),
    ("event_id", pa.string()),
    ("event_type", pa.string()),
    ("schema_version", pa.int32()),
    ("occurred_at", pa.string()),
    ("source_system", pa.string()),
    ("correlation_id", pa.string()),
    ("kafka_partition", pa.int32()),
    ("kafka_offset", pa.int64()),
    ("ingested_at", pa.string()),
])
REQUIRED_COLUMNS = {
    "event_id": StringType(),
    "event_type": StringType(),
    "schema_version": IntegerType(),
    "occurred_at": StringType(),
    "source_system": StringType(),
    "correlation_id": StringType(),
    "kafka_partition": IntegerType(),
    "kafka_offset": LongType(),
    "ingested_at": StringType(),
}


def get_catalog():
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


def get_table(catalog):
    if not catalog.namespace_exists("ledgerline_raw"):
        catalog.create_namespace("ledgerline_raw")

    try:
        table = catalog.load_table(TABLE_IDENTIFIER)
    except NoSuchTableError:
        return catalog.create_table(TABLE_IDENTIFIER, schema=EVENT_SCHEMA)

    existing_columns = set(table.schema().column_names)
    missing_columns = REQUIRED_COLUMNS.keys() - existing_columns
    if missing_columns:
        with table.update_schema() as update:
            for column_name in sorted(missing_columns):
                update.add_column(column_name, REQUIRED_COLUMNS[column_name])
    return catalog.load_table(TABLE_IDENTIFIER)


def normalize_message(message) -> dict[str, object]:
    payload = parse_message_value(message.value())
    validated_event = validate_event(payload)
    return {
        "id": validated_event["id"],
        "data": json.dumps(validated_event["data"], separators=(",", ":"), sort_keys=True),
        "event_id": validated_event["event_id"],
        "event_type": validated_event["event_type"],
        "schema_version": validated_event["schema_version"],
        "occurred_at": validated_event["occurred_at"],
        "source_system": validated_event["source_system"],
        "correlation_id": validated_event["correlation_id"],
        "kafka_partition": message.partition(),
        "kafka_offset": message.offset(),
        "ingested_at": datetime.now(UTC).isoformat(),
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
                "value": (message.value() or b"").decode("utf-8", errors="replace"),
            }
        ).encode("utf-8"),
    )
    producer.poll(0)


def run_sync(max_messages: int = 100, poll_timeout: float = 1.0) -> int:
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
    rejected_count = 0
    try:
        for _ in range(max_messages):
            message = consumer.poll(poll_timeout)
            if message is None:
                break
            if message.error():
                raise RuntimeError(f"Kafka error: {message.error()}")
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
            table.append(pa.Table.from_pylist(batch, schema=EVENT_SCHEMA))
        consumer.commit(asynchronous=False)
        LOGGER.info(
            "kafka_batch_committed",
            extra={"records_written": len(batch), "records_rejected": rejected_count},
        )
        return len(batch)
    finally:
        consumer.close()


if __name__ == "__main__":
    run_sync()
