import json
import sys
import unittest
from pathlib import Path
from unittest import mock

import pyarrow as pa

PROJECT_ROOT = Path(__file__).resolve().parents[1]
for _p in (PROJECT_ROOT / "dags", PROJECT_ROOT / "infra"):
    if str(_p) not in sys.path:
        sys.path.insert(0, str(_p))

import kafka_to_iceberg_run as consumer_mod  # noqa: E402
from producer import build_ledgerline_events  # noqa: E402

EVENT_ARROW_SCHEMA = pa.schema([
    ("event_id", pa.string()),
    ("event_type", pa.string()),
    ("schema_version", pa.int32()),
    ("occurred_at", pa.string()),
    ("source_system", pa.string()),
    ("correlation_id", pa.string()),
    ("kafka_partition", pa.int32()),
    ("kafka_offset", pa.int64()),
    ("ingested_at", pa.timestamp("us", tz="UTC")),
])

RUNS_ARROW_SCHEMA = pa.schema([
    ("run_id", pa.string()),
    ("started_at", pa.timestamp("us", tz="UTC")),
    ("finished_at", pa.timestamp("us", tz="UTC")),
    ("records_read", pa.int64()),
    ("records_written", pa.int64()),
    ("records_rejected", pa.int64()),
])

VALID_EVENTS = build_ledgerline_events("2026-10-01T10:00:00+00:00")


class FakeMessage:
    def __init__(self, payload: bytes, offset: int = 0, error=None):
        self._payload = payload
        self._offset = offset
        self._error = error

    def value(self):
        return self._payload

    def error(self):
        return self._error

    def topic(self):
        return "ledgerline_events"

    def partition(self):
        return 0

    def offset(self):
        return self._offset


class FakeConsumer:
    def __init__(self, script):
        self.script = list(script)
        self.committed = 0
        self.closed = False

    def subscribe(self, topics):
        self.subscribed = topics

    def poll(self, timeout):
        return self.script.pop(0) if self.script else None

    def commit(self, asynchronous=False):
        self.committed += 1

    def close(self):
        self.closed = True


class FakeProducer:
    def __init__(self, flush_pending=0):
        self.produced = []
        self.flush_pending = flush_pending

    def produce(self, topic, value, **kwargs):
        self.produced.append((topic, value))

    def poll(self, timeout):
        return 0

    def flush(self, timeout):
        return self.flush_pending


class FakeTable:
    def __init__(self, fail_on_append=False):
        self.appended = []
        self.fail_on_append = fail_on_append

    def append(self, arrow_table):
        if self.fail_on_append:
            raise RuntimeError("iceberg append failed")
        self.appended.append(arrow_table)


def run_with_fakes(script, dlq_flush_pending=0, append_fails=False):
    """Execute run_sync against fakes; return (result, exception, objects)."""

    consumer = FakeConsumer(script)
    dlq_producer = FakeProducer(flush_pending=dlq_flush_pending)
    events_table = FakeTable(fail_on_append=append_fails)
    runs_table = FakeTable()

    patches = [
        mock.patch.object(consumer_mod, "Consumer", lambda config: consumer),
        mock.patch.object(consumer_mod, "Producer", lambda config: dlq_producer),
        mock.patch.object(consumer_mod, "get_catalog", lambda: object()),
        mock.patch.object(consumer_mod, "get_table", lambda catalog: events_table),
        mock.patch.object(consumer_mod, "get_runs_table", lambda catalog: runs_table),
        mock.patch.object(consumer_mod, "event_arrow_schema", lambda: EVENT_ARROW_SCHEMA),
        mock.patch.object(consumer_mod, "runs_arrow_schema", lambda: RUNS_ARROW_SCHEMA),
    ]
    for patch in patches:
        patch.start()
    try:
        result, error = None, None
        try:
            result = consumer_mod.run_sync()
        except Exception as exc:  # noqa: BLE001 - asserted per test
            error = exc
    finally:
        for patch in patches:
            patch.stop()

    return result, error, {
        "consumer": consumer,
        "dlq_producer": dlq_producer,
        "events_table": events_table,
        "runs_table": runs_table,
    }


def valid_message(event, offset):
    return FakeMessage(json.dumps(event).encode("utf-8"), offset=offset)


class RunSyncTests(unittest.TestCase):
    def test_valid_batch_is_appended_then_committed(self):
        script = [valid_message(VALID_EVENTS[0], 1), valid_message(VALID_EVENTS[1], 2), None]

        result, error, objects = run_with_fakes(script)

        self.assertIsNone(error)
        self.assertEqual(result, 2)
        table = objects["events_table"].appended[0]
        self.assertEqual(table.num_rows, 2)
        self.assertEqual(table.column("event_id").to_pylist()[0], VALID_EVENTS[0]["event_id"])
        self.assertEqual(objects["consumer"].committed, 1)
        self.assertTrue(objects["consumer"].closed)

    def test_audit_row_records_batch_counts(self):
        script = [valid_message(VALID_EVENTS[0], 1), FakeMessage(b"{broken", offset=2), None]

        result, error, objects = run_with_fakes(script)

        self.assertIsNone(error)
        self.assertEqual(result, 1)
        audit = objects["runs_table"].appended[0].to_pylist()[0]
        self.assertEqual(audit["records_read"], 2)
        self.assertEqual(audit["records_written"], 1)
        self.assertEqual(audit["records_rejected"], 1)

    def test_invalid_message_is_sent_to_dlq_with_location(self):
        script = [FakeMessage(b'{"event_id": "nope"}', offset=7), None]

        result, error, objects = run_with_fakes(script)

        self.assertIsNone(error)
        self.assertEqual(result, 0)
        topic, value = objects["dlq_producer"].produced[0]
        self.assertEqual(topic, "ledgerline_events_dlq")
        record = json.loads(value)
        self.assertEqual(record["offset"], 7)
        self.assertIn("error", record)
        self.assertEqual(objects["consumer"].committed, 1)

    def test_malformed_utf8_is_sent_to_dlq(self):
        script = [FakeMessage(b"\xff\xfe not utf-8", offset=3), None]

        result, error, objects = run_with_fakes(script)

        self.assertIsNone(error)
        self.assertEqual(len(objects["dlq_producer"].produced), 1)

    def test_dlq_flush_timeout_fails_before_commit(self):
        script = [FakeMessage(b"{broken", offset=1), None]

        result, error, objects = run_with_fakes(script, dlq_flush_pending=1)

        self.assertIsInstance(error, RuntimeError)
        self.assertEqual(objects["consumer"].committed, 0)

    def test_append_failure_skips_offset_commit(self):
        script = [valid_message(VALID_EVENTS[0], 1), None]

        result, error, objects = run_with_fakes(script, append_fails=True)

        self.assertIsInstance(error, RuntimeError)
        self.assertEqual(objects["consumer"].committed, 0)

    def test_kafka_error_aborts_the_batch(self):
        script = [FakeMessage(b"", offset=1, error="broker transport failure"), None]

        result, error, objects = run_with_fakes(script)

        self.assertIsInstance(error, RuntimeError)
        self.assertEqual(objects["consumer"].committed, 0)

    def test_empty_batch_commits_without_writes_or_audit(self):
        result, error, objects = run_with_fakes([None])

        self.assertIsNone(error)
        self.assertEqual(result, 0)
        self.assertEqual(objects["events_table"].appended, [])
        self.assertEqual(objects["runs_table"].appended, [])
        self.assertEqual(objects["consumer"].committed, 1)


if __name__ == "__main__":
    unittest.main()
