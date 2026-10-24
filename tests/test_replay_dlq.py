import copy
import json
import sys
import unittest
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "scripts"))
sys.path.insert(0, str(PROJECT_ROOT / "dags"))
sys.path.insert(0, str(PROJECT_ROOT / "infra"))

from producer import build_ledgerline_events  # noqa: E402
from replay_dlq import classify  # noqa: E402


def dlq_record(event: dict, error: str = "schema_version: required") -> bytes:
    return json.dumps(
        {
            "error": error,
            "topic": "ledgerline_events",
            "partition": 0,
            "offset": 42,
            "value": json.dumps(event),
        }
    ).encode("utf-8")


class ReplayDlqTests(unittest.TestCase):
    def setUp(self) -> None:
        self.event = build_ledgerline_events("2026-10-01T10:00:00+00:00")[0]

    def test_replayable_event_is_reencoded_with_identity(self) -> None:
        status, detail, payload = classify(dlq_record(self.event))

        self.assertEqual(status, "replayable")
        self.assertEqual(detail, self.event["event_id"])
        self.assertEqual(json.loads(payload), self.event)

    def test_event_violating_current_contract_stays_invalid(self) -> None:
        legacy = copy.deepcopy(self.event)
        legacy["id"] = 1  # rejected since the demo sequence left the contract

        status, _, payload = classify(dlq_record(legacy))

        self.assertEqual(status, "still_invalid")
        self.assertIsNone(payload)

    def test_corrupt_record_stays_invalid(self) -> None:
        status, _, payload = classify(b"not-json")

        self.assertEqual(status, "still_invalid")
        self.assertIsNone(payload)

    def test_envelope_without_value_stays_invalid(self) -> None:
        record = json.dumps({"error": "boom", "topic": "t", "partition": 0, "offset": 1}).encode()

        status, _, _ = classify(record)

        self.assertEqual(status, "still_invalid")


if __name__ == "__main__":
    unittest.main()
