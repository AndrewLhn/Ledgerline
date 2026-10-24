import copy
import json
import sys
import unittest
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "dags"))
sys.path.insert(0, str(PROJECT_ROOT / "infra"))

from ledgerline_contract import ContractValidationError, validate_event  # noqa: E402
from producer import build_ledgerline_events, build_messages, build_v2_payment  # noqa: E402


class LedgerlineContractTests(unittest.TestCase):
    def setUp(self) -> None:
        self.event = build_ledgerline_events("2026-10-01T10:00:00+00:00")[0]

    def test_accepts_a_valid_order_event(self) -> None:
        self.assertEqual(validate_event(self.event), self.event)

    def test_rejects_unknown_event_type(self) -> None:
        event = copy.deepcopy(self.event)
        event["event_type"] = "order_deleted"

        with self.assertRaises(ContractValidationError):
            validate_event(event)

    def test_rejects_invalid_event_identifier(self) -> None:
        event = copy.deepcopy(self.event)
        event["event_id"] = "not-a-uuid"

        with self.assertRaises(ContractValidationError):
            validate_event(event)

    def test_rejects_invalid_event_payload(self) -> None:
        event = copy.deepcopy(self.event)
        del event["data"]["amount_minor"]

        with self.assertRaises(ContractValidationError):
            validate_event(event)

    def test_rejects_fractional_amount(self) -> None:
        event = copy.deepcopy(self.event)
        event["data"]["amount_minor"] = 100.5

        with self.assertRaises(ContractValidationError):
            validate_event(event)

    def test_rejects_legacy_sequence_field(self) -> None:
        event = copy.deepcopy(self.event)
        event["id"] = 1

        with self.assertRaises(ContractValidationError):
            validate_event(event)

    def test_duplicate_scenario_reuses_payment_event_identity(self) -> None:
        payment_event = build_ledgerline_events("2026-10-01T10:00:00+00:00")[1]
        key, duplicate = build_messages("duplicate")[0]

        self.assertEqual(key, payment_event["event_id"])
        self.assertIn(payment_event["event_id"].encode("utf-8"), duplicate)

    def test_invalid_scenario_bypasses_producer_validation_for_dlq_coverage(self) -> None:
        key, invalid_message = build_messages("invalid")[0]

        self.assertIsNone(key)
        self.assertEqual(invalid_message, b'{"event_id": "not-a-valid-ledgerline-event"}')


class ContractV2Tests(unittest.TestCase):
    def setUp(self) -> None:
        self.event = build_v2_payment("2026-10-01T10:00:00+00:00")

    def test_accepts_v2_payment_with_method(self) -> None:
        self.assertEqual(validate_event(self.event), self.event)

    def test_v2_requires_payment_method(self) -> None:
        event = copy.deepcopy(self.event)
        del event["data"]["method"]

        with self.assertRaises(ContractValidationError):
            validate_event(event)

    def test_v1_payment_rejects_method_field(self) -> None:
        event = build_ledgerline_events("2026-10-01T10:00:00+00:00")[1]
        event["data"]["method"] = "card"

        with self.assertRaises(ContractValidationError):
            validate_event(event)

    def test_unknown_version_is_rejected(self) -> None:
        event = copy.deepcopy(self.event)
        event["schema_version"] = 3

        with self.assertRaises(ContractValidationError):
            validate_event(event)

    def test_v2_scenario_publishes_keyed_v2_payment(self) -> None:
        key, payload = build_messages("v2")[0]

        self.assertEqual(key, self.event["event_id"])
        self.assertEqual(json.loads(payload)["schema_version"], 2)


if __name__ == "__main__":
    unittest.main()
