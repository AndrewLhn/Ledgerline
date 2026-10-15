import copy
import sys
import unittest
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "dags"))
sys.path.insert(0, str(PROJECT_ROOT / "infra"))

from ledgerline_contract import ContractValidationError, parse_message_value, validate_event
from producer import build_ledgerline_events, build_messages


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
        del event["data"]["amount"]

        with self.assertRaises(ContractValidationError):
            validate_event(event)

    def test_rejects_malformed_timestamp(self) -> None:
        for value in ("yesterday-ish", "2026-13-45T99:99:99Z", "2026-10-01T10:00:00"):
            with self.subTest(occurred_at=value):
                event = copy.deepcopy(self.event)
                event["occurred_at"] = value

                with self.assertRaises(ContractValidationError):
                    validate_event(event)

    def test_rejects_missing_required_field_for_every_event_type(self) -> None:
        for event in build_ledgerline_events("2026-10-01T10:00:00+00:00"):
            with self.subTest(event_type=event["event_type"]):
                broken = copy.deepcopy(event)
                del broken["data"][next(iter(broken["data"]))]

                with self.assertRaises(ContractValidationError):
                    validate_event(broken)

    def test_parse_message_value_accepts_json(self) -> None:
        self.assertEqual(parse_message_value(b'{"a": 1}'), {"a": 1})

    def test_parse_message_value_rejects_bad_input_instead_of_crashing(self) -> None:
        for raw in (None, b"", b"\xff\xfe", b"not json"):
            with self.subTest(raw=raw):
                with self.assertRaises(ContractValidationError):
                    parse_message_value(raw)

    def test_duplicate_scenario_reuses_payment_event_identity(self) -> None:
        payment_event = build_ledgerline_events("2026-10-01T10:00:00+00:00")[1]
        duplicate = build_messages("duplicate")[0]

        self.assertIn(payment_event["event_id"].encode("utf-8"), duplicate)

    def test_invalid_scenario_bypasses_producer_validation_for_dlq_coverage(self) -> None:
        invalid_message = build_messages("invalid")[0]

        self.assertEqual(invalid_message, b'{"event_id": "not-a-valid-ledgerline-event"}')


if __name__ == "__main__":
    unittest.main()
