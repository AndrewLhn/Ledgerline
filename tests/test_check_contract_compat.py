import json
import sys
import tempfile
import unittest
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT / "scripts") not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT / "scripts"))

from check_contract_compat import check_self_consistent, contract_files  # noqa: E402


class ContractCompatTests(unittest.TestCase):
    def test_repository_contracts_are_self_consistent(self) -> None:
        self.assertEqual(check_self_consistent(), [])

    def test_repository_tracks_expected_versions(self) -> None:
        self.assertEqual(sorted(contract_files()), [1, 2])

    def test_detects_version_mismatch(self) -> None:
        schema = json.loads(
            (PROJECT_ROOT / "contracts" / "kafka_event_v1.schema.json").read_text()
        )
        schema["properties"]["schema_version"]["const"] = 5

        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "kafka_event_v1.schema.json"
            path.write_text(json.dumps(schema))

            errors = check_self_consistent(Path(tmp))

        self.assertEqual(len(errors), 1)
        self.assertIn("const", errors[0])

    def test_detects_invalid_json(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            (Path(tmp) / "kafka_event_v1.schema.json").write_text("{broken")

            errors = check_self_consistent(Path(tmp))

        self.assertEqual(len(errors), 1)
        self.assertIn("invalid JSON", errors[0])

    def test_detects_invalid_schema(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            (Path(tmp) / "kafka_event_v1.schema.json").write_text(json.dumps({"type": 42}))

            errors = check_self_consistent(Path(tmp))

        self.assertEqual(len(errors), 1)
        self.assertIn("Draft 2020-12", errors[0])

    def test_empty_directory_is_an_error(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            errors = check_self_consistent(Path(tmp))

        self.assertEqual(len(errors), 1)
        self.assertIn("no versioned contract files", errors[0])


if __name__ == "__main__":
    unittest.main()
