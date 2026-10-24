import json
from pathlib import Path
from typing import Any

from jsonschema import Draft202012Validator, FormatChecker


class ContractValidationError(ValueError):
    """Raised when an event does not conform to a supported Ledgerline contract version."""


CONTRACT_DIR = Path(__file__).resolve().parents[1] / "contracts"

# One immutable file per contract version; readers support every version
# listed here (see docs/contracts.md for the governance rules).
CONTRACT_FILES = {
    1: CONTRACT_DIR / "kafka_event_v1.schema.json",
    2: CONTRACT_DIR / "kafka_event_v2.schema.json",
}


def _build_validator(path: Path) -> Draft202012Validator:
    schema = json.loads(path.read_text(encoding="utf-8"))
    return Draft202012Validator(schema, format_checker=FormatChecker())


VALIDATORS = {version: _build_validator(path) for version, path in CONTRACT_FILES.items()}


def validate_event(payload: Any) -> dict[str, Any]:
    """Validate and return one Ledgerline event of a supported version."""

    version = payload.get("schema_version") if isinstance(payload, dict) else None
    validator = VALIDATORS.get(version)
    if validator is None:
        supported = ", ".join(str(v) for v in sorted(VALIDATORS))
        raise ContractValidationError(
            f"schema_version: unsupported version {version!r} (supported: {supported})"
        )

    errors = sorted(validator.iter_errors(payload), key=lambda error: list(error.path))
    if errors:
        error = errors[0]
        field = ".".join(str(part) for part in error.path) or "event"
        raise ContractValidationError(f"{field}: {error.message}")

    return payload
