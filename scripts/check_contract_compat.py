"""Guard the event contracts in CI.

Two rules (docs/contracts.md):

1. Self-consistency: every contracts/kafka_event_v<N>.schema.json must be a
   valid Draft 2020-12 schema whose schema_version const equals N.
2. Immutability: an existing contract file may not change or disappear.
   Pre-release exceptions need the `contract-breaking` label on the PR,
   which the workflow translates into --allow-changes.

Usage:
    python scripts/check_contract_compat.py
    python scripts/check_contract_compat.py --base origin/main
"""

import argparse
import json
import re
import subprocess
import sys
from pathlib import Path

from jsonschema import Draft202012Validator

PROJECT_ROOT = Path(__file__).resolve().parents[1]
CONTRACTS_DIR = PROJECT_ROOT / "contracts"
FILENAME_PATTERN = re.compile(r"^kafka_event_v(?P<version>\d+)\.schema\.json$")


def contract_files(directory: Path = CONTRACTS_DIR) -> dict[int, Path]:
    found = {}
    for path in sorted(directory.glob("kafka_event_v*.schema.json")):
        match = FILENAME_PATTERN.match(path.name)
        if match:
            found[int(match.group("version"))] = path
    return found


def check_self_consistent(directory: Path = CONTRACTS_DIR) -> list[str]:
    errors = []
    files = contract_files(directory)
    if not files:
        return [f"no versioned contract files found in {directory}"]

    for version, path in files.items():
        try:
            schema = json.loads(path.read_text(encoding="utf-8"))
        except json.JSONDecodeError as error:
            errors.append(f"{path.name}: invalid JSON ({error})")
            continue
        try:
            Draft202012Validator.check_schema(schema)
        except Exception as error:
            errors.append(f"{path.name}: not a valid Draft 2020-12 schema ({error})")
            continue

        declared = schema.get("properties", {}).get("schema_version", {}).get("const")
        if declared != version:
            errors.append(
                f"{path.name}: schema_version const is {declared!r}, filename says {version}"
            )
    return errors


def git(*args: str) -> str:
    return subprocess.run(
        ["git", *args],
        cwd=PROJECT_ROOT,
        capture_output=True,
        text=True,
        check=True,
    ).stdout


def check_immutable(base_ref: str) -> list[str]:
    """Every contract file that exists in the base ref must be unchanged."""

    errors = []
    base_files = git("ls-tree", "--name-only", base_ref, "contracts/").split()
    for name in base_files:
        relative = Path(name).name
        path = CONTRACTS_DIR / relative
        try:
            base_content = git("show", f"{base_ref}:{name}")
        except subprocess.CalledProcessError:
            continue
        if not path.exists():
            errors.append(f"{relative}: deleted; contract files are immutable")
        elif path.read_text(encoding="utf-8") != base_content:
            errors.append(f"{relative}: modified; contract files are immutable")
    return errors


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Contract governance checks.")
    parser.add_argument(
        "--base",
        help="Git ref to compare against; enables the immutability check.",
    )
    parser.add_argument(
        "--allow-changes",
        action="store_true",
        help="Skip the immutability check (PRs labeled contract-breaking).",
    )
    args = parser.parse_args(argv)

    errors = check_self_consistent()
    if args.base and not args.allow_changes:
        errors += check_immutable(args.base)

    if errors:
        for error in errors:
            print(f"contract check failed: {error}", file=sys.stderr)
        print(
            "See docs/contracts.md: add a new version file instead of editing "
            "an existing one, or label the PR contract-breaking.",
            file=sys.stderr,
        )
        return 1

    print(f"contract checks passed ({len(contract_files())} versioned files)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
