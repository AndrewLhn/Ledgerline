"""Expire old snapshots on the Ledgerline raw tables.

Keeps seven days of snapshot history so time travel during incident triage
stays possible while metadata does not grow forever. Data-file compaction and
orphan-file removal need Spark procedures with pyiceberg 0.6 and are called
out as a production step in docs/architecture.md.
"""

import logging
import os
from datetime import UTC, datetime, timedelta

LOGGER = logging.getLogger("iceberg_maintenance")

RETENTION_DAYS = int(os.getenv("ICEBERG_SNAPSHOT_RETENTION_DAYS", "7"))
TABLES = [
    "ledgerline_raw.events",
    "ledgerline_raw.ingestion_runs",
]


def main() -> None:
    from kafka_to_iceberg_run import get_catalog
    from pyiceberg.exceptions import NoSuchTableError

    catalog = get_catalog()
    older_than_ms = int((datetime.now(UTC) - timedelta(days=RETENTION_DAYS)).timestamp() * 1000)

    for identifier in TABLES:
        try:
            table = catalog.load_table(identifier)
        except NoSuchTableError:
            LOGGER.info("table_missing_skipped", extra={"table": identifier})
            continue

        if not table.metadata.snapshots:
            LOGGER.info("no_snapshots_skipped", extra={"table": identifier})
            continue

        before = len(table.metadata.snapshots)
        # Never expire the newest snapshot, even on a stack idle for longer
        # than the retention window.
        newest_ms = max(s.timestamp_ms for s in table.metadata.snapshots)
        cutoff_ms = min(older_than_ms, newest_ms)
        table.manage_snapshots().expire_snapshots_older_than(cutoff_ms).commit()
        after = len(catalog.load_table(identifier).metadata.snapshots)
        LOGGER.info(
            "snapshots_expired",
            extra={"table": identifier, "before": before, "after": after},
        )
        print(f"{identifier}: snapshots {before} -> {after}")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
