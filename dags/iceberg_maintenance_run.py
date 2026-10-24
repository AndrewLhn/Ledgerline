"""Expire old snapshots on the Ledgerline raw tables.

pyiceberg exposes no high-level expire API (ManageSnapshots only handles
refs), so this commits a RemoveSnapshotsUpdate through the catalog: snapshots
older than the retention window leave the table metadata, and time travel
past the window stops. Data files are not deleted here; orphan-file cleanup
and compaction stay documented Spark steps (ADR-005).
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


def select_expired_snapshots(metadata, older_than_ms: int) -> list[int]:
    """Snapshot ids older than the cutoff that no branch or tag points at."""

    protected = {ref.snapshot_id for ref in metadata.refs.values()}
    return [
        snapshot.snapshot_id
        for snapshot in metadata.snapshots
        if snapshot.timestamp_ms < older_than_ms and snapshot.snapshot_id not in protected
    ]


def expire_table(catalog, identifier: str, older_than_ms: int) -> int:
    from pyiceberg.exceptions import NoSuchTableError
    from pyiceberg.table.update import RemoveSnapshotsUpdate

    try:
        table = catalog.load_table(identifier)
    except NoSuchTableError:
        LOGGER.info("table_missing_skipped", extra={"table": identifier})
        return 0

    snapshot_ids = select_expired_snapshots(table.metadata, older_than_ms)
    if not snapshot_ids:
        LOGGER.info("nothing_to_expire", extra={"table": identifier})
        return 0

    catalog.commit_table(table, (), (RemoveSnapshotsUpdate(snapshot_ids=snapshot_ids),))
    LOGGER.info(
        "snapshots_expired", extra={"table": identifier, "count": len(snapshot_ids)}
    )
    return len(snapshot_ids)


def main() -> None:
    from kafka_to_iceberg_run import get_catalog

    catalog = get_catalog()
    older_than_ms = int(
        (datetime.now(UTC) - timedelta(days=RETENTION_DAYS)).timestamp() * 1000
    )
    for identifier in TABLES:
        expired = expire_table(catalog, identifier, older_than_ms)
        print(f"{identifier}: expired {expired} snapshot(s)")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
