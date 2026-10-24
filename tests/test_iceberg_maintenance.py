import sys
import unittest
from pathlib import Path
from types import SimpleNamespace

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT / "dags") not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT / "dags"))

from iceberg_maintenance_run import select_expired_snapshots  # noqa: E402

DAY_MS = 86_400_000


def metadata(snapshots, ref_heads=()):
    return SimpleNamespace(
        snapshots=[
            SimpleNamespace(snapshot_id=snapshot_id, timestamp_ms=timestamp_ms)
            for snapshot_id, timestamp_ms in snapshots
        ],
        refs={
            name: SimpleNamespace(snapshot_id=snapshot_id)
            for name, snapshot_id in ref_heads
        },
    )


class SelectExpiredSnapshotsTests(unittest.TestCase):
    def test_selects_snapshots_older_than_cutoff(self) -> None:
        meta = metadata([(1, 10 * DAY_MS), (2, 20 * DAY_MS), (3, 30 * DAY_MS)])

        self.assertEqual(select_expired_snapshots(meta, 25 * DAY_MS), [1, 2])

    def test_keeps_snapshots_pointed_at_by_refs(self) -> None:
        meta = metadata(
            [(1, 10 * DAY_MS), (2, 12 * DAY_MS)],
            ref_heads=[("main", 2), ("audit-tag", 1)],
        )

        self.assertEqual(select_expired_snapshots(meta, 25 * DAY_MS), [])

    def test_keeps_recent_snapshots(self) -> None:
        meta = metadata([(1, 30 * DAY_MS)])

        self.assertEqual(select_expired_snapshots(meta, 25 * DAY_MS), [])

    def test_empty_metadata(self) -> None:
        self.assertEqual(select_expired_snapshots(metadata([]), 1), [])


if __name__ == "__main__":
    unittest.main()
