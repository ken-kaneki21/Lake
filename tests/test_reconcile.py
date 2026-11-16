import unittest
from datetime import datetime, timezone
from decimal import Decimal

from recon.reconcile import BUCKETS, bucket_map, key_differences, settled_windows


class ReconcileTest(unittest.TestCase):
    def test_windows_are_settled_and_cover_24h(self):
        windows = list(settled_windows(datetime.fromisoformat("2026-11-16T16:13:55+05:30")))
        self.assertEqual(len(windows), 288)
        self.assertEqual(windows[-1][1].isoformat(), "2026-11-16T10:30:00+00:00")
        self.assertTrue(all(a[1] == b[0] for a, b in zip(windows, windows[1:])))

    def test_bucket_contract_rejects_missing_duplicate_and_unknown(self):
        rows = [(name, 1, Decimal(10), 0, 0, 42) for name in sorted(BUCKETS)]
        self.assertEqual(bucket_map(rows)["ff"]["fare_cents"], 10)
        for invalid in (rows[:-1], rows + rows[:1], rows[:-1] + [("gg", 0, 0, 0, 0, 0)]):
            with self.assertRaises(ValueError):
                bucket_map(invalid)

    def test_exact_drill_down_classifies_rows(self):
        timestamp = datetime(2026, 11, 16, tzinfo=timezone.utc)

        def row(key, amount="1.00", deleted=False):
            return key, timestamp, Decimal(amount), Decimal("0.00"), deleted

        source = [row(1), row(2), row(3, deleted=True), row(5)]
        target = [row(2, "1.01"), row(3), row(4), row(5)]
        mismatches = list(key_differences(source, target))
        self.assertEqual(
            [(item["trip_id"], item["category"]) for item in mismatches],
            [(1, "missing"), (2, "changed"), (3, "delete_state"), (4, "extra")],
        )
        with self.assertRaises(ValueError):
            list(key_differences(source + [row(1)], target))
