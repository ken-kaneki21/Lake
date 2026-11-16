import unittest

from streaming.source_version import SourceVersion


def event(lsn, order, op="u"):
    return {"op": op, "source": {"lsn": lsn}, "transaction": {"total_order": order}}


class VersionTest(unittest.TestCase):
    def test_lsn_ties_use_transaction_order(self):
        older = SourceVersion.from_envelope(event(42, "7"))
        newer = SourceVersion.from_envelope(event(42, "11"))
        self.assertGreater(newer, older)
        self.assertEqual(newer, SourceVersion.from_envelope(event(42, "11")))
        self.assertGreater(SourceVersion.from_envelope(event(43, "1")), newer)

    def test_snapshot_cannot_overwrite_wal(self):
        snapshot = SourceVersion.from_envelope({"op": "r", "source": {"lsn": 99}})
        self.assertGreater(SourceVersion.from_envelope(event(42, "1", "d")), snapshot)

    def test_missing_order_fails_closed(self):
        with self.assertRaises(ValueError):
            SourceVersion.from_envelope({"op": "u", "source": {"lsn": 42}})
