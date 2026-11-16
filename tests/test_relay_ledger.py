import tempfile
import unittest
from pathlib import Path

from chaos.ledger import arm, delivered, open_ledger, pending, stage

TOPIC = "pg.app.trips"


def event(offset, key="4"):
    return TOPIC, 0, offset, key, b"key", b"value", 100 + offset


def rewrite(mode, value):
    return mode.encode() + b":" + value


class RelayLedgerTest(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.path = Path(directory.name) / "state.db"
        self.conn = open_ledger(self.path)
        self.addCleanup(lambda: self.conn.close())

    def test_input_replay_keeps_the_original_decision(self):
        arm(self.conn, TOPIC, "4", "duplicate", "two deliveries")
        stage(self.conn, event(1), rewrite, 10)
        stage(self.conn, event(1), rewrite, 11)
        self.conn.close()
        self.conn = open_ledger(self.path)
        rows = pending(self.conn, 12)
        self.assertEqual([row["offset"] for row in rows], [1, 1])
        for row in rows:
            delivered(self.conn, row, 12)
        self.assertEqual(pending(self.conn, 13), [])
        self.assertEqual(self.conn.execute("SELECT released_at FROM fault").fetchone()[0], 12)

    def test_drop_has_ledger_entry_without_output(self):
        arm(self.conn, TOPIC, "4", "drop", "absence")
        stage(self.conn, event(2), rewrite, 10)
        self.assertEqual(pending(self.conn, 20), [])
        self.assertEqual(self.conn.execute("SELECT source_lsn FROM fault").fetchone()[0], 102)

    def test_delay_holds_until_due(self):
        arm(self.conn, TOPIC, "4", "delay", "late", delay_seconds=30)
        stage(self.conn, event(3), rewrite, 10)
        self.assertEqual(pending(self.conn, 39), [])
        self.assertEqual(pending(self.conn, 40)[0]["offset"], 3)

    def test_reorder_requires_a_successor_for_same_key(self):
        arm(self.conn, TOPIC, "4", "reorder", "newer then older")
        stage(self.conn, event(4), rewrite, 10)
        stage(self.conn, event(5, "unrelated"), rewrite, 11)
        self.assertEqual([row["offset"] for row in pending(self.conn, 11)], [5])
        stage(self.conn, event(6), rewrite, 12)
        self.assertEqual([row["offset"] for row in pending(self.conn, 13)], [5, 6, 4])

    def test_mutation_and_schema_probe_persist_rewritten_payload(self):
        for index, mode in enumerate(("mutate", "schema_drift")):
            arm(self.conn, TOPIC, "4", mode, "rewritten")
            stage(self.conn, event(index), rewrite, 10 + index)
        self.assertEqual([row["value"] for row in pending(self.conn, 20)],
                         [b"mutate:value", b"schema_drift:value"])
