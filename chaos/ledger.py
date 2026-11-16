"""SQLite fault ledger and durable Kafka relay outbox."""

import sqlite3
import time

MODES = ("drop", "duplicate", "delay", "reorder", "mutate", "schema_drift")

SCHEMA = """
CREATE TABLE IF NOT EXISTS fault (
    id INTEGER PRIMARY KEY,
    topic TEXT NOT NULL,
    trip_key TEXT NOT NULL,
    mode TEXT NOT NULL,
    delay_seconds REAL NOT NULL,
    source_partition INTEGER,
    source_offset INTEGER,
    source_lsn INTEGER,
    injected_at REAL,
    released_at REAL,
    expected TEXT NOT NULL,
    observed TEXT,
    observed_at REAL
);
CREATE TABLE IF NOT EXISTS received (
    topic TEXT NOT NULL,
    partition INTEGER NOT NULL,
    offset INTEGER NOT NULL,
    PRIMARY KEY (topic, partition, offset)
);
CREATE TABLE IF NOT EXISTS pending (
    id INTEGER PRIMARY KEY,
    topic TEXT NOT NULL,
    partition INTEGER NOT NULL,
    offset INTEGER NOT NULL,
    trip_key TEXT NOT NULL,
    key BLOB NOT NULL,
    value BLOB NOT NULL,
    due REAL,
    held_for INTEGER REFERENCES fault(id),
    fault_id INTEGER REFERENCES fault(id),
    done_at REAL
);
CREATE INDEX IF NOT EXISTS pending_due ON pending (done_at, due);
"""


def open_ledger(path):
    conn = sqlite3.connect(path, timeout=20)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=FULL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.executescript(SCHEMA)
    return conn


def arm(conn, topic, key, mode, expected, delay_seconds=1200):
    if mode not in MODES or delay_seconds < 0:
        raise ValueError("Invalid fault mode or delay")
    with conn:
        return conn.execute(
            """INSERT INTO fault (topic, trip_key, mode, delay_seconds, expected)
               VALUES (?, ?, ?, ?, ?)""",
            (topic, key, mode, delay_seconds, expected),
        ).lastrowid


def stage(conn, event, rewrite, now=None):
    """Persist a delivery decision before allowing the caller to commit Kafka input."""
    now = time.time() if now is None else now
    topic, partition, offset, key, raw_key, raw_value, lsn = event
    with conn:
        inserted = conn.execute(
            "INSERT OR IGNORE INTO received VALUES (?, ?, ?)", (topic, partition, offset),
        )
        if not inserted.rowcount:
            return
        # On a later version of this key, place the held version after it.
        held = conn.execute(
            """SELECT id, fault_id FROM pending
               WHERE topic=? AND trip_key=? AND held_for IS NOT NULL ORDER BY id""",
            (topic, key),
        ).fetchall()
        fault = conn.execute(
            """SELECT * FROM fault WHERE topic=? AND trip_key=? AND injected_at IS NULL
               ORDER BY id LIMIT 1""",
            (topic, key),
        ).fetchone()
        # The successor of a reordered event must travel normally.
        if held:
            fault = None
        copies, due, hold, value = 1, now, None, raw_value
        fault_id = None
        if fault:
            fault_id, mode = fault["id"], fault["mode"]
            conn.execute(
                """UPDATE fault SET source_partition=?, source_offset=?, source_lsn=?,
                   injected_at=? WHERE id=?""",
                (partition, offset, lsn, now, fault_id),
            )
            if mode == "drop":
                copies = 0
            elif mode == "duplicate":
                copies = 2
            elif mode == "delay":
                due += fault["delay_seconds"]
            elif mode == "reorder":
                due, hold = None, fault_id
            else:
                value = rewrite(mode, raw_value)
        for _ in range(copies):
            conn.execute(
                """INSERT INTO pending (topic, partition, offset, trip_key, key, value,
                                        due, held_for, fault_id)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (topic, partition, offset, key, raw_key, value, due, hold, fault_id),
            )
        for row in held:
            conn.execute(
                "UPDATE pending SET held_for=NULL, due=? WHERE id=?",
                (now + 0.000001, row["id"]),
            )


def pending(conn, now=None, limit=500):
    now = time.time() if now is None else now
    return conn.execute(
        """SELECT * FROM pending WHERE done_at IS NULL AND due <= ?
           ORDER BY due, id LIMIT ?""", (now, limit),
    ).fetchall()


def delivered(conn, row, now=None):
    now = time.time() if now is None else now
    with conn:
        conn.execute("UPDATE pending SET done_at=? WHERE id=?", (now, row["id"]))
        fault = row["fault_id"]
        if fault is not None:
            left = conn.execute(
                "SELECT 1 FROM pending WHERE fault_id=? AND done_at IS NULL", (fault,),
            ).fetchone()
            if left is None:
                conn.execute("UPDATE fault SET released_at=? WHERE id=?", (now, fault))
