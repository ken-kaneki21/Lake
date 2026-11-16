"""Compare settled PostgreSQL pickup windows with a pinned Iceberg snapshot."""

import argparse
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from uuid import uuid4

SQL_DIR = Path(__file__).parent
WINDOW = timedelta(minutes=5)
SETTLE = timedelta(minutes=10)
HISTORY = timedelta(hours=24)
BUCKETS = {f"{index:02x}" for index in range(256)}
METRICS = ("row_count", "fare_cents", "tip_cents", "deleted_count", "fingerprint_sum")


def settled_windows(cutoff, history=HISTORY):
    if cutoff.utcoffset() is None:
        raise ValueError("cutoff must be timezone-aware")
    end = (cutoff.astimezone(timezone.utc) - SETTLE).replace(second=0, microsecond=0)
    end -= timedelta(minutes=end.minute % 5)
    if history <= timedelta(0) or history % WINDOW:
        raise ValueError("history must be a positive multiple of five minutes")
    start = end - history
    while start < end:
        yield start, start + WINDOW
        start += WINDOW


def bucket_map(rows):
    result = {}
    for row in rows:
        name, *values = row
        if name in result or len(values) != len(METRICS):
            raise ValueError(f"Invalid bucket row: {row!r}")
        result[name] = dict(zip(METRICS, map(int, values)))
    if set(result) != BUCKETS:
        raise ValueError("Expected exactly 256 distinct canonical buckets")
    return result


def key_differences(source_rows, lake_rows):
    def index(rows):
        result = {}
        for key, pickup, fare, tip, deleted in rows:
            if key in result:
                raise ValueError(f"Duplicate trip id in drill-down: {key}")
            result[key] = (pickup, fare, tip, deleted)
        return result

    source, lake = index(source_rows), index(lake_rows)
    for key in sorted(source.keys() | lake.keys()):
        expected, observed = source.get(key), lake.get(key)
        if expected == observed:
            continue
        if expected is None:
            category = "extra"
        elif observed is None:
            category = "missing"
        elif expected[3] != observed[3]:
            category = "delete_state"
        else:
            category = "changed"
        yield {"trip_id": key, "category": category,
               "source": expected, "lake": observed}


def reconcile(conn, spark, cutoff, history=HISTORY):
    run_id = uuid4()
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    refs = spark.sql(
        "SELECT snapshot_id FROM lake.silver.trips.refs WHERE name = 'main'"
    ).collect()
    if len(refs) != 1:
        raise RuntimeError("Expected one current silver.trips snapshot")
    snapshot = int(refs[0]["snapshot_id"])
    current_table = f"lake.silver.trips VERSION AS OF {snapshot}"
    source_buckets = (SQL_DIR / "source_buckets.sql").read_text()
    lake_buckets = (SQL_DIR / "lake_buckets.sql").read_text().replace(
        "FROM lake.silver.trips", f"FROM {current_table}"
    )
    pg_keys = """
        SELECT trip_id, pickup_ts, fare_amount, tip_amount, is_deleted
        FROM source_audit.trip_state
        WHERE pickup_ts >= %(window_start)s AND pickup_ts < %(window_end)s
          AND substring(md5(trip_id::text) for 2) = ANY(%(buckets)s)
    """
    lake_keys = f"""
        SELECT trip_id, pickup_ts, fare_amount, tip_amount, is_deleted
        FROM {current_table}
        WHERE pickup_ts >= :window_start AND pickup_ts < :window_end
          AND substring(md5(CAST(trip_id AS STRING)), 1, 2) IN :buckets
    """
    windows = mismatch_count = 0
    with conn.transaction():
        conn.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
        conn.execute("SET LOCAL TIME ZONE 'UTC'")
        from psycopg.types.json import Jsonb

        conn.execute(
            "INSERT INTO recon.runs (run_id, scheduled_at, snapshot_id) VALUES (%s, %s, %s)",
            (run_id, cutoff, snapshot),
        )
        for start, end in settled_windows(cutoff, history):
            params = {"window_start": start, "window_end": end}
            source = bucket_map(conn.execute(source_buckets, params))
            lake = bucket_map(spark.sql(lake_buckets, args=params).collect())
            mismatches = sorted(bucket for bucket in BUCKETS if source[bucket] != lake[bucket])
            conn.execute(
                """INSERT INTO recon.windows
                   (run_id, window_start, window_end, source_buckets, lake_buckets, status)
                   VALUES (%s, %s, %s, %s, %s, %s)""",
                (run_id, start, end, Jsonb(source), Jsonb(lake), "drift" if mismatches else "match"),
            )
            windows += 1
            if not mismatches:
                continue
            mismatch_count += 1
            placeholders = ", ".join(f":b{index}" for index in range(len(mismatches)))
            key_params = params | {f"b{index}": item for index, item in enumerate(mismatches)}
            pg_rows = conn.execute(pg_keys, params | {"buckets": mismatches}).fetchall()
            sp_rows = spark.sql(lake_keys.replace(":buckets", f"({placeholders})"), args=key_params)
            # Spark collects TimestampType values in the driver process local timezone.
            normalized = [
                (row[0], datetime.fromtimestamp(row[1].timestamp(), timezone.utc), *row[2:])
                for row in sp_rows.collect()
            ]
            differences = list(key_differences(pg_rows, normalized))
            evidence = json.loads(json.dumps(
                {"buckets": mismatches, "keys": differences}, default=str,
            ))
            conn.execute(
                """INSERT INTO recon.alerts (run_id, window_start, evidence)
                   VALUES (%s, %s, %s)""",
                (run_id, start, Jsonb(evidence)),
            )
    return {"run_id": str(run_id), "snapshot_id": snapshot, "windows": windows,
            "drift_windows": mismatch_count}


def main():
    import psycopg
    from pyspark.sql import SparkSession

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cutoff", type=datetime.fromisoformat)
    args = parser.parse_args()
    spark = SparkSession.builder.appName("ParityLake reconciliation").getOrCreate()
    try:
        with psycopg.connect(autocommit=True) as conn:
            result = reconcile(conn, spark, args.cutoff or datetime.now(timezone.utc))
        print(json.dumps(result))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
