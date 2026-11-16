"""Container probes for the Windows validation runner."""

import argparse
import json
import time
from datetime import datetime, timezone
from pathlib import Path

import psycopg

PICKUP = "2024-01-01 00:03:00+00"
CUTOFF = datetime(2024, 1, 1, 0, 15, tzinfo=timezone.utc)


def source(args):
    start = time.monotonic()
    with psycopg.connect() as conn:
        if args.action == "insert":
            conn.execute("""
                INSERT INTO app.trips (trip_id, pickup_ts, fare_amount, tip_amount, total_amount)
                SELECT id, %s, 10, 1, 11 FROM generate_series(%s, %s) AS id
            """, (PICKUP, args.key, args.key + args.count - 1))
        elif args.action == "evolve":
            conn.execute("ALTER TABLE app.trips ADD COLUMN paritylake_optional text")
        elif args.action == "delete":
            assert conn.execute("DELETE FROM app.trips WHERE trip_id=%s", (args.key,)).rowcount == 1
        else:
            assert conn.execute("""
                UPDATE app.trips SET fare_amount=fare_amount+1,
                    total_amount=total_amount+1, updated_at=clock_timestamp()
                WHERE trip_id=%s
            """, (args.key,)).rowcount == 1
    print(json.dumps({"action": args.action, "key": args.key, "count": args.count,
                      "commit_observed_utc": datetime.now(timezone.utc).isoformat(),
                      "transaction_seconds": time.monotonic() - start}))


def ledger(args):
    from chaos.ledger import open_ledger

    conn = open_ledger("/var/lib/paritylake/relay.db")
    try:
        row = conn.execute("SELECT * FROM fault WHERE id=?", (args.fault,)).fetchone()
        if row is None:
            raise ValueError(f"Fault {args.fault} not found")
        if args.release:
            with conn:
                changed = conn.execute(
                    "UPDATE pending SET due=? WHERE fault_id=? AND done_at IS NULL AND held_for IS NULL",
                    (time.time(), args.fault),
                ).rowcount
                if row["mode"] != "delay" or changed != 1:
                    raise RuntimeError(f"Expected exactly one delayed outbox record, got {changed}")
        deliveries = conn.execute(
            "SELECT id, offset, due, done_at FROM pending WHERE fault_id=? ORDER BY id",
            (args.fault,),
        ).fetchall()
        print(json.dumps({"fault": dict(row), "deliveries": [dict(item) for item in deliveries]}))
    finally:
        conn.close()


def offsets(args):
    from confluent_kafka.admin import AdminClient, OffsetSpec
    from confluent_kafka import TopicPartition

    admin = AdminClient({"bootstrap.servers": "kafka:29092"})
    request = {TopicPartition(name, 0): OffsetSpec.latest()
               for name in ("pg.app.trips", "relayed.app.trips")}
    print(json.dumps({key.topic: value.result(timeout=12).offset
                      for key, value in admin.list_offsets(request, request_timeout=10).items()}))


def lake(args):
    from pyspark.sql import SparkSession
    from recon.reconcile import WINDOW, reconcile

    spark = SparkSession.builder.appName("ParityLake Windows evidence").getOrCreate()
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    try:
        with psycopg.connect(autocommit=True) as conn:
            conn.execute(Path("recon/schema.sql").read_text())
            comparison = reconcile(conn, spark, CUTOFF, history=WINDOW)
            alerts = conn.execute("SELECT evidence FROM recon.alerts WHERE run_id=%s",
                                  (comparison["run_id"],)).fetchall()
            source_count = conn.execute(
                "SELECT count(*) FROM source_audit.trip_state WHERE trip_id BETWEEN %s AND %s",
                (args.key, args.key + args.count - 1),
            ).fetchone()[0]
        params = {"lo": args.key, "hi": args.key + args.count - 1}
        silver = spark.sql("""
            SELECT trip_id, fare_amount, is_deleted,
                   (unix_millis(silver_commit_ts) - source_ts_ms) / 1000.0 AS latency
            FROM lake.silver.trips WHERE trip_id BETWEEN :lo AND :hi
        """, args=params).collect()
        bronze = spark.sql("""
            SELECT CAST(get_json_object(record_json, '$.trip_id') AS BIGINT) AS id,
                   op, offset, source_lsn, transaction_order, schema_id
            FROM lake.bronze.cdc_events
            WHERE entity='trips' AND CAST(get_json_object(record_json, '$.trip_id') AS BIGINT)
                BETWEEN :lo AND :hi ORDER BY partition, offset
        """, args=params).collect()
        duplicates = spark.sql("""
            SELECT count(*) - count(DISTINCT struct(topic, partition, offset))
            FROM lake.bronze.cdc_events
        """).first()[0]
        result = {
            "comparison": comparison, "alerts": [item[0] for item in alerts],
            "source_count": source_count, "silver_count": len(silver),
            "silver_unique": len({item.trip_id for item in silver}),
            "duplicate_bronze_offsets": duplicates,
            "silver": [item.asDict() for item in silver] if args.count <= 100 else [],
            "bronze": [item.asDict() for item in bronze] if args.count <= 100 else [],
            "bronze_count": len(bronze),
            "latency_seconds": [float(item.latency) for item in silver if item.latency is not None],
        }
        print(json.dumps(result, default=str))
    finally:
        spark.stop()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    for name in ("source", "lake"):
        command = sub.add_parser(name)
        command.add_argument("key", type=int)
        command.add_argument("--count", type=int, default=1)
        if name == "source":
            command.add_argument("action", choices=["insert", "update", "delete", "evolve"])
    command = sub.add_parser("ledger")
    command.add_argument("fault", type=int)
    command.add_argument("--release", action="store_true")
    sub.add_parser("offsets")
    args = parser.parse_args()
    {"source": source, "lake": lake, "ledger": ledger, "offsets": offsets}[args.command](args)


if __name__ == "__main__":
    main()
