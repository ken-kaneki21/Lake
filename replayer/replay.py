"""Replay public TLC records through committed PostgreSQL writes."""

import argparse
import json
import random
import time
from collections import Counter
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from zoneinfo import ZoneInfo

COLUMNS = (
    "tpep_pickup_datetime", "tpep_dropoff_datetime", "PULocationID", "DOLocationID",
    "trip_distance", "fare_amount", "tip_amount", "total_amount",
)


def new_york_timestamp(value):
    if value.tzinfo is None:
        value = value.replace(tzinfo=ZoneInfo("America/New_York"))
    return value.astimezone(timezone.utc)


def amounts(row):
    return tuple(
        Decimal(str(row[column])).quantize(Decimal("0.01")) for column in COLUMNS[4:]
    )


def main():
    import psycopg
    from pyarrow.parquet import ParquetFile

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--month", default="2024-01")
    parser.add_argument("--input-dir", type=Path, default=Path("data"))
    parser.add_argument("--rate", type=float, default=200)
    parser.add_argument("--speedup", type=float, default=60)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--limit", type=int)
    parser.add_argument("--anchor-utc", type=datetime.fromisoformat)
    args = parser.parse_args()
    try:
        month = datetime.strptime(args.month, "%Y-%m")
        if month.strftime("%Y-%m") != args.month:
            raise ValueError
    except ValueError:
        parser.error("--month requires YYYY-MM")
    if args.rate <= 0 or args.speedup <= 0 or (args.limit is not None and args.limit < 1):
        parser.error("rate, speedup and limit must be positive")
    if args.anchor_utc and args.anchor_utc.utcoffset() is None:
        parser.error("--anchor-utc requires an explicit offset")
    rng = random.Random(args.seed)
    rows = ParquetFile(args.input_dir / f"yellow_tripdata_{args.month}.parquet")
    counts = Counter()
    recent = []
    started = datetime.now(timezone.utc)
    first_pickup = None
    clock = time.monotonic()
    last_commit = clock
    with psycopg.connect(autocommit=True) as conn:
        driver_ids = [row[0] for row in conn.execute("SELECT driver_id FROM app.drivers ORDER BY 1")]
        zones = {row[0] for row in conn.execute("SELECT zone_id FROM app.zones")}
        if not driver_ids or not zones:
            raise RuntimeError("Seed drivers and zones before replay")
        for batch in rows.iter_batches(batch_size=2048, columns=list(COLUMNS)):
            for row in batch.to_pylist():
                if row[COLUMNS[0]] is None or row[COLUMNS[0]].strftime("%Y-%m") != args.month:
                    counts["excluded_input"] += 1
                    continue
                if any(row[column] is None for column in COLUMNS) or not {
                    row[COLUMNS[2]], row[COLUMNS[3]]
                }.issubset(zones):
                    counts["excluded_input"] += 1
                    continue
                pickup, dropoff = map(new_york_timestamp, (row[COLUMNS[0]], row[COLUMNS[1]]))
                if first_pickup is None:
                    first_pickup = pickup
                due = clock + max(0, (pickup - first_pickup).total_seconds() / args.speedup)
                time.sleep(max(0, due - time.monotonic(), last_commit + 1 / args.rate - time.monotonic()))
                if args.anchor_utc:
                    duration = dropoff - pickup
                    pickup = args.anchor_utc + timedelta(seconds=(pickup - first_pickup).total_seconds() / args.speedup)
                    dropoff = pickup + duration
                draw = rng.randrange(100)
                with conn.transaction():
                    if draw < 93 or not recent:
                        op = "insert"
                        result = conn.execute(
                            """INSERT INTO app.trips (
                                driver_id, pickup_ts, dropoff_ts, pu_zone_id, do_zone_id,
                                trip_distance, fare_amount, tip_amount, total_amount
                            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING trip_id""",
                            (rng.choice(driver_ids), pickup, dropoff, row[COLUMNS[2]],
                             row[COLUMNS[3]], *amounts(row)),
                        )
                        recent.append(result.fetchone()[0])
                        if len(recent) > 4096:
                            recent.pop(0)
                    elif draw < 98:
                        op = "update"
                        conn.execute(
                            """UPDATE app.trips SET fare_amount = fare_amount + .25,
                               total_amount = total_amount + .25, status = 'adjusted',
                               updated_at = now() WHERE trip_id = %s""", (rng.choice(recent),),
                        )
                    elif draw < 99:
                        op = "delete"
                        conn.execute("DELETE FROM app.trips WHERE trip_id = %s",
                                     (recent.pop(rng.randrange(len(recent))),))
                    else:
                        op = "driver_update"
                        conn.execute(
                            """UPDATE app.drivers SET rating = %s, updated_at = now()
                               WHERE driver_id = %s""",
                            (Decimal(rng.randint(300, 500)).scaleb(-2), rng.choice(driver_ids)),
                        )
                last_commit = time.monotonic()
                counts[op] += 1
                if args.limit and sum(counts[op] for op in ("insert", "update", "delete", "driver_update")) >= args.limit:
                    break
            else:
                continue
            break
    committed = {op: counts[op] for op in ("insert", "update", "delete", "driver_update")}
    total = sum(committed.values())
    print(json.dumps(dict(
        month=args.month, seed=args.seed, target_rate=args.rate, speedup=args.speedup,
        started_utc=started.isoformat(), ended_utc=datetime.now(timezone.utc).isoformat(),
        elapsed_seconds=time.monotonic() - clock, committed_transactions=total,
        committed_by_operation=committed, excluded_input=counts["excluded_input"],
        operation_share={op: count / total for op, count in committed.items()} if total else {},
    ), sort_keys=True))


if __name__ == "__main__":
    main()
