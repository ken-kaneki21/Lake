"""Load public TLC zone references and generate deterministic synthetic drivers."""

import argparse
import csv
import random
from decimal import Decimal
from pathlib import Path


def drivers(seed, count):
    rng = random.Random(seed)
    for driver_id in range(1, count + 1):
        yield (
            driver_id,
            Decimal(rng.randint(300, 500)).scaleb(-2),
            rng.choice(("sedan", "hybrid", "van")),
            "New York",
        )


def main():
    import psycopg

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--zones", required=True, type=Path)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--drivers", type=int, default=2000)
    args = parser.parse_args()
    if args.drivers < 1:
        parser.error("--drivers must be positive")
    with args.zones.open(encoding="utf-8-sig", newline="") as file:
        zones = [
            (int(row["LocationID"]), row["Borough"], row["Zone"], row["service_zone"])
            for row in csv.DictReader(file)
        ]
    if not zones:
        parser.error("zone lookup is empty")
    with psycopg.connect() as conn, conn.cursor() as cursor:
        cursor.executemany(
            """INSERT INTO app.zones (zone_id, borough, zone, service_zone)
               VALUES (%s, %s, %s, %s) ON CONFLICT (zone_id) DO NOTHING""",
            zones,
        )
        cursor.executemany(
            """INSERT INTO app.drivers (driver_id, rating, vehicle_type, city)
               VALUES (%s, %s, %s, %s) ON CONFLICT (driver_id) DO NOTHING""",
            drivers(args.seed, args.drivers),
        )
    print(f"seed committed: {len(zones)} supplied zones, {args.drivers} synthetic drivers")


if __name__ == "__main__":
    main()
