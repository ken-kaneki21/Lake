"""Fetch and validate a public NYC TLC Yellow Taxi month and zone lookup."""

import argparse
import csv
import hashlib
import json
import os
import shutil
import time
from datetime import datetime
from pathlib import Path
from tempfile import NamedTemporaryFile
from urllib.error import HTTPError, URLError
from urllib.request import urlopen

from replayer.replay import COLUMNS

BASE = "https://d37ci6vzurychx.cloudfront.net"
ZONE_FIELDS = {"LocationID", "Borough", "Zone", "service_zone"}


def validate_trips(path):
    from pyarrow.parquet import ParquetFile

    with ParquetFile(path) as parquet:
        missing = set(COLUMNS) - set(parquet.schema_arrow.names)
        if missing or parquet.metadata.num_rows == 0:
            raise ValueError(f"Invalid TLC trip file. Missing columns: {sorted(missing)}")
        return parquet.metadata.num_rows


def validate_zones(path):
    with path.open(encoding="utf-8-sig", newline="") as stream:
        reader = csv.DictReader(stream)
        if not ZONE_FIELDS.issubset(reader.fieldnames or []):
            raise ValueError("Invalid TLC zone lookup header")
        ids = [int(row["LocationID"]) for row in reader]
    if not ids or len(ids) != len(set(ids)):
        raise ValueError("TLC zone lookup has empty or duplicate IDs")
    return len(ids)


def download(url, destination, validate):
    if destination.is_file():
        return validate(destination)
    for attempt in range(3):
        with NamedTemporaryFile(dir=destination.parent, delete=False) as temp:
            partial = Path(temp.name)
        try:
            with urlopen(url, timeout=30) as response, partial.open("wb") as stream:
                shutil.copyfileobj(response, stream)
                expected = response.headers.get("Content-Length")
            if expected and partial.stat().st_size != int(expected):
                raise OSError("Content-Length does not match the received file")
            rows = validate(partial)
            os.replace(partial, destination)
            return rows
        except (OSError, URLError, ValueError) as error:
            permanent = isinstance(error, HTTPError) and (
                400 <= error.code < 500 and error.code not in {408, 429}
            )
            if permanent or attempt == 2:
                raise RuntimeError(f"TLC download failed for {url}: {error}") from error
            time.sleep(2 ** attempt)
        finally:
            partial.unlink(missing_ok=True)


def describe(path, url, rows):
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return {"file": path.name, "url": url, "rows": rows,
            "bytes": path.stat().st_size, "sha256": digest.hexdigest()}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--month", default="2024-01")
    parser.add_argument("--output", type=Path, default=Path("data"))
    args = parser.parse_args()
    try:
        if datetime.strptime(args.month, "%Y-%m").strftime("%Y-%m") != args.month:
            raise ValueError
    except ValueError:
        parser.error("--month requires YYYY-MM")
    args.output.mkdir(parents=True, exist_ok=True)
    trips = f"yellow_tripdata_{args.month}.parquet"
    zones = "taxi_zone_lookup.csv"
    verified_files = []
    for name, url, validate in [
        (trips, f"{BASE}/trip-data/{trips}", validate_trips),
        (zones, f"{BASE}/misc/{zones}", validate_zones),
    ]:
        path = args.output / name
        try:
            rows = download(url, path, validate)
        except (OSError, RuntimeError, ValueError) as error:
            parser.exit(1, f"{error}\n")
        verified_files.append(describe(path, url, rows))
    print(json.dumps({"dataset": "NYC TLC Yellow Taxi", "month": args.month,
                      "files": verified_files}, sort_keys=True))


if __name__ == "__main__":
    main()
