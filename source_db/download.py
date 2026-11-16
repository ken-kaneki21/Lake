"""Fetch one public NYC TLC Yellow Taxi month and the official zone lookup."""

import argparse
import os
import shutil
from datetime import datetime
from pathlib import Path
from tempfile import NamedTemporaryFile
from urllib.request import urlopen

BASE = "https://d37ci6vzurychx.cloudfront.net"


def download(url, destination):
    if destination.is_file():
        return
    with NamedTemporaryFile(dir=destination.parent, delete=False) as temp:
        partial = Path(temp.name)
        try:
            with urlopen(url, timeout=60) as response:
                shutil.copyfileobj(response, temp)
            temp.close()
            os.replace(partial, destination)
        finally:
            temp.close()
            partial.unlink(missing_ok=True)


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
    filename = f"yellow_tripdata_{args.month}.parquet"
    download(f"{BASE}/trip-data/{filename}", args.output / filename)
    download(f"{BASE}/misc/taxi_zone_lookup.csv", args.output / "taxi_zone_lookup.csv")


if __name__ == "__main__":
    main()
