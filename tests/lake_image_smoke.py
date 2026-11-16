"""Exercise the lake image's Python packages with a real local Spark worker."""

import io
import json
import platform
import sys
from datetime import datetime
from decimal import Decimal
from importlib.metadata import version

from fastavro import parse_schema, schemaless_reader, schemaless_writer
from psycopg import pq
from pyspark.sql import SparkSession

from streaming.consume import quality


def main():
    assert sys.version_info[:2] == (3, 11), sys.version
    assert pq.__impl__ == "binary", pq.__impl__
    schema = parse_schema({
        "type": "record",
        "name": "Fare",
        "fields": [{
            "name": "amount",
            "type": {"type": "bytes", "logicalType": "decimal", "precision": 8, "scale": 2},
        }],
    })
    data = io.BytesIO()
    schemaless_writer(data, schema, {"amount": Decimal("12.34")})
    data.seek(0)
    assert schemaless_reader(data, schema)["amount"] == Decimal("12.34")

    spark = SparkSession.builder.appName("paritylake-image-smoke").getOrCreate()
    try:
        assert spark.version == "3.5.5", spark.version
        worker = spark.sparkContext.parallelize([None], 1).map(
            lambda _: (platform.python_version(), sys.executable)
        ).first()
        assert worker == (platform.python_version(), sys.executable), worker
        rows = spark.createDataFrame(
            [
                (1, datetime(2024, 1, 1), Decimal("12.34"), Decimal("2.00"), Decimal("14.34"), "completed"),
                (2, datetime(2024, 1, 1), Decimal("12.34"), Decimal("2.00"), Decimal("14.34"), "bad"),
            ],
            "trip_id long, pickup_ts timestamp, fare_amount decimal(8,2), "
            "tip_amount decimal(8,2), total_amount decimal(8,2), status string",
        ).selectExpr(
            "*", "cast(NULL as bigint) as driver_id", "cast(NULL as timestamp) as dropoff_ts",
            "cast(NULL as int) as pu_zone_id", "cast(NULL as int) as do_zone_id",
            "cast(NULL as decimal(8,2)) as trip_distance", "pickup_ts as updated_at",
        ).orderBy("trip_id")
        assert quality(rows, "trips", "trip_id") == {1: ["allowed_trip_status"]}
        print(json.dumps({
            "python": platform.python_version(), "architecture": platform.machine(),
            "spark": spark.version,
            "packages": {name: version(name) for name in (
                "fastavro", "psycopg", "psycopg-binary", "great-expectations",
            )},
        }))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
