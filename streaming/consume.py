"""Stream Confluent Avro CDC through Iceberg bronze, quality and silver."""

import argparse
import io
import json
import re
from urllib.request import urlopen

from streaming.source_version import SourceVersion

TABLES = {
    "trips": {
        "trip_id": "BIGINT", "driver_id": "BIGINT", "pickup_ts": "TIMESTAMP",
        "dropoff_ts": "TIMESTAMP", "pu_zone_id": "INT", "do_zone_id": "INT",
        "trip_distance": "DECIMAL(8,2)", "fare_amount": "DECIMAL(10,2)",
        "tip_amount": "DECIMAL(10,2)", "total_amount": "DECIMAL(10,2)",
        "status": "STRING", "updated_at": "TIMESTAMP",
    },
    "drivers": {
        "driver_id": "BIGINT", "rating": "DECIMAL(3,2)", "vehicle_type": "STRING",
        "city": "STRING", "updated_at": "TIMESTAMP",
    },
    "zones": {"zone_id": "INT", "borough": "STRING", "zone": "STRING",
              "service_zone": "STRING"},
}
BRONZE = """
    topic STRING, partition INT, offset BIGINT, kafka_ts TIMESTAMP, payload BINARY,
    schema_id INT, entity STRING, op STRING, source_ts_ms BIGINT, source_lsn BIGINT,
    is_wal INT, transaction_order BIGINT, before_json STRING, after_json STRING,
    record_json STRING, error STRING
"""


def decode_partition(records, registry):
    from fastavro import parse_schema, schemaless_reader

    schemas = {}
    for row in records:
        topic, partition, offset, kafka_ts, payload = row
        schema_id = None
        try:
            if not payload or len(payload) < 6 or payload[0] != 0:
                raise ValueError("Expected Confluent Avro framing")
            schema_id = int.from_bytes(payload[1:5], "big")
            if schema_id not in schemas:
                # Transport or Registry failure aborts the batch for a retry.
                with urlopen(f"{registry}/schemas/ids/{schema_id}", timeout=10) as response:
                    schemas[schema_id] = parse_schema(json.loads(json.load(response)["schema"]))
            event = schemaless_reader(io.BytesIO(payload[5:]), schemas[schema_id])
            entity = event["source"]["table"]
            fields = TABLES[entity]
            operation = event["op"]
            version = SourceVersion.from_envelope(event)
            record = event["before"] if operation == "d" else event["after"]
            if not isinstance(record, dict) or set(fields) - record.keys():
                raise ValueError("Required CDC fields are missing")

            def encode(value):
                return None if value is None else json.dumps(value, default=str)

            metadata = (
                schema_id, entity, operation, event["source"]["ts_ms"],
                event["source"]["lsn"], version.is_wal, version.transaction_order,
                encode(event["before"]), encode(event["after"]), encode(record), None,
            )
        except (ValueError, TypeError, KeyError, EOFError) as error:
            metadata = (schema_id, *([None] * 9), str(error))
        yield (topic, partition, offset, kafka_ts, payload, *metadata)


def initialize(spark):
    for namespace in ("bronze", "silver", "quarantine", "gold"):
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS lake.{namespace}")
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS lake.bronze.cdc_events ({BRONZE}, ingest_ts TIMESTAMP)
        USING iceberg PARTITIONED BY (days(ingest_ts))
    """)
    spark.sql("""
        CREATE TABLE IF NOT EXISTS lake.quarantine.cdc_events (
            topic STRING, partition INT, offset BIGINT, entity STRING,
            expectation STRING, payload BINARY, rejected_at TIMESTAMP
        ) USING iceberg
    """)
    for entity, fields in TABLES.items():
        column_sql = ", ".join(f"{name} {kind}" for name, kind in fields.items())
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS lake.silver.{entity} (
                {column_sql}, is_deleted BOOLEAN, is_wal INT, source_lsn BIGINT,
                transaction_order BIGINT, source_ts_ms BIGINT, source_topic STRING,
                source_partition INT, source_offset BIGINT, silver_commit_ts TIMESTAMP
            ) USING iceberg
        """)


def merge_offsets(spark, table, rows):
    rows.createOrReplaceTempView("paritylake_offset_rows")
    spark.sql(f"""
        MERGE INTO {table} t USING paritylake_offset_rows s
        ON t.topic = s.topic AND t.partition = s.partition AND t.offset = s.offset
        WHEN NOT MATCHED THEN INSERT *
    """)


def quality(rows, entity, key):
    from great_expectations.dataset import PandasDataset

    columns = list(TABLES[entity])
    data = PandasDataset(rows.select(*columns).toPandas())
    expectations = [(f"{key}_required", data.expect_column_values_to_not_be_null, (key,))]
    if entity == "trips":
        for field in ("pickup_ts", "fare_amount", "tip_amount", "total_amount"):
            expectations.append((f"{field}_required", data.expect_column_values_to_not_be_null, (field,)))
        expectations.append((
            "allowed_trip_status", data.expect_column_values_to_be_in_set,
            ("status", ["completed", "adjusted"]),
        ))
        expectations.append((
            "nonnegative_tip", data.expect_column_values_to_be_between, ("tip_amount", 0),
        ))
    issues = {}
    for name, expect, args in expectations:
        result = expect(*args, result_format="COMPLETE")
        for index in result.result.get("unexpected_index_list", []):
            issues.setdefault(int(index), []).append(name)
    return issues


def write_batch(spark, batch, registry):
    from pyspark.sql import Window, functions as F

    batch = batch.select("topic", "partition", "offset", "timestamp", "value")
    decoded = spark.createDataFrame(
        batch.rdd.mapPartitions(lambda rows: decode_partition(rows, registry)), BRONZE,
    ).persist()
    try:
        if decoded.isEmpty():
            return
        merge_offsets(spark, "lake.bronze.cdc_events", decoded.withColumn("ingest_ts", F.current_timestamp()))
        rejections = decoded.where("error IS NOT NULL").selectExpr(
            "topic", "partition", "offset", "entity", "error AS expectation", "payload",
            "current_timestamp() AS rejected_at",
        )
        for entity, fields in TABLES.items():
            key = next(iter(fields))
            typed = decoded.where((F.col("entity") == entity) & F.col("error").isNull())
            for name, kind in fields.items():
                typed = typed.withColumn(
                    name, F.expr(f"try_cast(get_json_object(record_json, '$.{name}') AS {kind})"),
                )
            # Stable index shared by Spark and GX for this bounded microbatch.
            index = Window.orderBy("topic", "partition", "offset")
            typed = typed.withColumn("_index", F.row_number().over(index) - 1)
            issues = quality(typed.orderBy("_index"), entity, key)
            if issues:
                rejected = spark.createDataFrame(
                    [(idx, ",".join(errors)) for idx, errors in issues.items()],
                    "_index INT, expectation STRING",
                )
                rejections = rejections.unionByName(
                    typed.join(rejected, "_index").selectExpr(
                        "topic", "partition", "offset", "entity", "expectation", "payload",
                        "current_timestamp() AS rejected_at",
                    )
                )
                typed = typed.join(rejected, "_index", "left_anti")
            by_key = Window.partitionBy(key).orderBy(
                F.desc("is_wal"), F.desc("source_lsn"), F.desc("transaction_order"), F.desc("offset"),
            )
            current = typed.withColumn("_latest", F.row_number().over(by_key)).where("_latest = 1")
            current.selectExpr(
                *fields, "op = 'd' AS is_deleted", "is_wal", "CASE WHEN is_wal = 0 THEN 0 ELSE source_lsn END AS source_lsn",
                "transaction_order", "source_ts_ms", "topic AS source_topic",
                "partition AS source_partition", "offset AS source_offset",
                "current_timestamp() AS silver_commit_ts",
            ).createOrReplaceTempView("paritylake_silver_rows")
            spark.sql(f"""
                MERGE INTO lake.silver.{entity} t USING paritylake_silver_rows s ON t.{key}=s.{key}
                WHEN MATCHED AND (
                    s.is_wal > t.is_wal OR (s.is_wal = t.is_wal AND (
                        s.source_lsn > t.source_lsn OR (s.source_lsn = t.source_lsn
                        AND s.transaction_order > t.transaction_order)
                    ))
                ) THEN UPDATE SET *
                WHEN NOT MATCHED THEN INSERT *
            """)
        merge_offsets(spark, "lake.quarantine.cdc_events", rejections)
    finally:
        decoded.unpersist()


def main():
    from pyspark.sql import SparkSession

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--prefix", default="pg")
    parser.add_argument("--checkpoint", required=True)
    parser.add_argument("--available-now", action="store_true")
    args = parser.parse_args()
    if not re.fullmatch(r"[a-z0-9_-]+", args.prefix):
        parser.error("--prefix must be a single Kafka topic prefix")
    spark = SparkSession.builder.appName("ParityLake CDC").getOrCreate()
    try:
        initialize(spark)
        events = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "kafka:29092")
        topics = ",".join(f"{args.prefix}.app.{entity}" for entity in TABLES)
        events = (events.option("subscribe", topics).option("startingOffsets", "earliest")
                  .option("maxOffsetsPerTrigger", 2000).load())
        writer = events.writeStream.option("checkpointLocation", args.checkpoint).foreachBatch(
            lambda batch, _: write_batch(spark, batch, "http://schema-registry:8081")
        )
        if args.available_now:
            writer = writer.trigger(availableNow=True)
        else:
            writer = writer.trigger(processingTime="15 seconds")
        writer.start().awaitTermination()
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
