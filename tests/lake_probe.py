"""Assert the live CDC fixture after streaming commits to Iceberg."""

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path

import psycopg
from pyspark.sql import SparkSession

from recon.reconcile import WINDOW, reconcile


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("phase", choices=["baseline", "drift", "recovered"])
    phase = parser.parse_args().phase
    spark = SparkSession.builder.appName("ParityLake runtime probe").getOrCreate()
    try:
        bronze = spark.table("lake.bronze.cdc_events")
        bronze_count = bronze.count()
        wanted = 5 if phase == "recovered" else 4
        assert bronze_count == wanted, bronze_count
        assert bronze.select("topic", "partition", "offset").distinct().count() == wanted
        quarantined = spark.table("lake.quarantine.cdc_events").count()
        assert quarantined == 0, quarantined
        silver = spark.sql("""
            SELECT trip_id, is_deleted, CAST(fare_amount AS STRING) AS fare
            FROM lake.silver.trips ORDER BY trip_id
        """).collect()
        expected_fare = "8.00" if phase == "recovered" else "7.00"
        assert [tuple(row) for row in silver] == [
            (900001, True, "13.45"), (900002, False, expected_fare)
        ], silver

        cutoff = datetime(2024, 1, 1, 0, 15, tzinfo=timezone.utc)
        with psycopg.connect(autocommit=True) as conn:
            if phase == "baseline":
                conn.execute(Path("recon/schema.sql").read_text())
            if phase == "drift":
                # Hold the updated CDC event upstream by leaving the consumer stopped.
                conn.execute("""
                    UPDATE app.trips SET fare_amount=8.00, total_amount=8.00
                    WHERE trip_id=900002
                """)
            result = reconcile(conn, spark, cutoff, history=WINDOW)
            assert result["windows"] == 1, result
            assert result["drift_windows"] == int(phase == "drift"), result
            if phase == "drift":
                alert = conn.execute("""
                    SELECT evidence FROM recon.alerts WHERE run_id=%s
                """, (result["run_id"],)).fetchone()[0]
                assert len(alert["keys"]) == 1, alert
                assert alert["keys"][0]["trip_id"] == 900002, alert
                assert alert["keys"][0]["category"] == "changed", alert
                result["drift_evidence"] = alert
        print(json.dumps({"phase": phase, "bronze": bronze_count, "silver": len(silver),
                          "quarantined": quarantined, "reconciliation": result}))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
