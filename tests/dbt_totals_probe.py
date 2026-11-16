"""Compare built dbt totals with the live PostgreSQL source."""

import json

import psycopg
from pyspark.sql import SparkSession


def main():
    with psycopg.connect() as conn:
        expected = conn.execute("""
            SELECT count(*), sum(fare_amount), sum(tip_amount), sum(total_amount)
            FROM app.trips
        """).fetchone()
    if expected[0] == 0:
        raise AssertionError("PostgreSQL source fixture is empty")

    spark = SparkSession.builder.appName("ParityLake dbt totals probe").getOrCreate()
    try:
        fact = tuple(spark.sql("""
            SELECT count(*), sum(fare_amount), sum(tip_amount), sum(total_amount)
            FROM lake.gold.fct_trips
        """).first())
        mart = tuple(spark.sql("""
            SELECT sum(trips), sum(fare_amount), sum(tip_amount), sum(revenue)
            FROM lake.gold.mart_revenue_zone_day
        """).first())
        metrics = ("trip_count", "fare", "tip", "revenue")
        totals = {name: dict(zip(metrics, values)) for name, values in (
            ("postgres", expected), ("dbt_fact", fact), ("dbt_mart", mart)
        )}
        print(json.dumps(totals, default=str))
        assert fact == expected, f"dbt fact differs from PostgreSQL: {fact} != {expected}"
        assert mart == expected, f"dbt mart differs from PostgreSQL: {mart} != {expected}"
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
