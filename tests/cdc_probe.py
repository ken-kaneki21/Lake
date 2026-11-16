"""Exercise live PostgreSQL, Debezium, Kafka and Confluent Avro in Compose."""

import argparse
import io
import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.request import Request, urlopen

import psycopg
from confluent_kafka import Consumer, KafkaError
from fastavro import parse_schema, schemaless_reader
from psycopg import sql

from streaming.source_version import SourceVersion
from tests.connect_readiness import wait_for_connector

CONNECT = "http://connect:8083"
REGISTRY = "http://schema-registry:8081"
TOPIC = "pg.app.trips"
CONNECTOR = "pg-app"
FIXTURE = 900001


def get_json(url, value=None, method=None):
    data = None if value is None else json.dumps(value).encode()
    request = Request(url, data=data, headers={"Content-Type": "application/json"}, method=method)
    with urlopen(request, timeout=10) as response:
        return json.load(response)


def running_connector():
    return wait_for_connector(lambda: get_json(f"{CONNECT}/connectors/{CONNECTOR}/status"), CONNECTOR)


def register_connector():
    config = json.loads(Path("docker/connect/debezium-postgres.json").read_text())
    if config["name"] != CONNECTOR:
        raise ValueError("Connector fixture name does not match the status endpoint")
    expected = config["config"]["connector.class"]
    plugins = get_json(f"{CONNECT}/connector-plugins")
    plugin = next((item for item in plugins if item["class"] == expected), None)
    if plugin is None:
        raise RuntimeError(f"Kafka Connect did not discover {expected}")
    registered = get_json(f"{CONNECT}/connectors", config, "POST")
    if registered.get("name") != CONNECTOR:
        raise RuntimeError("Kafka Connect returned an unexpected connector name")
    return {"name": CONNECTOR, "plugin_class": expected, "plugin_version": plugin.get("version")}



def prepare_source(conn):
    conn.execute(Path("tests/source_audit.sql").read_text(encoding="utf-8"))
    assert conn.execute("SHOW wal_level").fetchone()[0] == "logical"
    conn.execute(sql.SQL("CREATE ROLE paritylake_cdc LOGIN REPLICATION PASSWORD {}").format(
        sql.Literal(os.environ["CDC_PGPASSWORD"])
    ))
    conn.execute("GRANT USAGE ON SCHEMA app TO paritylake_cdc")
    conn.execute("GRANT SELECT ON app.zones, app.drivers, app.trips TO paritylake_cdc")
    conn.execute(Path("source_db/publication.sql").read_text(encoding="utf-8"))
    conn.execute(
        """INSERT INTO app.trips (trip_id, pickup_ts, fare_amount, tip_amount, total_amount)
        VALUES (%s, '2024-01-01 00:00+00', 12.34, 2, 14.34)""",
        (FIXTURE,),
    )


def read_events(consumer, key, sequence):
    schemas = {}
    found = []
    last_metadata_error = None
    limit = time.monotonic() + 150
    while time.monotonic() < limit:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        error = msg.error()
        if error is not None:
            if not found and error.code() in {KafkaError.UNKNOWN_TOPIC_OR_PART, KafkaError._UNKNOWN_TOPIC}:
                last_metadata_error = error
                continue
            raise RuntimeError(error)
        value = msg.value()
        if not value or len(value) < 6 or value[0] != 0:
            raise ValueError("Expected a Confluent Avro envelope")
        schema_id = int.from_bytes(value[1:5], "big")
        if schema_id not in schemas:
            raw = get_json(f"{REGISTRY}/schemas/ids/{schema_id}")
            schemas[schema_id] = parse_schema(json.loads(raw["schema"]))
        decoded = schemaless_reader(io.BytesIO(value[5:]), schemas[schema_id])
        op = decoded["op"]
        row = decoded["before"] if op == "d" else decoded["after"]
        if row["trip_id"] != key:
            continue
        version = SourceVersion.from_envelope(decoded)
        found.append({
            "topic": msg.topic(),
            "partition": msg.partition(),
            "offset": msg.offset(),
            "op": op,
            "key": key,
            "schema_id": schema_id,
            "source_lsn": decoded["source"]["lsn"],
            "transaction_order": version.transaction_order,
            "before_fare": str(decoded["before"]["fare_amount"]) if decoded["before"] else None,
            "after_fare": str(decoded["after"]["fare_amount"]) if decoded["after"] else None,
        })
        if len(found) == len(sequence):
            assert [row["op"] for row in found] == sequence, found
            assert len({row["partition"] for row in found}) == 1, found
            assert all(a["offset"] < b["offset"] for a, b in zip(found, found[1:])), found
            wal = [row for row in found if row["op"] != "r"]
            versions = [(row["source_lsn"], row["transaction_order"]) for row in wal]
            assert all(a < b for a, b in zip(versions, versions[1:])), found
            return found
    detail = f"; last topic metadata error: {last_metadata_error}" if last_metadata_error else ""
    raise TimeoutError(f"Expected {sequence} for {key}, observed {found}{detail}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("phase", choices=["initial", "restart", "relay-baseline", "relay-update"])
    args = parser.parse_args()
    started = datetime.now(timezone.utc)
    compatibility = get_json(f"{REGISTRY}/config")["compatibilityLevel"]
    assert compatibility == "BACKWARD", compatibility
    consumer = Consumer({
        "bootstrap.servers": "kafka:29092",
        "group.id": f"paritylake-docker-probe-{args.phase}",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    topic = TOPIC.replace("pg.", "relayed.", 1) if args.phase.startswith("relay-") else TOPIC
    consumer.subscribe([topic])
    registration = None
    try:
        with psycopg.connect(autocommit=True) as conn:
            if args.phase == "initial":
                prepare_source(conn)
                registration = register_connector()
                status = running_connector()
                events = read_events(consumer, FIXTURE, ["r"])
                with conn.transaction():
                    conn.execute(
                        """UPDATE app.trips SET fare_amount = 13.45, total_amount = 15.45
                        WHERE trip_id = %s""", (FIXTURE,),
                    )
                    conn.execute("DELETE FROM app.trips WHERE trip_id = %s", (FIXTURE,))
                events.extend(read_events(consumer, FIXTURE, ["u", "d"]))
                assert events[1]["after_fare"] == "13.45"
                assert events[2]["before_fare"] == "13.45"
            elif args.phase == "restart":
                status = running_connector()
                slot = conn.execute(
                    """SELECT plugin, active FROM pg_replication_slots
                       WHERE slot_name = 'paritylake'"""
                ).fetchone()
                assert slot == ("pgoutput", True), slot
                key = FIXTURE + 1
                conn.execute(
                    """INSERT INTO app.trips (trip_id, pickup_ts, fare_amount, total_amount)
                    VALUES (%s, '2024-01-01 00:02+00', 7.00, 7.00)""", (key,),
                )
                events = read_events(consumer, key, ["c"])
            else:
                status = running_connector()
                expected = ["c", "u"] if args.phase == "relay-update" else ["c"]
                events = read_events(consumer, FIXTURE + 1, expected)
        print(json.dumps({
            "phase": args.phase,
            "dataset": "deterministic synthetic CDC fixture",
            "started_utc": started.isoformat(),
            "finished_utc": datetime.now(timezone.utc).isoformat(),
            "registry_compatibility": compatibility,
            "status": status,
            "registration": registration,
            "events": events,
        }, indent=2))
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
