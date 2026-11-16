"""Relay Debezium Avro with durable failure decisions and an at-least-once outbox."""

import argparse
import io
import json
import os
from decimal import Decimal
from urllib.request import urlopen

from chaos.ledger import MODES, arm, delivered, open_ledger, pending, stage

TOPICS = ["pg.app.trips", "pg.app.drivers", "pg.app.zones"]


class Avro:
    def __init__(self, registry):
        self.registry = registry
        self.schemas = {}

    def read(self, wire):
        from fastavro import parse_schema, schemaless_reader

        if not wire or len(wire) < 6 or wire[0]:
            raise ValueError("Invalid Confluent Avro framing")
        schema_id = int.from_bytes(wire[1:5], "big")
        if schema_id not in self.schemas:
            with urlopen(f"{self.registry}/schemas/ids/{schema_id}", timeout=10) as response:
                self.schemas[schema_id] = parse_schema(json.loads(json.load(response)["schema"]))
        schema = self.schemas[schema_id]
        return schemaless_reader(io.BytesIO(wire[5:]), schema), schema

    def rewrite(self, mode, wire):
        from fastavro import schemaless_writer

        if mode == "schema_drift":
            # Relay-side malformed-payload probe. Registry compatibility has its own gate.
            return bytes([1]) + wire[1:]
        event, schema = self.read(wire)
        record = event["before"] if event["op"] == "d" else event["after"]
        if event["source"]["table"] != "trips":
            raise ValueError("Amount mutation requires the trips topic")
        for field in ("fare_amount", "total_amount"):
            record[field] += Decimal("0.01")
        output = io.BytesIO(wire[:5])
        output.seek(5)
        schemaless_writer(output, schema, event)
        return output.getvalue()


def flush_outbox(conn, producer):
    for row in pending(conn):
        errors = []
        producer.produce(
            "relayed." + row["topic"].split(".", 1)[1],
            key=row["key"], value=row["value"],
            headers={"paritylake-source-offset": str(row["offset"]),
                     "paritylake-source-partition": str(row["partition"])},
            on_delivery=lambda error, _: errors.append(error) if error is not None else None,
        )
        if producer.flush(15) or errors:
            raise RuntimeError(f"Kafka relay delivery failed: {errors}")
        delivered(conn, row)


def forward(conn):
    from confluent_kafka import Consumer, Producer

    bootstrap = os.getenv("KAFKA_BOOTSTRAP", "kafka:29092")
    codec = Avro(os.getenv("SCHEMA_REGISTRY", "http://schema-registry:8081"))
    consumer = Consumer({
        "bootstrap.servers": bootstrap, "group.id": "paritylake-relay",
        "auto.offset.reset": "earliest", "enable.auto.commit": False,
    })
    producer = Producer({"bootstrap.servers": bootstrap, "enable.idempotence": True})
    try:
        consumer.subscribe(TOPICS)
        while True:
            flush_outbox(conn, producer)
            message = consumer.poll(0.5)
            if message is None:
                continue
            if message.error():
                raise RuntimeError(message.error())
            key, _ = codec.read(message.key())
            event, _ = codec.read(message.value())
            _, id_value = next(iter(key.items()))
            stage(conn, (
                message.topic(), message.partition(), message.offset(), str(id_value),
                message.key(), message.value(), event["source"]["lsn"],
            ), codec.rewrite)
            consumer.commit(message=message, asynchronous=False)
    finally:
        consumer.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", required=True)
    action = parser.add_subparsers(dest="action", required=True)
    action.add_parser("forward")
    command = action.add_parser("arm")
    command.add_argument("--key", required=True)
    command.add_argument("--topic", choices=TOPICS, default=TOPICS[0])
    command.add_argument("--mode", choices=MODES, required=True)
    command.add_argument("--expected", required=True)
    command.add_argument("--delay-seconds", type=float, default=1200)
    args = parser.parse_args()
    conn = open_ledger(args.db)
    try:
        if args.action == "forward":
            forward(conn)
        else:
            if args.topic != TOPICS[0] and args.mode == "mutate":
                parser.error("Mutation requires the trips topic")
            print(arm(conn, args.topic, args.key, args.mode, args.expected, args.delay_seconds))
    finally:
        conn.close()


if __name__ == "__main__":
    main()
