# ParityLake - CDC & Data Reliability Platform

Local-first PostgreSQL, Debezium, Kafka, Spark and Apache Iceberg project.
The source-level checks pass. The Docker release path remains unverified.

[Architecture](docs/architecture.md) | [Work handoff](docs/WORK_HANDOFF.md) |
[Prior source evidence](docs/evidence/source-probe.json)

## Integration and data

The [Ubuntu integration workflow](.github/workflows/docker-cdc.yml) builds
the isolated Compose project and checks CDC envelopes, relayed delivery,
Iceberg merges, temporary drift, checkpoint resume and dbt. It keeps
synthetic fixture evidence separate from the official NYC TLC Yellow Taxi
download and replay. The handoff lists the known gaps and exact commands
to publish the branch so the workflow can run. No host ports are published.

Driver identities and update traffic are synthetic. NYC TLC publishes the
[public trip data and zone lookup](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page).
[StreamGuard](https://github.com/ken-kaneki21/streamguard/tree/6807a8e358f5bcdacbd371ec0d7c4a91accb5053)
is the reference for TLC ingestion, KRaft setup and quality testing.

## Unit verification

    python -m unittest discover -s tests -v

The fault experiment, 24-hour observation, performance measurements,
Airflow and release remain open. No AWS or Azure resources were provisioned.

## Windows validation and acceptance evidence

See the [Windows runbook](docs/windows-validation.md) for the mandatory
Docker preflight, isolated fault matrix, clean observation, evidence
collection and safe teardown. The [acceptance matrix](docs/final-acceptance.md)
separates measured CI results from gates waiting on execution.
