# Work handoff

Canonical destination [ken-kaneki21/Lake](https://github.com/ken-kaneki21/Lake).
GitHub returned HTTP 409 because Lake was empty. The local branch is
feat/local-first with origin set to Lake. Its initial security commit is
39868a5. Run git rev-parse HEAD to resolve the source checkpoint containing
this file. The remote is untouched. StreamGuard was inspected at
6807a8e358f5bcdacbd371ec0d7c4a91accb5053. Both specifications were read
before implementation.

| Gate | Status | Work and evidence |
| --- | --- | --- |
| L0 Repository and environment | PARTIAL | Remote, local branch and ignores verified. Docker access blocked. See [environment](environment.md). |
| L1 Source and CDC | PARTIAL | Native source schema and hard-delete checks passed. TLC seed/replay and live connector checks are open. See [evidence](evidence/source-probe.json). |
| L2 Relay | NOT DONE | Persistent relay and ledger remain unimplemented. |
| L3 Iceberg | NOT DONE | MinIO, catalog, Spark, bronze, silver and GX remain unimplemented. Version comparator has unit tests. |
| L4 Gold | NOT DONE | dbt models, SCD2 and build remain unimplemented. |
| L5 Reconciliation | PARTIAL | PostgreSQL bucket query passed Python fixture parity. Spark SQL draft is untested. Runner, drill-down and alerts remain unimplemented. See [decisions](decisions.md). |
| L6 Operations | NOT DONE | Airflow and CI remain unimplemented. |
| L7 Experiments | NOT DONE | No 50-fault run, 24-hour observation, latency, throughput or resource benchmark. |
| L8 Release | NOT DONE | No PR, merge, tag, AWS adapter validation or release. |

The checkpoint includes source DDL, publication SQL, a native test probe,
TLC download/seed/replay scripts, a source Compose file, a connector JSON
draft, source and destination bucket SQL, and six unit tests. No services
are running. Native fixtures are under the ignored data/pg-test-* directories.
Compose PostgreSQL uses loopback port 55432 and a named data volume.
Git CLI remote discovery and TLC download both hit DNS errors.

## Verified commands

    python -m unittest discover -s tests -v
    pwsh -NoProfile -File tests/run_postgres_probe.ps1
    $env:PGPASSWORD = [guid]::NewGuid().ToString('N')
    docker compose -f docker/compose.source.yml config --quiet
    Remove-Item Env:PGPASSWORD
    python -m json.tool docker/connect/debezium-postgres.json

The native probe requires installed PostgreSQL binaries and creates a fresh
ignored fixture directory. It avoids network sockets.

## First commands on a host with Docker and network access

    docker info --format '{{.ServerVersion}}'
    $env:PGPASSWORD = [guid]::NewGuid().ToString('N')
    docker compose -f docker/compose.source.yml up -d --wait postgres
    python -m pip install -r requirements-source.txt
    python -m source_db.download --month 2024-01
    $env:PGHOST = '127.0.0.1'
    $env:PGPORT = '55432'
    $env:PGDATABASE = 'app'
    $env:PGUSER = 'paritylake'
    python -m source_db.seed --zones data/taxi_zone_lookup.csv
    python -m replayer.replay --month 2024-01 --rate 20 --speedup 120 --limit 100

These commands exercise the source. Replay was authored for one writer and
follows Parquet file order without a global sort. Its operation mix and rate
require live verification. Next implement KRaft, Registry and a Connect image
with PostgreSQL and Confluent Avro plugins. Apply the publication, provision a
dedicated replication role, and configure Connect's environment config
provider. Inspect the real c/u/d/r Avro envelopes and restart behavior before
wiring the version comparator into an Iceberg merge.
