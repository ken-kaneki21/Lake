# Docker CDC checkpoint

The recovered local history contains the original source commit
5f56828480d2ec26ccbd260ad62f0c30a49e7ad3. The branch remains feat/local-first.
Run git rev-parse HEAD for the new CDC workflow checkpoint. Both commits
are still local. Lake was empty at inspection.

## What changed

- Added PostgreSQL 16, Kafka KRaft, Confluent Registry, Debezium Connect,
  and a containerized integration client.
- Added an Ubuntu Actions workflow with readiness checks, source SQL
  assertions, Avro checks, Connect restart, diagnostics, artifacts and
  cleanup. The CDC fixture uses its own deterministic synthetic records.
- The TLC downloader now retries transient errors up to three attempts,
  verifies the transport size when provided, checks Parquet columns and
  CSV keys, and prints a SHA-256 manifest. The workflow runs TLC replay
  separately and keeps its evidence distinct from the synthetic fixture.
- Nine Python unit tests passed on the Work host. The merged Docker
  Compose configuration passed static validation. No container or
  workflow execution occurred in this session.

## Reproduce the first runtime gate

Use a clean disposable Docker Compose project on a machine with an
accessible Docker engine. These commands run from the repository root
in bash.

    export COMPOSE_PROJECT_NAME=paritylake-cdc-test
    export COMPOSE_FILE=docker/compose.source.yml:docker/compose.cdc.yml
    export PGPASSWORD="$(openssl rand -hex 24)"
    export CDC_PGPASSWORD="$(openssl rand -hex 24)"
    docker compose --profile integration build --pull
    docker compose up -d --wait --wait-timeout 240 postgres kafka schema-registry connect
    docker compose run --rm --no-deps -T integration python -m tests.cdc_probe initial
    docker compose restart connect
    docker compose up -d --wait --wait-timeout 180 connect
    docker compose run --rm --no-deps -T integration python -m tests.cdc_probe restart

Use the workflow for the TLC replay command and artifact collection.
After saving diagnostics, clean up the disposable test project with
docker compose --profile integration down --volumes --remove-orphans.

## Publication blocker and exact recovery

Git push failed at DNS in the Work runtime. GitHub repository reads
worked, but the connected integration returned 403 Resource not
accessible by integration for the initial .gitignore write and reported
no accessible installations. The browser fallback did not expose a
usable GitHub session. Docker Desktop's named pipe denied engine access
and no separate Docker endpoint was configured.

On an authenticated Git host, after confirming Lake still has no refs:

    git ls-remote --heads origin
    git push --atomic origin 39868a5:refs/heads/main HEAD:refs/heads/feat/local-first

The main branch starts at the original security commit. The feature
branch keeps the exact recovered history. Open a draft PR into main and
inspect the PostgreSQL and CDC integration run. CI tests planned CDC
behavior. A passing run is required before treating it as verified.

Relay, Iceberg, dbt, reconciliation orchestration, Airflow, both timed
experiments, security scans and release remain open.
