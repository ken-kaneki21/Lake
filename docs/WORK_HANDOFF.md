# Work handoff

The canonical GitHub repository is [ken-kaneki21/Lake](https://github.com/ken-kaneki21/Lake).
GitHub returned HTTP 409 because the repository remains empty. The GitHub
integration reported zero accessible installations. Earlier direct Git
publication failed at DNS and the connector returned HTTP 403 on write.
This session did not retry either failing write path. Windows Docker
Desktop and existing services were left alone. No alternate Docker endpoint
was available. No containers or Actions jobs ran in this session.

## Exact history

- main is anchored at 39868a5562ef19b0a09930a2a6f021922ef5896e.
- Native source checkpoint 5f56828480d2ec26ccbd260ad62f0c30a49e7ad3.
- Previous CDC workflow checkpoint 84ccefb7273a0dc4556ae52d8e03b79891ad5ec6.
- Local feature tip is the commit containing this handoff. Run git rev-parse
  feat/local-first for its full SHA. The updated bundle includes both branch
  refs and preserves the original objects.

## Publish from Windows PowerShell

From the existing checkout, or from a new clone of the bundle, set the
canonical origin before publication. In a fresh folder, use:

    git clone --branch feat/local-first .\ParityLake-latest.bundle ParityLake
    if ($LASTEXITCODE -ne 0) { throw 'Bundle clone failed' }
    Set-Location .\ParityLake
    git remote set-url origin https://github.com/ken-kaneki21/Lake.git
    $remoteHeads = @(git ls-remote --heads origin)
    if ($LASTEXITCODE -ne 0) { throw 'Could not verify the remote' }
    if ($remoteHeads.Count -ne 0) { throw 'Remote has branches. Inspect before pushing.' }
    git push --atomic origin 39868a5:refs/heads/main HEAD:refs/heads/feat/local-first
    if ($LASTEXITCODE -ne 0) { throw 'Push failed' }
    git ls-remote --heads origin

Rename the downloaded bundle to ParityLake-latest.bundle or pass its
actual path. The push initializes main and publishes the feature branch
without a force update. After push, inspect the PostgreSQL and CDC
integration workflow on the feature commit and open a draft PR into main.

## First runtime gate

Use the repository workflow on Ubuntu 24.04. It starts isolated
PostgreSQL 16, Kafka KRaft, Confluent Registry and Debezium. It then
checks snapshot, update, hard delete and insert-after-restart envelopes,
starts the baseline relay, persists rows to Iceberg, restarts MinIO and
the REST catalog, and checks baseline/drift/recovery with a saved S3A
checkpoint. It runs dbt build with full refresh and incremental rebuild,
then downloads and separately replays official TLC data. The workflow
collects logs and removes its disposable Compose project. Compose
publishes no host ports. The AWS-named environment variables configure
the local MinIO S3 client. Neither AWS nor Azure is provisioned.

## Executed checks

- 17 Python unit tests passed (9 existing, 3 reconciliation and 5 ledger).
- Python compilation and static Compose validation passed.
- Workflow and dbt YAML parsed. dbt packages, Jinja compilation and
  runtime execution are still unverified.
- Prior native PostgreSQL 18.4 source results are preserved in
  evidence/source-probe.json. This session did not perform native runtime
  verification or reuse those results as Docker evidence.

## Gate status

| Gate | Status |
| --- | --- |
| Source and CDC | Source fixture previously verified with native PostgreSQL. Docker CDC workflow is unrun. |
| Relay and ledger | Durable local outbox, planned modes and unit tests added. Kafka runtime, crash/retry and observed-outcome matching remain open. |
| Iceberg | MinIO, persistent REST catalog, bronze, quarantine, silver MERGE and checkpoint assertions authored. Docker runtime and schema evolution remain open. |
| Gold | dbt-spark models and checks authored. Full-refresh and incremental commands are unrun and exact per-row parity needs a dedicated assertion. |
| Reconciliation | Independent runner, 24-hour lookback, durable alert evidence and exact differences implemented. Spark-to-PostgreSQL parity assertions are unrun. A 24-hour lookback query does not measure a clean 24-hour observation. |
| Operations and security | Docker workflow authored. Airflow DAGs, full security scans and Terraform adapter remain unfinished. |
| Experiments | No 50-fault experiment, 24-hour clean run, latency, throughput, CPU, RAM or disk measurements. |
| Publication | No push, PR, merge, tag or release. |

## Design details and limitations

The relay persists input decisions before committing Kafka offsets. A
crash after Kafka output acknowledgement and before the SQLite delivery
mark can redeliver an output. Silver must absorb the duplicate. Run one
relay process per state volume. A reorder needs a same-key successor. The
relay schema mode injects a malformed Confluent frame. An independent
incompatible Registry schema experiment remains required.

Bronze and quarantine use offset-keyed insert-only Iceberg MERGEs. Silver
retains source versions alongside tombstones. The dbt incremental backing
table keeps the latest trip version and exposes live rows through
fct_trips. The SCD2 driver dimension rebuilds intervals from valid bronze
events and keeps the last version at each source timestamp in milliseconds.
Timestamps tied inside a single millisecond are intentionally collapsed in
gold. The project retains the underlying versions in bronze.

Run the first runner result before treating any new runtime behavior as
passed. Validate real schema ordering and cross-engine parity, then
implement Airflow and fault-outcome matching and execute the timed drills.
