# Work handoff

## Current integration checkpoint

GitHub main is at 39868a5562ef19b0a09930a2a6f021922ef5896e. The latest
remote feature commit visible in this session was
84ccefb7273a0dc4556ae52d8e03b79891ad5ec6. GitHub returned no commit for
42c1c469fcf9b52153ab8b36d791c857aefc90c5. The new local tip preserves that
checkpoint and all prior commits. Retrieve the exact tip with
git rev-parse feat/local-first.

[Ubuntu Actions run 43853598688](https://github.com/ken-kaneki21/Lake/actions/runs/43853598688)
ran the 84ccefb workflow. PostgreSQL, Kafka, Confluent Schema Registry and
Kafka Connect reached Compose health, and nine Python unit tests passed
inside the integration container. The CDC probe failed on the first
connector status GET with HTTP 404 after registration returned without an
HTTP error. The workflow stopped before capturing a CDC envelope.
GitHub listed the diagnostic archive. The Work runtime could not read its
contents, so connector-worker logs and the exact reason why status was
temporarily absent remain unconfirmed.

## What changed

- Verify Debezium PostgreSQL plugin discovery and registration response
  name before polling. Emit class and version only, with no config echo.
- Wait for a running connector and at least one running task. Retry status
  HTTP 404 within 75 attempts. Other HTTP errors and terminal states fail
  immediately. Existing event and restart assertions remain in place.
- Redact ephemeral credential values before uploading Compose logs.
- Four readiness regression tests passed. Total Python unit tests: 21.

## Publish the incremental bundle from Windows PowerShell

Run from the existing authenticated checkout. Replace the filename path
with the downloaded file location. The bundle requires commit 84ccefb,
already present on the remote feature branch observed in this session.

    git fetch origin feat/local-first
    if ($LASTEXITCODE -ne 0) { throw 'Remote fetch failed' }
    $bundle = "$env:USERPROFILE\Downloads\ParityLake-ci-readiness.bundle"
    git bundle verify $bundle
    if ($LASTEXITCODE -ne 0) { throw 'Bundle prerequisites missing' }
    git fetch $bundle refs/heads/feat/local-first
    if ($LASTEXITCODE -ne 0) { throw 'Bundle fetch failed' }
    $bundleTip = git rev-parse FETCH_HEAD
    git merge-base --is-ancestor origin/feat/local-first $bundleTip
    if ($LASTEXITCODE -ne 0) { throw 'Remote feature branch diverged. Inspect before pushing.' }
    git push origin "${bundleTip}:refs/heads/feat/local-first"
    if ($LASTEXITCODE -ne 0) { throw 'Fast-forward push failed' }
    git ls-remote --heads origin

This push updates only the feature branch. Inspect its Ubuntu workflow
and preserve the new run URL. The workflow builds isolated Docker Compose
services, captures logs, and removes its disposable project. Use
python -m unittest discover -s tests -v for the local Python checks.

## Remaining gates

Latest Docker runtime, Postgres CDC Avro envelopes, durable relay,
Spark/Iceberg MERGE and restart, dbt execution, cross-engine parity, fault
outcome matching, 50-fault experiment, independent 24-hour observation,
Airflow, full security review, Terraform static validation and release all
remain open. No new Docker services ran in Work. No AWS or Azure
resources were provisioned.

## Security evidence and limits

The known PostgreSQL Compose value is a required variable reference.
The inspected CDC and MinIO references are placeholders or generated
runtime credentials. The tracked credential-file check found no .env,
key files or Terraform state. The GitGuardian occurrence list was not
available here, so the incident as a whole is unclassified. A full secret
and dependency scan has not been run.

## Run 43857040155 and raw topic readiness

The Ubuntu run at 36d440d reached healthy PostgreSQL, Kafka, Schema
Registry and Connect. All 21 unit tests passed inside Docker. Connect
registration and RUNNING readiness completed. The initial snapshot
consumer then failed with UNKNOWN_TOPIC_OR_PART for pg.app.trips.
The relay, Iceberg, dbt, reconciliation and public TLC stages were skipped.

The next commit pre-creates the three raw CDC topics on the isolated
broker. The probe tolerates initial unknown-topic errors within its
existing 150-second event deadline and still requires every expected
event. An error after events begin or any other Kafka error fails the
probe. The new bundle is ParityLake-cdc-topics.bundle and requires
36d440d. Use the same fast-forward commands above with this filename.

## Run 43857964856 and the Spark Python runtime

[GitHub Actions run 43857964856](https://github.com/ken-kaneki21/Lake/actions/runs/43857964856)
passed the PostgreSQL audit and Kafka CDC step on 10295987. Debezium
3.0.8.Final delivered snapshot, update and delete, then an insert
after Connect restart. All 21 unit tests passed inside the integration
container. The lake image failed while resolving fastavro 1.12.1 and
none of the lake or dbt runtime checks executed.

The Spark distribution stays at 3.5.5. The lake build now uses Python
3.11 on Debian Bookworm and Java 17, with separate checked virtual
environments for lake and dbt dependencies. The Docker build prints
the Python version and architecture for the upstream and new images.
The image smoke test runs Spark in local mode with an isolated config,
checks the driver and worker interpreter, exercises the Avro decimal
codec and the existing quality function, and checks the dbt CLI.

The Work Docker named pipe denied access, so package co-installation,
the Spark smoke test and the full Docker workflow must run on GitHub.
Publish ParityLake-spark-python.bundle using the existing fast-forward
commands above. Its prerequisite is 10295987. For the container test,
see the Build and smoke-test Spark image step in docker-cdc.yml.

## Run 43859763933 and dbt launcher

The Docker run at bfe40a6 passed live CDC and the Spark build. The smoke
test exercised Python 3.11.18 on x86_64, Spark 3.5.5, the Avro decimal
round-trip and the Great Expectations check. The dbt invocation stopped
at No such option: --project-dir. Iceberg runtime work was skipped.

The launcher now starts in /app/dbt and keeps the installed dbt 1.11
profile, log and target paths in supported environment variables.
Derby scratch stays under /tmp because the project image directory is
read-only to the Spark user. The regression step runs dbt --version and
dbt parse in the built image. The integration step runs dbt debug with
services up, then retains full-refresh and incremental dbt build.

No Docker engine is accessible from Work. These updated dbt checks must
run on GitHub. Publish ParityLake-dbt-launcher.bundle by fast-forward
using the existing commands above. Its prerequisite is bfe40a6.
