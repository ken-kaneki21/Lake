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

## Run 43862518416 and dbt command exit isolation

The Docker run at b844c60 passed live CDC and the Spark smoke assertions.
The complete log shows Core 1.11.0, an update notice for 1.12.7, and
Spark adapter 1.11.0. The container step exited 2 without emitting parse
output. The shell used errexit and recorded only the group status, so
the log does not establish whether version reporting or parse failed.
The update notice alone does not establish the cause.

The next run captures dbt --version and dbt --debug parse statuses
independently inside the same image. It reports the manifest assertion
status and requires every check to pass. The service-backed dbt debug
and full-refresh and incremental builds remain mandatory. No dependency
pins or launcher behavior changed. Docker access is denied from Work,
so actual command statuses are pending runner execution.

Publish ParityLake-dbt-exit-status.bundle with the existing fast-forward
commands. The prerequisite is b844c60.

## Run 43863854226 and Maven Central rate limiting

The Docker Connect build at beaa9fe failed downloading the pinned
Debezium 3.0.8.Final archive with HTTP 429. CDC, Spark, and the dbt
exit-status diagnostics were skipped.

The Connect build now allows six transient retries per download, using
curl's exponential backoff and Retry-After behavior with a 180-second
retry budget and 60-second transfer timeout. It validates Maven Central's
published SHA-1 sidecar before extracting the archive. The sidecar
is a transport integrity check, not an independently pinned digest.
CI loads the built Connect image from Buildx and caches the verified
layer with a dedicated GitHub Actions cache scope. Cache export failure
does not bypass artifact verification or fail an otherwise valid build.
The existing Docker integration and dbt diagnostic assertions remain.

Publish ParityLake-debezium-cache.bundle using the fast-forward commands.
Its prerequisite is beaa9fe. The Docker and Maven Central endpoints
were inaccessible from Work, so the first runner must confirm the
download, verification, cache behavior and the downstream workflow.

## Run 43865064444 and dbt parse diagnostics

On 8d17bc3, the Connect build and CDC assertions passed. The Spark
smoke test passed. dbt --version returned 0, parse returned 2, and the
manifest assertion returned 1. The runner log showed no parse error
text and the container was removed without exporting dbt.log.

The launcher now checks project/profile readability and initializes
writable target, log and Derby directories as the container user.
The smoke test passes explicit project/profile/target CLI options to
parse, prints dbt.log if parsing fails, and asks dbtRunner to expose
an exception hidden by the console entry point. The original CLI exit
and mandatory nonempty manifest assertion are retained. Diagnostic
success cannot turn a failing parse green. The debug and two
lakehouse builds still follow the runtime integration assertions.

The root parse exception is still unconfirmed. Docker was unavailable
from Work. Publish ParityLake-dbt-parse.bundle with the existing
fast-forward commands. Its prerequisite is 8d17bc3.

## Run 43867180258 and dbt logger permissions

The runner on 70139d5 reproduced the parse exit 2 and exposed the
exception from dbt preflight. Logger initialization attempted to create
/app/dbt/logs and failed with PermissionError. CDC, Spark, Avro, Great
Expectations and dbt --version passed. Iceberg and dbt builds were skipped.

The launcher supplies an explicit global --log-path for dbt commands
and exports both env var spellings for writable output paths. CI uses
the same log path for the diagnostic dbtRunner and explicitly targets
/tmp for both builds. The smoke test requires UID 185, project source
that is not writable by that user, a nonempty /tmp manifest and dbt.log,
and no logs or target directories inside the project.

Docker execution is unavailable from Work. Publish the verified
incremental ParityLake-dbt-log-path.bundle from 70139d5 and check the next
runner. Build, reconciliation, fault experiments, performance, Airflow,
security validation, clean observation and release remain open.

## Object-storage image availability after 69e8d39

The previous workflow reached the streaming phase, then Docker Hub
denied the pinned MinIO server pull. Exact historical Quay image pulls
could not be verified from Work. The replacement is the publisher's
multi-platform RustFS 1.0.5 index pinned by digest. A second service uses
the same non-root image's curl client for signed SigV4 bucket setup.
Apache Iceberg documents this combination in its Spark quickstart.

The endpoint is object-store:9000 for both Hadoop S3A and Iceberg
S3FileIO. The existing MINIO_ROOT_USER and MINIO_ROOT_PASSWORD inputs
remain accepted and are wired to RustFS and the S3 clients. There are
no host port mappings. RustFS uses its own named volume and does not
reuse an existing MinIO volume. Existing MinIO data requires an S3-level
migration. This integration only asserts the bucket and object operations
it exercises, not full compatibility with MinIO extensions.

Before building Connect, the Ubuntu workflow pulls both roles, starts
RustFS, creates and checks buckets, then writes and reads a signed test
object. After the catalog and object-store restart, it must read the
original object without rewriting it. The existing streaming, MERGE,
checkpoint, reconciliation and dbt assertions remain mandatory.

Work has no accessible Docker engine. Registry tag metadata was checked,
but actual pulls and runtime checks await an Actions runner. Publish
ParityLake-rustfs.bundle with the prior fast-forward commands. Its
prerequisite is 69e8d39.

## Published 9492a20 and dbt debug Git dependency

The reported runner reached Spark/Iceberg and independent reconciliation.
The baseline had four bronze records, two silver records and zero drift
windows. The deliberate source change on trip 900002 showed PostgreSQL
8.00 versus Iceberg 7.00 in one drift window. Recovery reached five
bronze records, two silver records and zero drift windows. Spark
connectivity passed dbt debug, which failed because git was missing.

The lake image now installs the Debian Git package. UID 185 and package
pins for Spark, Python and dbt remain unchanged. CI checks git --version
in the Spark smoke container, then runs dbt debug, full-refresh build,
and incremental build. Both builds are followed by direct PostgreSQL
comparisons against the dbt fact and revenue mart for trip count, fare,
tip and revenue. Baseline, drift, recovery and gold comparison files
are uploaded by the existing always-run evidence step.

Docker was inaccessible from Work. Publish ParityLake-dbt-git.bundle
with the prior fast-forward commands, using 9492a20 as prerequisite.
The new image, dbt builds and gold parity still require the runner.
