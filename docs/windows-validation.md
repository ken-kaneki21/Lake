# ParityLake Windows validation

The verified Ubuntu integration baseline is commit
`be4a55899d17a95594f4d17bca4c35ded84b0b53` on [run 43881532134](https://github.com/ken-kaneki21/Lake/actions/runs/43881532134).
The Windows procedures reuse its Compose definitions, Spark checkpoint,
source audit and reconciliation queries. Each mode uses its own Compose
project, network, credentials and named volumes. Nothing is published on
host ports, and no paid infrastructure is involved.

## Run in order

Open PowerShell 7 at `C:\Users\saura\Downloads\ParityLake`. Start Docker
Desktop in Linux container mode. Use the same checkout and evidence root for
every command. Leave Docker running and prevent Windows sleep during the
observation. The verified pipeline needs 8 GiB Docker RAM, two or more Docker
CPUs, and 15 GiB free on both the evidence drive and the Docker filesystem
for this validation procedure.

| Stage | Exact command |
| --- | --- |
| 1. Windows Docker preflight | `pwsh -NoProfile -File .\scripts\Start-ParityLake.ps1` |
| 2. Fault trials and benchmarks | `pwsh -NoProfile -File .\scripts\Run-FaultMatrix.ps1` |
| 3. Clean observation | `pwsh -NoProfile -File .\scripts\Start-Observation24h.ps1` |
| 4. Independent observation verification | `pwsh -NoProfile -File .\scripts\Verify-Observation.ps1` |
| 5. Collect evidence | `pwsh -NoProfile -File .\scripts\Collect-ParityLakeEvidence.ps1` |
| 6. Stop the project | `pwsh -NoProfile -File .\scripts\Stop-ParityLake.ps1` |

Preflight must pass on the current commit. It checks the Docker engine and
resources, starts the full stack, exercises the CDC fixture and independent
Iceberg reconciliation, and runs dbt debug, both builds and the financial
parity probe. Allow time for the initial image and Maven downloads. Stage 2
automatically stops the preflight project. Stage 3 requires the fault matrix
to pass and stops that project before starting its separate clean stack.

A failed fault trial stops the run and records failure. A failed or partially
initialized fixture is retained. To retry with clean volumes, stop the old
projects using stage 6, then repeat the commands with
`-EvidenceRoot .\evidence\windows-retry` on every stage. Re-running stage 3
after interruption collects one additional sample where possible and preserves
the original run's **INVALID** status. Use a fresh evidence root for a new
uninterrupted attempt. Stage 6 stops containers and leaves volumes, checkpoints
and evidence in place. It is safe to run again.

PowerShell writes timestamps and stage state directly to the evidence root.
If a checkout created before the LF rule still has CRLF shell files, first
check that both tracked shell scripts have no local edits, then restore the
checkout representation with
`git checkout-index --force -- docker/object-store.sh docker/run-dbt.sh`.

## What is measured

- The fault matrix runs 10 repetitions of each of drop, duplicate, delay,
  reorder and mutate, with ordinary fixture traffic in each trial. It
  measures captured faults on actual Kafka delivery, plus nine distinct
  service-stop, Spark kill, delete and additive-schema scenarios. The
  controlled delay is explicitly released after a missing-key assertion.
  The matrix runs serially to make exact per-trial counts attributable.
  This procedure does not implement the spec's fixed two-hour clean baseline
  and 60-minute injection schedule. A scheduled formal drill remains open.
- The two benchmark sizes are 100 and 1,000 synthetic row changes each
  committed in a single transaction, after a CDC fixture warmup. Throughput
  includes container command dispatch, relay and a Spark available-now drain.
  Source timestamps come from Debezium and target timestamps from the
  recorded silver MERGE. These are nearest-rank end-to-end samples, not
  per-event processing times or a sustained rated throughput claim.
  Docker stats represent a point-in-time snapshot of persistent services
  before drain, so they do not measure peak Spark CPU/RAM.
- The clean procedure injects 10 distinct synthetic trips every five
  minutes, drains the existing checkpoint, checks exact independent parity
  against PostgreSQL and samples service health between batches. Its fixed
  fixture pickup window deliberately keeps every sample in the same
  settled audit window. This uses one synthetic insert workload, distinct
  from TLC replay and the fault project. UTC and monotonic clocks plus
  Windows power events guard against interruption. The verifier requires
  289 successful integrity samples, bounds health and integrity gaps, checks
  service identities/restarts and verifies saved reconciliation JSON.
  Resource snapshots exclude the already-exited Spark one-shot jobs.
- Commands are timestamped with native exit statuses. Logs redact the
  generated credential values. The collector excludes .env, archives only
  JSON, JSONL and logs, and records SHA-256 file fingerprints. Review and
  share the ZIP printed by stage 5.

## Evidence paths

- `evidence/windows/preflight/preflight.json` and adjacent Docker/dbt logs
- `evidence/windows/fault/matrix.json`, `fault-*.json` and `additional-*.json`
- `evidence/windows/fault/benchmark-100.json` and `benchmark-1000.json`
- `evidence/windows/clean/observation.json`, `samples.jsonl`, `health.jsonl`,
  `host-power-events.json` and `observation-verification.json`
- `evidence/windows/export-*.zip` with an evidence manifest and hashes

## Gates requiring further runtime work

The formal timed fault protocol requires a continuously running fixture
with two measured clean hours before faults, 50 injections over one hour,
and two settled reconciliation cycles after the last fault. The serial
Windows matrix above proves mechanism-level outcomes independently. A live
scheduler must enforce the formal timing and measure its cycle-based
detection times. Run a separate breaking-schema compatibility check; the
additive-schema case in the matrix exercises a different contract.

Airflow DAG deployment and execution remain open. Route the hourly gold
build to the existing dbt launcher, the 15-minute reconciliation DAG to
`recon/reconcile.py`, and any fault DAG to the isolated fault environment.
Verify scheduling, retries and a failed-task alert on a real Airflow
deployment before recording a pass. This checkout has no Airflow DAGs or
Terraform adapter directory. Terraform validation requires implementing the
optional adapter, then running `terraform fmt -check -recursive`,
`terraform init -backend=false` and `terraform validate` from that module.
Do not run `terraform apply`.

For an image/dependency security review, use the collected image IDs with
`trivy image --severity HIGH,CRITICAL --exit-code 1 <image-id>`, audit the
resolved Python environments with `pip-audit`, and scan full Git history
with `gitleaks git --redact`. Retain scanner versions, findings and any
explicit triage. The filename and configuration review in this commit is
partial security evidence.
