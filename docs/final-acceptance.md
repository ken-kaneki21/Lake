# ParityLake acceptance matrix

This matrix records the last proven runtime baseline and the local handoff.
Release-critical gates remain open.

| Requirement | Status | Evidence or completion procedure |
| --- | --- | --- |
| Ubuntu Compose, CDC, registry and relay | PASS | [CI baseline](evidence/ci-baseline.json) |
| Spark, RustFS, Iceberg MERGE, durability and restart | PASS | Same run. Bronze 4 to 5, two silver rows and recovered checkpoint. |
| PostgreSQL-to-Iceberg reconciliation and deliberate alert | PASS | Same run. One changed key in bucket a0, then zero drift. |
| dbt debug, full-refresh/incremental and financial parity | PASS | Same run. Both builds passed 21 checks and matched independent source totals. |
| Public TLC smoke | PASS | Same run. January 2024, seed 42, 100 committed transactions. |
| Windows preflight | BLOCKED | Work Docker API access was denied. Run stage 1 of [Windows validation](windows-validation.md). |
| Controlled Docker fault outcomes | BLOCKED | Run stage 2 after Windows preflight. Review 50 relay trial files and nine additional scenarios. |
| Formal timed protocol and incompatible-schema drill | BLOCKED | Serial matrix has different timing. Use the formal protocol in the runbook after validating the Windows runner. |
| Performance and recovery | BLOCKED | Run stage 2. Review both workloads and the resource sampling limits. Peak Spark measurements still require in-job sampling. |
| Independent 24-hour clean observation | BLOCKED | Run stages 3 and 4. No clean-window claim is made without complete evidence. |
| Airflow and Terraform | BLOCKED | No deployment/DAGs or optional adapter in the checkout. See the execution and static-check steps in the runbook. |
| Security review | BLOCKED | Partial source review only. No tracked credential files in filename/history check. CI uses read-only contents permission and SHA-pinned actions. Complete full Git, dependency and container scans. |
| Fresh Windows runtime and new-commit CI | BLOCKED | Publish the incremental bundle and run Docker CI and the Windows preflight from a fresh checkout. |
| PR, merge, tag and release | BLOCKED | Publish scripts, open a focused draft PR, attach actual evidence, then merge and tag only after release-critical gates pass. |

## Interview case studies from the green run

1. **Connect startup race.** A healthy Connect worker initially returned 404
   for connector status. Retrying that specific readiness condition let the
   probe wait for a running task while preserving terminal-error failures.
2. **Kafka topic provisioning.** A snapshot consumer saw an unknown topic
   during startup. Provisioning raw topics in CI removed dependence on
   asynchronous creation. The verified run still requires real Avro events.
3. **Spark Python compatibility.** The original image could not resolve the
   pinned Avro package. Keeping Spark 3.5.5 and using Python 3.11 made the
   dependency and worker checks reproducible while keeping dbt isolated.
4. **Non-root dbt logging.** dbt preflight tried to create a log directory
   under read-only source. An explicit scratch log path allowed parse and
   both builds while preserving UID 185 and the read-only project.
5. **Object-store replacement.** Historical MinIO image pulls failed.
   Digest-pinned RustFS and a signed bucket probe retained S3 semantics.
   The verified run read the original object after restart and recovered
   the Spark checkpoint. Existing MinIO volumes need S3-level migration.
6. **Independent financial drift.** A source change moved one fare from 7.00
   to 8.00. Reconciliation independently saw the discrepancy. Replay
   cleared it, and both gold builds matched the separate PostgreSQL totals.
   The fixture validates correctness on a small controlled input.

## Verified resume bullets

- Validated PostgreSQL-to-Iceberg recovery by detecting one deliberate
  financial drift window and returning to zero after checkpoint replay,
  with bronze increasing from four to five records.
- Verified dbt financial parity across two builds with 21 passing checks
  each by comparing fact and mart totals directly with PostgreSQL.
- Validated source replay on 100 January 2024 NYC TLC transactions by
  verifying 89 inserts, seven updates and four deletes in Ubuntu Docker CI.
