# ParityLake - CDC & Data Reliability Platform

Local-first development checkpoint. The implemented foundation is the
PostgreSQL source schema, hard-delete audit, seed and replay scripts, CDC
configuration draft, and PostgreSQL-side bucket reconciliation query.

## Verified checks

- Python unit tests for exact-cent hashing and CDC version comparisons.
- Native PostgreSQL 18.4 source audit and fixture parity with the Python hash.
- Static validation of the PostgreSQL 16 Compose configuration.

See [the handoff](docs/WORK_HANDOFF.md) for exact commands and gates.
NYC TLC publishes the [trip input and zone lookup](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page).
Driver records and replay mutations are synthetic.
[StreamGuard](https://github.com/ken-kaneki21/streamguard/tree/6807a8e358f5bcdacbd371ec0d7c4a91accb5053)
is the reference for TLC ingestion, KRaft setup and quality testing. This
checkpoint uses new code for PostgreSQL WAL replay and reconciliation.
