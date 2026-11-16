# Source-side decisions

## Current pickup-window truth and hard deletes

PostgreSQL writes a tombstone with the old pickup timestamp and exact
amounts in the same transaction as a hard delete. Reinserting the same
primary key clears it atomically. The view uses the current pickup time for
live rows and the last pickup time for deleted rows. Windows are half-open
UTC intervals. Counts and sums refer to live rows, while deletion count and
fingerprints retain deleted keys. Cancellation event history is a distinct
gold requirement that still needs an event-grain model.

The canonical payload is ASCII decimal ID, two exact fixed-scale amounts,
and a 0 or 1 deletion flag separated by pipe characters. A null amount
uses a tilde. Bucket assignment uses the first MD5 byte of the ID. The
fingerprint is the unsigned first four bytes of the payload MD5. An
order-independent exact sum has collisions and does not prove equality.
The two fixture rows demonstrate PostgreSQL/Python parity. Spark parity,
cross-window orchestration and mismatched-key drill-down remain open.

## Version ordering

Snapshot reads rank below WAL changes. Within a source key and PostgreSQL
timeline, the proposed WAL version is LSN then Debezium transaction
total_order. Keep transport identity independently. Python tests cover
same-LSN ordering, exact duplicate versions and stale snapshot ordering.
A live multi-row, multi-transaction and restart experiment must confirm
this rule before silver uses it. No failover timeline support is claimed.
