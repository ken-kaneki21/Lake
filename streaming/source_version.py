"""Source version comparison for a key within one PostgreSQL timeline."""

from dataclasses import dataclass


@dataclass(frozen=True, order=True)
class SourceVersion:
    is_wal: int
    lsn: int
    transaction_order: int

    @classmethod
    def from_envelope(cls, envelope):
        op = envelope["op"]
        if op == "r":
            # Snapshot events rank below every committed WAL change.
            return cls(0, 0, 0)
        if op not in {"c", "u", "d"}:
            raise ValueError(f"unsupported CDC operation {op!r}")
        lsn = envelope["source"]["lsn"]
        transaction = envelope.get("transaction")
        if type(lsn) is not int or lsn <= 0 or not transaction:
            raise ValueError("WAL event lacks a valid LSN or transaction metadata")
        raw_order = transaction["total_order"]
        if isinstance(raw_order, bool) or not isinstance(raw_order, (str, int)):
            raise ValueError("invalid transaction order")
        order = int(raw_order)
        if order <= 0:
            raise ValueError("transaction order must be positive")
        return cls(1, lsn, order)
