"""Canonical fingerprints for the Postgres/Iceberg reconciliation contract."""

from decimal import Decimal
from hashlib import md5

CENT = Decimal("0.01")


def amount(value: Decimal | None) -> str:
    if value is None:
        return "~"
    quantized = value.quantize(CENT)
    if quantized != value:
        raise ValueError("amount contains a fraction of a cent")
    return format(abs(quantized) if quantized == 0 else quantized, ".2f")


def bucket(trip_id: int) -> int:
    return int(md5(str(trip_id).encode("ascii"), usedforsecurity=False).hexdigest()[:2], 16)


def fingerprint(
    trip_id: int, fare: Decimal | None, tip: Decimal | None, is_deleted: bool
) -> int:
    payload = f"{trip_id}|{amount(fare)}|{amount(tip)}|{int(is_deleted)}"
    return int(md5(payload.encode("ascii"), usedforsecurity=False).hexdigest()[:8], 16)
