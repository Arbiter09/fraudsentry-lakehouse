"""Shared bronze-layer validation, used by both the local consumer
(ingestion/consumer.py) and the AWS Lambda handler (lambda/handler.py) so
the two ingestion paths enforce identical rules.
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

REQUIRED_FIELDS = {
    "transaction_id", "account_id", "timestamp", "amount",
    "merchant_category", "city", "lat", "lon", "is_fraud",
}


class ValidationError(ValueError):
    pass


def validate_transaction(record: dict[str, Any]) -> dict[str, Any]:
    """Validate a raw transaction record and return it enriched with an
    `ingested_at` timestamp and `dt` partition key.

    Raises ValidationError on any rule violation, so the caller (consumer
    or Lambda handler) can route the record to a dead-letter path instead
    of writing it to bronze.
    """
    missing = REQUIRED_FIELDS - record.keys()
    if missing:
        raise ValidationError(f"missing fields: {sorted(missing)}")

    if not isinstance(record["amount"], (int, float)) or record["amount"] <= 0:
        raise ValidationError(f"invalid amount: {record.get('amount')!r}")

    try:
        ts = datetime.fromisoformat(record["timestamp"])
    except (TypeError, ValueError) as e:
        raise ValidationError(f"invalid timestamp: {record.get('timestamp')!r}") from e

    if record["is_fraud"] not in (0, 1):
        raise ValidationError(f"invalid is_fraud: {record.get('is_fraud')!r}")

    enriched = dict(record)
    # Timezone-aware to match the tz-aware `timestamp` field -- utcnow()
    # would give a naive datetime, making the two incomparable downstream.
    enriched["ingested_at"] = datetime.now(timezone.utc).isoformat()
    enriched["dt"] = ts.date().isoformat()
    return enriched
