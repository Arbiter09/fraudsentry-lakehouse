"""Synthetic financial transaction generator for fraudsentry-lakehouse.

Produces a stream of plausible card-transaction events with a configurable
fraction of injected anomalies (fraud-like patterns: abnormal amount, odd
hour, impossible travel velocity). Used both for one-shot batch files and
as the payload source for the Kafka producer in ingestion/producer.py.
"""
from __future__ import annotations

import argparse
import json
import random
import uuid
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone

MERCHANT_CATEGORIES = [
    "grocery", "electronics", "travel", "dining", "fuel",
    "entertainment", "utilities", "healthcare", "clothing", "online_retail",
]

CITIES = [
    ("New York", 40.7128, -74.0060),
    ("Los Angeles", 34.0522, -118.2437),
    ("Chicago", 41.8781, -87.6298),
    ("Houston", 29.7604, -95.3698),
    ("Phoenix", 33.4484, -112.0740),
    ("Seattle", 47.6062, -122.3321),
]


@dataclass
class Transaction:
    transaction_id: str
    account_id: str
    timestamp: str
    amount: float
    merchant_category: str
    city: str
    lat: float
    lon: float
    is_fraud: int

    def to_json(self) -> str:
        return json.dumps(asdict(self))


def _normal_transaction(account_id: str, ts: datetime) -> Transaction:
    city, lat, lon = random.choice(CITIES)
    return Transaction(
        transaction_id=str(uuid.uuid4()),
        account_id=account_id,
        timestamp=ts.isoformat(),
        amount=round(random.uniform(5, 350), 2),
        merchant_category=random.choice(MERCHANT_CATEGORIES),
        city=city,
        lat=lat,
        lon=lon,
        is_fraud=0,
    )


def _fraud_transaction(account_id: str, ts: datetime) -> Transaction:
    """Injects one of a few simple fraud patterns."""
    pattern = random.choice(["high_amount", "odd_hour", "impossible_travel"])
    city, lat, lon = random.choice(CITIES)

    if pattern == "high_amount":
        amount = round(random.uniform(2000, 9000), 2)
    else:
        amount = round(random.uniform(5, 350), 2)

    if pattern == "odd_hour":
        ts = ts.replace(hour=random.choice([1, 2, 3, 4]))

    return Transaction(
        transaction_id=str(uuid.uuid4()),
        account_id=account_id,
        timestamp=ts.isoformat(),
        amount=amount,
        merchant_category=random.choice(MERCHANT_CATEGORIES),
        city=city,
        lat=lat,
        lon=lon,
        is_fraud=1,
    )


def generate_transaction(fraud_rate: float = 0.02) -> Transaction:
    account_id = f"acct_{random.randint(1000, 1200)}"
    ts = datetime.now(timezone.utc)
    if random.random() < fraud_rate:
        return _fraud_transaction(account_id, ts)
    return _normal_transaction(account_id, ts)


def generate_batch(n: int, fraud_rate: float = 0.02) -> list[Transaction]:
    return [generate_transaction(fraud_rate) for _ in range(n)]


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--count", type=int, default=1000)
    parser.add_argument("--fraud-rate", type=float, default=0.02)
    parser.add_argument("--out", type=str, default=None, help="Write NDJSON to this path instead of stdout")
    args = parser.parse_args()

    batch = generate_batch(args.count, args.fraud_rate)
    lines = "\n".join(t.to_json() for t in batch)

    if args.out:
        with open(args.out, "w") as f:
            f.write(lines + "\n")
    else:
        print(lines)


if __name__ == "__main__":
    main()
