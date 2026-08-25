"""Synthetic financial transaction generator for fraudsentry-lakehouse.

Produces a stream of plausible card-transaction events with a configurable
fraction of injected anomalies. Each account has a stable "home" city, and
most of its legitimate activity happens there -- that baseline is what
makes the `impossible_travel` fraud pattern detectable.

Three fraud patterns, each targeting a different feature:

    high_amount        far above the account's normal spend  -> amount,
                                                                amount_deviation
    odd_hour           1am-4am local                          -> hour_of_day
    impossible_travel  a distant city minutes after a         -> geo velocity
                       transaction at home

Every record carries `fraud_pattern` so evaluation can break recall down
per pattern instead of averaging three very different signals together.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import math
import random
import uuid
from collections import defaultdict
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

FRAUD_PATTERNS = ("high_amount", "odd_hour", "impossible_travel")

# How often a legitimate transaction happens in the account's home city.
# The remainder is ordinary travel -- deliberate noise, so geo velocity
# isn't a trivially perfect discriminator.
HOME_CITY_PROBABILITY = 0.9

# Minutes after the previous transaction that an impossible_travel fraud
# lands. Short enough that the implied speed is physically absurd.
IMPOSSIBLE_TRAVEL_GAP_MINUTES = (5, 90)


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
    fraud_pattern: str  # "none" for legitimate transactions

    def to_json(self) -> str:
        return json.dumps(asdict(self))


def haversine_miles(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Great-circle distance in miles. Mirrored in PySpark in
    02_silver_to_gold.py -- keep the two in sync."""
    radius = 3958.8
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dlambda / 2) ** 2
    return 2 * radius * math.asin(math.sqrt(a))


def home_city(account_id: str) -> tuple[str, float, float]:
    """Stable home city for an account. Hashed rather than stored so the
    streaming producer and the batch builder agree without shared state."""
    digest = int(hashlib.sha256(account_id.encode()).hexdigest(), 16)
    return CITIES[digest % len(CITIES)]


def _farthest_city(origin: tuple[str, float, float]) -> tuple[str, float, float]:
    _, lat, lon = origin
    return max(CITIES, key=lambda c: haversine_miles(lat, lon, c[1], c[2]))


def _pick_city(account_id: str) -> tuple[str, float, float]:
    if random.random() < HOME_CITY_PROBABILITY:
        return home_city(account_id)
    return random.choice(CITIES)


def _build(
    account_id: str,
    ts: datetime,
    amount: float,
    city: tuple[str, float, float],
    is_fraud: int,
    pattern: str,
) -> Transaction:
    name, lat, lon = city
    return Transaction(
        transaction_id=str(uuid.uuid4()),
        account_id=account_id,
        timestamp=ts.isoformat(),
        amount=round(amount, 2),
        merchant_category=random.choice(MERCHANT_CATEGORIES),
        city=name,
        lat=lat,
        lon=lon,
        is_fraud=is_fraud,
        fraud_pattern=pattern,
    )


def _legit(account_id: str, ts: datetime) -> Transaction:
    return _build(
        account_id, ts, random.uniform(5, 350), _pick_city(account_id), 0, "none"
    )


def _fraud(
    account_id: str,
    ts: datetime,
    pattern: str,
    previous: Transaction | None,
) -> Transaction:
    if pattern == "high_amount":
        return _build(
            account_id, ts, random.uniform(2000, 9000), _pick_city(account_id), 1, pattern
        )

    if pattern == "odd_hour":
        ts = ts.replace(hour=random.choice([1, 2, 3, 4]))
        return _build(
            account_id, ts, random.uniform(5, 350), _pick_city(account_id), 1, pattern
        )

    # impossible_travel: land in a distant city only minutes after the
    # previous transaction, so implied speed is thousands of mph. Needs a
    # predecessor to be impossible relative to -- callers fall back to
    # another pattern when there isn't one.
    origin = previous_city(previous) if previous else home_city(account_id)
    if previous is not None:
        ts = datetime.fromisoformat(previous.timestamp) + timedelta(
            minutes=random.uniform(*IMPOSSIBLE_TRAVEL_GAP_MINUTES)
        )
    return _build(
        account_id, ts, random.uniform(5, 350), _farthest_city(origin), 1, pattern
    )


def previous_city(txn: Transaction) -> tuple[str, float, float]:
    return (txn.city, txn.lat, txn.lon)


def generate_transaction(fraud_rate: float = 0.02, backdate_days: int = 0) -> Transaction:
    """Generate one standalone transaction, for the streaming producer.

    `backdate_days` spreads the timestamp over the last N days instead of
    pinning it to now; streaming callers want the default of 0.

    Note: with no per-account history available here, `impossible_travel`
    is emitted as "a transaction in the farthest city from home" -- the
    distance signal is present but not the tight time gap. `generate_batch`
    produces the fully-formed version.
    """
    account_id = f"acct_{random.randint(1000, 1200)}"
    ts = datetime.now(timezone.utc)
    if backdate_days > 0:
        ts -= timedelta(
            days=random.uniform(0, backdate_days), seconds=random.uniform(0, 86400)
        )
    if random.random() < fraud_rate:
        return _fraud(account_id, ts, random.choice(FRAUD_PATTERNS), None)
    return _legit(account_id, ts)


def generate_batch(
    n: int, fraud_rate: float = 0.02, backdate_days: int = 14
) -> list[Transaction]:
    """Generate n transactions as per-account timelines.

    Unlike `generate_transaction`, this builds each account's history in
    time order, which is what lets `impossible_travel` be positioned
    relative to a real predecessor.
    """
    account_count = max(1, n // 25)
    accounts = [f"acct_{1000 + i}" for i in range(account_count)]

    now = datetime.now(timezone.utc)
    schedule: dict[str, list[datetime]] = defaultdict(list)
    for _ in range(n):
        account = random.choice(accounts)
        offset = timedelta(
            days=random.uniform(0, backdate_days), seconds=random.uniform(0, 86400)
        ) if backdate_days > 0 else timedelta(0)
        schedule[account].append(now - offset)

    out: list[Transaction] = []
    for account, times in schedule.items():
        previous: Transaction | None = None
        for ts in sorted(times):
            if random.random() < fraud_rate:
                pattern = random.choice(FRAUD_PATTERNS)
                # impossible_travel is meaningless without a predecessor.
                if pattern == "impossible_travel" and previous is None:
                    pattern = "high_amount"
                txn = _fraud(account, ts, pattern, previous)
            else:
                txn = _legit(account, ts)
            out.append(txn)
            previous = txn

    out.sort(key=lambda t: t.timestamp)
    return out


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--count", type=int, default=1000)
    parser.add_argument("--fraud-rate", type=float, default=0.02)
    parser.add_argument("--out", type=str, default=None, help="Write NDJSON to this path instead of stdout")
    parser.add_argument(
        "--backdate-days",
        type=int,
        default=14,
        help="Spread timestamps over the last N days instead of pinning them to now",
    )
    args = parser.parse_args()

    batch = generate_batch(args.count, args.fraud_rate, args.backdate_days)
    lines = "\n".join(t.to_json() for t in batch)

    if args.out:
        with open(args.out, "w") as f:
            f.write(lines + "\n")
    else:
        print(lines)


if __name__ == "__main__":
    main()
