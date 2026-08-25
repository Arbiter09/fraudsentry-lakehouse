"""Produce a bronze-shaped sample file without running Kafka.

`generate_transactions.py` emits *raw* transactions. The bronze layer
additionally carries the `dt` and `ingested_at` fields that
`common.validation.validate_transaction` attaches at ingestion time --
so raw generator output is NOT a drop-in substitute for real bronze
data (the Databricks notebooks partition by `dt` and would fail on it).

This script closes that gap by piping generated transactions through the
exact same validation function the local consumer and the Lambda handler
use, so the output is byte-for-byte the shape those two produce. Useful
for seeding a Databricks Unity Catalog Volume when you want to exercise
the notebooks without standing up Kafka or AWS.

    python3 data_generator/make_bronze_sample.py --count 5000 --out-dir bronze_sample/
"""
from __future__ import annotations

import argparse
import json
import sys
from collections import Counter
from dataclasses import asdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from common.validation import ValidationError, validate_transaction
from data_generator.generate_transactions import generate_batch


def build(count: int, fraud_rate: float, out_dir: Path, single_file: bool, backdate_days: int) -> None:
    by_partition: dict[str, list[dict]] = {}
    rejected = 0

    # generate_batch (not generate_transaction in a loop) -- it builds
    # per-account timelines, which is what makes the impossible_travel
    # pattern land minutes after a real predecessor.
    for txn in generate_batch(count, fraud_rate, backdate_days):
        raw = asdict(txn)
        try:
            record = validate_transaction(raw)
        except ValidationError as e:  # shouldn't happen, but don't silently drop
            print(f"warning: generated record failed validation: {e}", file=sys.stderr)
            rejected += 1
            continue
        by_partition.setdefault(record["dt"], []).append(record)

    out_dir.mkdir(parents=True, exist_ok=True)

    if single_file:
        # Flat file -- simplest thing to drag into a UC Volume via the UI.
        path = out_dir / "bronze.ndjson"
        with open(path, "w") as f:
            for records in by_partition.values():
                for r in records:
                    f.write(json.dumps(r) + "\n")
        written = sum(len(v) for v in by_partition.values())
        print(f"wrote {written} records -> {path}")
    else:
        # Hive-style dt= partitions, matching what the consumer/Lambda write.
        for dt, records in by_partition.items():
            partition_dir = out_dir / f"dt={dt}"
            partition_dir.mkdir(parents=True, exist_ok=True)
            path = partition_dir / "bronze.ndjson"
            with open(path, "w") as f:
                for r in records:
                    f.write(json.dumps(r) + "\n")
            print(f"wrote {len(records)} records -> {path}")

    all_records = [r for records in by_partition.values() for r in records]
    fraud = Counter(r["is_fraud"] for r in all_records)
    patterns = Counter(r["fraud_pattern"] for r in all_records if r["is_fraud"])
    total = len(all_records)
    if total:
        print(f"total={total} fraud={fraud[1]} rate={fraud[1] / total:.1%} rejected={rejected}")
        print(f"fraud patterns: {dict(patterns)}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--count", type=int, default=5000)
    parser.add_argument("--fraud-rate", type=float, default=0.03)
    parser.add_argument("--out-dir", default="bronze_sample")
    parser.add_argument(
        "--backdate-days",
        type=int,
        default=14,
        help="Spread timestamps over the last N days so the daily/rolling "
             "models downstream have real day-over-day variation.",
    )
    parser.add_argument(
        "--single-file",
        action="store_true",
        help="Write one flat bronze.ndjson instead of dt= partition dirs "
             "(easier to upload through the Databricks UI).",
    )
    args = parser.parse_args()
    build(args.count, args.fraud_rate, Path(args.out_dir), args.single_file, args.backdate_days)


if __name__ == "__main__":
    main()
