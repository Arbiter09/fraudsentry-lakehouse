"""Local stand-in for the Lambda consumer: reads from the `transactions`
Kafka topic, validates each record, and writes valid ones to a local
bronze directory as date-partitioned NDJSON (swap in pyarrow/parquet once
schemas stabilize -- kept as NDJSON here for zero-dependency local runs).

Invalid records go to a sibling dead-letter file instead of failing the
batch, mirroring what the Lambda handler does against a DLQ in AWS.
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from kafka import KafkaConsumer

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from common.validation import ValidationError, validate_transaction

TOPIC = "transactions"


def run(bootstrap_servers: str, out_dir: Path, group_id: str) -> None:
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=bootstrap_servers,
        group_id=group_id,
        auto_offset_reset="earliest",
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    )

    out_dir.mkdir(parents=True, exist_ok=True)
    dlq_path = out_dir / "_dead_letter.ndjson"
    written, rejected = 0, 0

    print(f"consuming from {bootstrap_servers} topic={TOPIC} -> {out_dir}", file=sys.stderr)
    try:
        for msg in consumer:
            try:
                record = validate_transaction(msg.value)
            except ValidationError as e:
                rejected += 1
                with open(dlq_path, "a") as f:
                    f.write(json.dumps({"error": str(e), "raw": msg.value}) + "\n")
                continue

            partition_dir = out_dir / f"dt={record['dt']}"
            partition_dir.mkdir(parents=True, exist_ok=True)
            with open(partition_dir / "bronze.ndjson", "a") as f:
                f.write(json.dumps(record) + "\n")
            written += 1

            if (written + rejected) % 50 == 0:
                print(f"written={written} rejected={rejected}", file=sys.stderr)
    except KeyboardInterrupt:
        pass
    finally:
        print(f"done. written={written} rejected={rejected}", file=sys.stderr)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bootstrap-servers", default="localhost:9092")
    parser.add_argument("--out-dir", default="data/bronze")
    parser.add_argument("--group-id", default="fraudsentry-local-consumer")
    args = parser.parse_args()
    run(args.bootstrap_servers, Path(args.out_dir), args.group_id)


if __name__ == "__main__":
    main()
