"""Streams synthetic transactions onto the `transactions` Kafka topic.

Points at localhost:9092 (docker-compose Kafka) by default; pass
--bootstrap-servers to target a real MSK cluster during a demo session.
"""
from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

from kafka import KafkaProducer
from kafka.serializer import DefaultSerializer

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from data_generator.generate_transactions import generate_transaction

TOPIC = "transactions"


def run(bootstrap_servers: str, rate_per_sec: float, fraud_rate: float, count: int | None) -> None:
    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=DefaultSerializer(),
        key_serializer=DefaultSerializer(),
    )

    sent = 0
    delay = 1.0 / rate_per_sec if rate_per_sec > 0 else 0
    try:
        while count is None or sent < count:
            txn = generate_transaction(fraud_rate)
            producer.send(TOPIC, key=txn.account_id, value=txn.to_json())
            sent += 1
            if sent % 50 == 0:
                producer.flush()
                print(f"sent {sent} transactions", file=sys.stderr)
            if delay:
                time.sleep(delay)
    except KeyboardInterrupt:
        pass
    finally:
        producer.flush()
        producer.close()
        print(f"done. sent {sent} transactions total", file=sys.stderr)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bootstrap-servers", default="localhost:9092")
    parser.add_argument("--rate", type=float, default=5.0, help="transactions per second")
    parser.add_argument("--fraud-rate", type=float, default=0.02)
    parser.add_argument("--count", type=int, default=None, help="stop after N transactions (default: run forever)")
    args = parser.parse_args()
    run(args.bootstrap_servers, args.rate, args.fraud_rate, args.count)


if __name__ == "__main__":
    main()
