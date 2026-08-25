"""AWS Lambda handler triggered by an MSK event-source mapping.

Kafka records arrive base64-encoded, batched by partition. Each record is
validated with the same rules the local consumer uses (common/validation.py)
and valid ones are written to the S3 bronze prefix, partitioned by date.
Invalid records are pushed to an SQS dead-letter queue instead of failing
the whole batch (partial-batch failure would just cause Kafka to redeliver
the same poison record forever).
"""
from __future__ import annotations

import base64
import json
import os
from datetime import datetime, timezone

import boto3

from common.validation import ValidationError, validate_transaction

s3 = boto3.client("s3")
sqs = boto3.client("sqs")

BRONZE_BUCKET = os.environ["BRONZE_BUCKET"]
DLQ_URL = os.environ.get("DLQ_URL")


def handler(event, context):
    written, rejected = 0, 0

    for topic_partition, records in event.get("records", {}).items():
        for kafka_record in records:
            raw = base64.b64decode(kafka_record["value"]).decode("utf-8")
            try:
                payload = json.loads(raw)
                record = validate_transaction(payload)
            except (ValidationError, json.JSONDecodeError) as e:
                rejected += 1
                _send_to_dlq(raw, str(e))
                continue

            _write_bronze(record)
            written += 1

    return {"written": written, "rejected": rejected}


def _write_bronze(record: dict) -> None:
    key = (
        f"bronze/transactions/dt={record['dt']}/"
        f"{record['transaction_id']}.json"
    )
    s3.put_object(
        Bucket=BRONZE_BUCKET,
        Key=key,
        Body=json.dumps(record).encode("utf-8"),
        ContentType="application/json",
    )


def _send_to_dlq(raw: str, error: str) -> None:
    if not DLQ_URL:
        return
    sqs.send_message(
        QueueUrl=DLQ_URL,
        MessageBody=json.dumps({
            "error": error,
            "raw": raw,
            "failed_at": datetime.now(timezone.utc).isoformat(),
        }),
    )
