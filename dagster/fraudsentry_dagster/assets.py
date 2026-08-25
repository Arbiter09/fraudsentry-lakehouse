"""Ingestion + Glue assets. dbt assets live in dbt_assets.py (loaded from
the dbt manifest); the Databricks scoring asset lives in
databricks_asset.py, which carries its own notes on Databricks Free
Edition's job quotas -- see that module's docstring.
"""
from __future__ import annotations

import os
import time

import boto3
from dagster import AssetExecutionContext, Failure, MaterializeResult, asset

GLUE_CRAWLER_NAME = os.environ.get("GLUE_CRAWLER_NAME", "fraudsentry-bronze-crawler")
POLL_SECONDS = 15


@asset(
    description=(
        "Triggers the Glue crawler over the S3 bronze prefix (infra/glue.tf) "
        "and blocks until it reaches READY, so the Glue Data Catalog reflects "
        "the latest files before Databricks/dbt read from it."
    ),
)
def bronze_catalog_refreshed(context: AssetExecutionContext) -> MaterializeResult:
    glue = boto3.client("glue")
    glue.start_crawler(Name=GLUE_CRAWLER_NAME)
    context.log.info(f"started Glue crawler {GLUE_CRAWLER_NAME}")

    while True:
        crawler = glue.get_crawler(Name=GLUE_CRAWLER_NAME)["Crawler"]
        if crawler["State"] == "READY":
            break
        context.log.info(f"crawler state={crawler['State']}, polling again in {POLL_SECONDS}s")
        time.sleep(POLL_SECONDS)

    last_crawl = crawler.get("LastCrawl", {})
    status = last_crawl.get("Status")
    if status not in (None, "SUCCEEDED"):
        raise Failure(f"Glue crawler finished with status={status!r}: {last_crawl.get('ErrorMessage')}")

    return MaterializeResult(metadata={"crawler_status": status or "SUCCEEDED"})
