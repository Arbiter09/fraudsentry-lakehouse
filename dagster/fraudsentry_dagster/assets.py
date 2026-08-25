"""Ingestion + Glue assets. dbt assets live in dbt_assets.py (loaded from
the dbt manifest); the Databricks scoring asset lives in
databricks_asset.py, which carries its own notes on Databricks Free
Edition's job quotas -- see that module's docstring.
"""
import os
import time

from dagster import AssetExecutionContext, Failure, MaterializeResult, asset

GLUE_CRAWLER_NAME = os.environ.get("GLUE_CRAWLER_NAME", "fraudsentry-bronze-crawler")
POLL_SECONDS = 15


def aws_enabled() -> bool:
    """Whether to actually call AWS.

    The Glue crawler is the one asset that needs live AWS infrastructure
    (infra/glue.tf). Everything else in this pipeline -- the Databricks
    notebooks, dbt, the quality checks -- runs against Unity Catalog and
    needs no AWS at all.

    Rather than make the whole graph unrunnable until `terraform apply`
    has happened, this defaults to OFF: the asset materializes as an
    explicit no-op that records why it skipped. Set FRAUDSENTRY_AWS=1
    once the Glue crawler exists to switch to the real call.
    """
    return os.environ.get("FRAUDSENTRY_AWS", "").lower() in ("1", "true", "yes")


@asset(
    description=(
        "Triggers the Glue crawler over the S3 bronze prefix (infra/glue.tf) "
        "and blocks until it reaches READY, so the Glue Data Catalog reflects "
        "the latest files before Databricks/dbt read from it. Skips as a no-op "
        "unless FRAUDSENTRY_AWS=1 -- see aws_enabled()."
    ),
)
def bronze_catalog_refreshed(context: AssetExecutionContext) -> MaterializeResult:
    if not aws_enabled():
        context.log.info(
            "FRAUDSENTRY_AWS is not set -- skipping the Glue crawler. The "
            "bronze data is read directly from the Unity Catalog Volume, so "
            "downstream assets are unaffected. Set FRAUDSENTRY_AWS=1 after "
            "`terraform apply` to exercise the real crawler."
        )
        return MaterializeResult(
            metadata={
                "mode": "skipped (no AWS)",
                "reason": "FRAUDSENTRY_AWS unset; bronze served from UC Volume",
            }
        )

    import boto3  # imported lazily so the graph loads without AWS deps configured

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

    return MaterializeResult(metadata={"mode": "live", "crawler_status": status or "SUCCEEDED"})
