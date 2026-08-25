"""Databricks scoring step, submitting the three notebooks as a one-time
job run via the Databricks SDK.

This was originally written defensively because Community Edition had no
usable Jobs API. CE was retired 2026-01-01, and its replacement --
Databricks Free Edition -- does support Jobs (capped at 5 concurrent job
tasks per account), so this asset is expected to work as intended.

Two Free Edition quotas to keep in mind if it fails: the 5-concurrent-task
cap, and the fact that only workspace-level APIs are available (no
account-level API access). See databricks/README.md.
"""
from __future__ import annotations

from dagster import AssetExecutionContext, Failure, MaterializeResult, asset

from fraudsentry_dagster.dbt_assets import fraudsentry_dbt_assets

NOTEBOOK_PATHS = [
    "/Repos/fraudsentry/01_bronze_to_silver",
    "/Repos/fraudsentry/02_silver_to_gold",
    "/Repos/fraudsentry/03_anomaly_detection",
]


@asset(
    deps=[fraudsentry_dbt_assets],
    description="Submits the bronze->silver->gold->scoring notebooks as a one-time Databricks job run.",
)
def gold_scored_transactions(context: AssetExecutionContext) -> MaterializeResult:
    from databricks.sdk import WorkspaceClient
    from databricks.sdk.service.jobs import NotebookTask, SubmitTask

    client = WorkspaceClient()
    tasks = [
        SubmitTask(task_key=f"step_{i}", notebook_task=NotebookTask(notebook_path=path))
        for i, path in enumerate(NOTEBOOK_PATHS)
    ]

    try:
        waiter = client.jobs.submit(run_name="fraudsentry-scoring-run", tasks=tasks)
        run = waiter.result()
    except Exception as e:
        context.log.warning(
            "Databricks job submission failed. On Free Edition, check the "
            "5-concurrent-job-task quota and that your PAT targets the "
            "workspace API (account-level APIs are unavailable). See "
            f"databricks/README.md. Original error: {e}"
        )
        raise Failure(f"Databricks job submission failed: {e}") from e

    return MaterializeResult(metadata={"databricks_run_id": run.run_id, "state": str(run.state)})
