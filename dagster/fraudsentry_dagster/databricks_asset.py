"""Databricks scoring step, kept separate from assets.py because whether
this can actually run programmatically is unresolved (see the Phase 0
spike note in the project plan and databricks/README.md): Community
Edition has historically restricted/disallowed Jobs API submission.

This asset makes a real attempt via the Databricks SDK. If your
workspace rejects it, that's the expected Community Edition limitation,
not a bug here -- run the three notebooks manually (or on Databricks'
own notebook scheduler) instead, and treat this asset as documentation
of the intended automation for a paid workspace.
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
            "Databricks job submission failed. If you're on Community "
            "Edition this is the expected API restriction (see "
            "databricks/README.md), not a bug in this asset -- run the "
            f"notebooks manually instead. Original error: {e}"
        )
        raise Failure(f"Databricks job submission failed: {e}") from e

    return MaterializeResult(metadata={"databricks_run_id": run.run_id, "state": str(run.state)})
