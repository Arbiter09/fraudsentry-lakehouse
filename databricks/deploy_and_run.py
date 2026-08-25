"""Import the notebooks into a Databricks workspace and run them in order.

Auth comes from the standard Databricks config chain -- `~/.databrickscfg`
or the DATABRICKS_HOST / DATABRICKS_TOKEN environment variables. This
script never reads or prints a token itself.

    python3 databricks/deploy_and_run.py --import-only
    python3 databricks/deploy_and_run.py            # import, then run 01-03
    python3 databricks/deploy_and_run.py --run-only --notebook 01_bronze_to_silver

Free Edition notes: jobs are capped at 5 concurrent tasks, so the
notebooks are submitted as a single sequential job rather than in
parallel. Only workspace-level APIs are used (no account-level calls).
"""
from __future__ import annotations

import argparse
import base64
import sys
from pathlib import Path

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import NotebookTask, SubmitTask, TaskDependency
from databricks.sdk.service.workspace import ImportFormat, Language

NOTEBOOK_DIR = Path(__file__).resolve().parent / "notebooks"
ORDER = ["00_config", "01_bronze_to_silver", "02_silver_to_gold", "03_anomaly_detection", "99_verify"]
RUNNABLE = ORDER[1:-1]  # 00_config is %run-included; 99_verify runs on demand


def workspace_dir(client: WorkspaceClient, override: str | None) -> str:
    """Resolve the workspace folder to import into.

    Looking up the current user needs an extra token scope (`iam`), which
    is otherwise unnecessary -- so treat it as best-effort and let
    --target-dir bypass it entirely.
    """
    if override:
        return override.rstrip("/")
    try:
        user = client.current_user.me().user_name
        return f"/Users/{user}/fraudsentry"
    except Exception as e:
        sys.exit(
            f"could not look up current user ({e}).\n"
            "Either grant the token the `iam` scope, or re-run with "
            "--target-dir /Users/<your-email>/fraudsentry"
        )


def do_import(client: WorkspaceClient, target_dir: str) -> None:
    client.workspace.mkdirs(target_dir)
    for name in ORDER:
        local = NOTEBOOK_DIR / f"{name}.py"
        if not local.exists():
            sys.exit(f"missing notebook: {local}")
        remote = f"{target_dir}/{name}"
        client.workspace.import_(
            path=remote,
            content=base64.b64encode(local.read_bytes()).decode(),
            format=ImportFormat.SOURCE,
            language=Language.PYTHON,
            overwrite=True,
        )
        print(f"imported {remote}")


def do_run(client: WorkspaceClient, target_dir: str, only: str | None) -> int:
    names = [only] if only else RUNNABLE
    tasks = []
    for i, name in enumerate(names):
        task = SubmitTask(
            task_key=name,
            notebook_task=NotebookTask(notebook_path=f"{target_dir}/{name}"),
        )
        # Chain sequentially -- 02 needs 01's silver table, 03 needs 02's gold.
        if i > 0:
            task.depends_on = [TaskDependency(task_key=names[i - 1])]
        tasks.append(task)

    print(f"submitting run with {len(tasks)} task(s): {', '.join(names)}")
    waiter = client.jobs.submit(run_name="fraudsentry-pipeline", tasks=tasks)
    print(f"run_id={waiter.run_id} -- waiting (this can take several minutes on serverless)")

    run = waiter.result()
    print(f"\nrun finished: {run.state.life_cycle_state} / {run.state.result_state}")
    if run.state.state_message:
        print(f"message: {run.state.state_message}")

    failed = 0
    for task in run.tasks or []:
        state = task.state
        status = state.result_state if state else "UNKNOWN"
        print(f"\n--- task {task.task_key}: {status} ---")
        if state and state.state_message:
            print(state.state_message)
        try:
            output = client.jobs.get_run_output(task.run_id)
            if output.error:
                failed += 1
                print(f"ERROR: {output.error}")
            if output.error_trace:
                print(output.error_trace)
            if output.notebook_output and output.notebook_output.result:
                print(f"result: {output.notebook_output.result}")
            if output.logs:
                print("logs (tail):")
                print("\n".join(output.logs.splitlines()[-40:]))
        except Exception as e:
            print(f"(could not fetch output: {e})")

    return 1 if failed or (run.state.result_state and "SUCCESS" not in str(run.state.result_state)) else 0


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--import-only", action="store_true")
    parser.add_argument("--run-only", action="store_true")
    parser.add_argument("--notebook", help="run just this one notebook, e.g. 01_bronze_to_silver")
    parser.add_argument(
        "--target-dir",
        help="workspace folder to import into; avoids needing the `iam` token scope",
    )
    args = parser.parse_args()

    client = WorkspaceClient()
    target = workspace_dir(client, args.target_dir)
    print(f"target workspace dir: {target}\n")

    if not args.run_only:
        do_import(client, target)
        print()

    if args.import_only:
        return

    sys.exit(do_run(client, target, args.notebook))


if __name__ == "__main__":
    main()
