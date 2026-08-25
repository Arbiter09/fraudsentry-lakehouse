import shutil
import sys
from pathlib import Path

from dagster import Definitions
from dagster_dbt import DbtCliResource

from fraudsentry_dagster.assets import bronze_catalog_refreshed
from fraudsentry_dagster.databricks_asset import gold_scored_transactions
from fraudsentry_dagster.dbt_assets import fraudsentry_dbt_assets, fraudsentry_dbt_project


def _dbt_executable() -> str:
    """Locate the dbt binary.

    DbtCliResource defaults to bare "dbt" and requires it on PATH, which
    it isn't when Dagster is launched as `.venv/bin/python -m ...` without
    the venv activated. Prefer the dbt sitting next to the running
    interpreter, then fall back to PATH.
    """
    candidate = Path(sys.executable).parent / "dbt"
    if candidate.exists():
        return str(candidate)
    return shutil.which("dbt") or "dbt"


defs = Definitions(
    assets=[bronze_catalog_refreshed, fraudsentry_dbt_assets, gold_scored_transactions],
    resources={
        "dbt": DbtCliResource(
            project_dir=fraudsentry_dbt_project.project_dir,
            profiles_dir=fraudsentry_dbt_project.project_dir,
            dbt_executable=_dbt_executable(),
        ),
    },
)
