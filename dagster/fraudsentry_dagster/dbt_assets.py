"""dagster-dbt integration: loads the fraudsentry_dbt project's manifest
and turns every model/test into a Dagster asset, so the dbt DAG (and its
test results) show up directly in the Dagster asset graph and lineage
view instead of as an opaque black-box step.
"""
from __future__ import annotations

import os
from pathlib import Path

from dagster import AssetExecutionContext
from dagster_dbt import DbtCliResource, DbtProject, dbt_assets

DBT_PROJECT_DIR = Path(__file__).resolve().parent.parent.parent / "dbt" / "fraudsentry_dbt"

fraudsentry_dbt_project = DbtProject(project_dir=os.fspath(DBT_PROJECT_DIR))
fraudsentry_dbt_project.prepare_if_dev()


@dbt_assets(manifest=fraudsentry_dbt_project.manifest_path)
def fraudsentry_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()
