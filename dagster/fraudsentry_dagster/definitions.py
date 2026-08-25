from dagster import Definitions
from dagster_dbt import DbtCliResource

from fraudsentry_dagster.assets import bronze_catalog_refreshed
from fraudsentry_dagster.databricks_asset import gold_scored_transactions
from fraudsentry_dagster.dbt_assets import fraudsentry_dbt_assets, fraudsentry_dbt_project

defs = Definitions(
    assets=[bronze_catalog_refreshed, fraudsentry_dbt_assets, gold_scored_transactions],
    resources={
        "dbt": DbtCliResource(project_dir=fraudsentry_dbt_project.project_dir),
    },
)
