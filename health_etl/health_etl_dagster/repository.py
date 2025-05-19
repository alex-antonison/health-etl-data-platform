from dagster import Definitions
from .assets import (
    run_load_date_time_app_results,
    run_load_integer_app_results,
    run_load_range_app_results,
)
from pathlib import Path
from dagster_dbt import DbtCliResource, DbtProject, dbt_assets
import dagster as dg

RELATIVE_PATH_TO_MY_DBT_PROJECT = "../dbt"

dbt_project = DbtProject(
    project_dir=Path(__file__)
    .joinpath("..", RELATIVE_PATH_TO_MY_DBT_PROJECT)
    .resolve(),
)
dbt_project.prepare_if_dev()


@dbt_assets(manifest=dbt_project.manifest_path)
def dbt_assets(context: dg.AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()


defs = Definitions(
    assets=[
        run_load_date_time_app_results,
        run_load_integer_app_results,
        run_load_range_app_results,
        dbt_assets,
    ],
    resources={
        "dbt": DbtCliResource(project_dir=dbt_project),
    },
)
