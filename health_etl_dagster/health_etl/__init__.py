import os
from dagster import Definitions, load_assets_from_modules
from dagster_dbt import load_assets_from_dbt_project, DbtCliResource

from . import assets

# Get the path to the dbt project
DBT_PROJECT_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "dbt")

# Load dbt assets
dbt_assets = load_assets_from_dbt_project(
    project_dir=DBT_PROJECT_DIR, profiles_dir=DBT_PROJECT_DIR, use_build_command=True
)

# Load other assets
raw_assets = load_assets_from_modules([assets])

# Combine all assets
all_assets = [*raw_assets, *dbt_assets]

defs = Definitions(
    assets=all_assets,
    resources={
        "dbt": DbtCliResource(project_dir=DBT_PROJECT_DIR),
    },
)
