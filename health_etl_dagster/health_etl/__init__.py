import os
from dagster import Definitions, load_assets_from_modules, define_asset_job
from dagster_duckdb import DuckDBResource

from . import assets

# Load all assets from the assets module
all_assets = load_assets_from_modules([assets])

# Define the DuckDB resource
duckdb_resource = DuckDBResource(database=assets.DUCKDB_PATH)

# Define a job that materializes all assets
all_assets_job = define_asset_job("all_assets_job", selection=all_assets)

defs = Definitions(
    assets=all_assets,
    resources={
        "duckdb_io_manager": duckdb_resource,
    },
    jobs=[all_assets_job],
)
