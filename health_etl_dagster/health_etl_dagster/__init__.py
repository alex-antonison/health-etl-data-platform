from dagster import Definitions, load_assets_from_modules
from dagster_duckdb import DuckDBResource

from . import assets

# Define the DuckDB resource
duckdb_resource = DuckDBResource(
    database="data/health_etl.duckdb"  # This will create/use a DuckDB file in the data directory
)

# Load all assets from the assets module
all_assets = load_assets_from_modules([assets])

# Create the Dagster definitions
defs = Definitions(assets=all_assets, resources={"duckdb": duckdb_resource})
