from dagster import Definitions
from dagster_duckdb_pandas import DuckDBPandasIOManager

from .jobs import dlt_job

defs = Definitions(
    jobs=[dlt_job],
    # resources={"io_manager": DuckDBPandasIOManager(database="healthetl_pipeline.duckdb")},    
)
