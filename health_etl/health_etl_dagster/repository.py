from dagster import Definitions
from .assets import run_load_date_time_app_results, run_load_integer_app_results, run_load_range_app_results

# from .jobs import dlt_job

defs = Definitions(
    assets=[
        run_load_date_time_app_results,
        run_load_integer_app_results,
        run_load_range_app_results
    ],
)
