from dagster import op
from health_etl_dlt.health_etl_dlt import run_loading_app_data

@op
def run_dlt_pipeline():
    return run_loading_app_data()