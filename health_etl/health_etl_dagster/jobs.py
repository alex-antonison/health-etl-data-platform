from dagster import job
from .ops import run_dlt_pipeline

@job
def dlt_job():
    run_dlt_pipeline()