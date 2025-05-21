from dagster import asset
import dlt
from health_etl_dlt.health_etl_dlt import (
    load_date_time_app_results,
    load_integer_app_results,
    load_range_app_results,
)


@asset
def stg_date_time_app_results(context):
    pipeline = dlt.pipeline(
        pipeline_name="healthetl_pipeline",
        destination="postgres",
        dataset_name="healthetl_data",
        pipelines_dir="/tmp/healthetl_pipeline/date_time_app_results",
    )

    load_info = pipeline.run(load_date_time_app_results())
    context.log.info(load_info)

    trace = pipeline.last_trace
    row_counts = trace.last_normalize_info.row_counts
    context.log.info(f"Rows loaded: {row_counts}")

    return load_info


@asset
def stg_integer_app_results(context):
    pipeline = dlt.pipeline(
        pipeline_name="healthetl_pipeline",
        destination="postgres",
        dataset_name="healthetl_data",
        pipelines_dir="/tmp/healthetl_pipeline/integer_app_results",
    )

    load_info = pipeline.run(load_integer_app_results())

    context.log.info(load_info)

    trace = pipeline.last_trace
    row_counts = trace.last_normalize_info.row_counts
    context.log.info(f"Rows loaded: {row_counts}")
    return load_info


@asset
def stg_range_app_results(context):
    pipeline = dlt.pipeline(
        pipeline_name="healthetl_pipeline",
        destination="postgres",
        dataset_name="healthetl_data",
        pipelines_dir="/tmp/healthetl_pipeline/range_app_results",
    )

    load_info = pipeline.run(load_range_app_results())
    context.log.info(load_info)

    trace = pipeline.last_trace
    row_counts = trace.last_normalize_info.row_counts
    context.log.info(f"Rows loaded: {row_counts}")
    return load_info
