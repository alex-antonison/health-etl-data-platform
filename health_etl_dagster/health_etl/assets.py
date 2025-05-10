import os
from datetime import datetime, timedelta
from typing import Dict, Any

import dlt
from dagster import AssetIn, asset
from dagster_duckdb import DuckDBResource
from dlt.sources.helpers import requests
from dlt.common.runtime.slack import send_slack_message

# Database connection parameters
POSTGRES_CONNECTION = {
    "host": "localhost",
    "port": 5433,
    "database": "healthetl",
    "user": "healthetl_user",
    "password": "healthetl_password",
}

DUCKDB_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(__file__))),
    "..",
    "..",
    "duckdb-database",
    "health_etl.duckdb",
)


def get_last_modified_time(pipeline, table_name: str) -> datetime:
    """Get the last modified time from the destination"""
    try:
        with pipeline.destination.client() as client:
            result = client.execute(f"""
                SELECT MAX(modified_time) as last_modified
                FROM {table_name}
            """).fetchone()
            return result[0] if result and result[0] else datetime.min
    except Exception:
        return datetime.min


def notify_schema_changes(load_info: Dict[str, Any], hook_url: str = None) -> None:
    """Notify about schema changes through Slack"""
    if not hook_url:
        return

    for package in load_info.load_packages:
        for table_name, table in package.schema_update.items():
            for column_name, column in table["columns"].items():
                send_slack_message(
                    hook_url,
                    message=(
                        f"\tTable updated: {table_name}: "
                        f"Column changed: {column_name}: "
                        f"{column['data_type']}"
                    ),
                )


@dlt.resource(
    schema_contract={"tables": "evolve", "columns": "evolve", "data_type": "freeze"}
)
def app_results_resource(last_modified: datetime):
    """Resource for app results with schema contract"""
    return dlt.resources.postgres(
        connection_string=f"postgresql://{POSTGRES_CONNECTION['user']}:{POSTGRES_CONNECTION['password']}@{POSTGRES_CONNECTION['host']}:{POSTGRES_CONNECTION['port']}/{POSTGRES_CONNECTION['database']}",
        query=f"""
            SELECT ar.*, 
                   CASE 
                       WHEN ar.polymorphic_type = 'IntegerAppResult' THEN iar.value
                       WHEN ar.polymorphic_type = 'DateTimeAppResult' THEN dar.value
                       WHEN ar.polymorphic_type = 'RangeAppResult' THEN rar.from_value || '-' || rar.to_value
                   END as metric_value
            FROM app_results ar
            LEFT JOIN integer_app_results iar ON ar.id = iar.app_result_id
            LEFT JOIN datetime_app_results dar ON ar.id = dar.app_result_id
            LEFT JOIN range_app_results rar ON ar.id = rar.app_result_id
            WHERE ar.modified_time > '{last_modified}'
        """,
    )


@dlt.resource(
    schema_contract={"tables": "evolve", "columns": "evolve", "data_type": "freeze"}
)
def integer_app_results_resource(last_modified: datetime):
    """Resource for integer app results with schema contract"""
    return dlt.resources.postgres(
        connection_string=f"postgresql://{POSTGRES_CONNECTION['user']}:{POSTGRES_CONNECTION['password']}@{POSTGRES_CONNECTION['host']}:{POSTGRES_CONNECTION['port']}/{POSTGRES_CONNECTION['database']}",
        query=f"""
            SELECT iar.*
            FROM integer_app_results iar
            JOIN app_results ar ON ar.id = iar.app_result_id
            WHERE ar.modified_time > '{last_modified}'
        """,
    )


@dlt.resource(
    schema_contract={"tables": "evolve", "columns": "evolve", "data_type": "freeze"}
)
def datetime_app_results_resource(last_modified: datetime):
    """Resource for datetime app results with schema contract"""
    return dlt.resources.postgres(
        connection_string=f"postgresql://{POSTGRES_CONNECTION['user']}:{POSTGRES_CONNECTION['password']}@{POSTGRES_CONNECTION['host']}:{POSTGRES_CONNECTION['port']}/{POSTGRES_CONNECTION['database']}",
        query=f"""
            SELECT dar.*
            FROM datetime_app_results dar
            JOIN app_results ar ON ar.id = dar.app_result_id
            WHERE ar.modified_time > '{last_modified}'
        """,
    )


@dlt.resource(
    schema_contract={"tables": "evolve", "columns": "evolve", "data_type": "freeze"}
)
def range_app_results_resource(last_modified: datetime):
    """Resource for range app results with schema contract"""
    return dlt.resources.postgres(
        connection_string=f"postgresql://{POSTGRES_CONNECTION['user']}:{POSTGRES_CONNECTION['password']}@{POSTGRES_CONNECTION['host']}:{POSTGRES_CONNECTION['port']}/{POSTGRES_CONNECTION['database']}",
        query=f"""
            SELECT rar.*
            FROM range_app_results rar
            JOIN app_results ar ON ar.id = rar.app_result_id
            WHERE ar.modified_time > '{last_modified}'
        """,
    )


@asset(
    group_name="raw_data",
    description="Raw data from PostgreSQL database with incremental loading and schema evolution support",
    io_manager_key="duckdb_io_manager",
)
def raw_app_results():
    """Load raw app results data from PostgreSQL to DuckDB with incremental loading and schema evolution support"""
    pipeline = dlt.pipeline(
        pipeline_name="health_etl",
        destination="duckdb",
        dataset_name="raw",
        full_refresh=False,
    )

    # Get the last modified time from the destination
    last_modified = get_last_modified_time(pipeline, "app_results")

    # Load data from PostgreSQL with incremental query and schema contract
    load_info = pipeline.run(
        app_results_resource(last_modified),
        table_name="app_results",
        write_disposition="merge",
    )

    # Notify about schema changes
    notify_schema_changes(load_info)

    return load_info


@asset(
    group_name="raw_data",
    description="Raw integer app results data from PostgreSQL to DuckDB with schema evolution support",
    io_manager_key="duckdb_io_manager",
)
def raw_integer_app_results():
    """Load raw integer app results data from PostgreSQL to DuckDB with schema evolution support"""
    pipeline = dlt.pipeline(
        pipeline_name="health_etl",
        destination="duckdb",
        dataset_name="raw",
        full_refresh=False,
    )

    # Get the last modified time from the destination
    last_modified = get_last_modified_time(pipeline, "integer_app_results")

    load_info = pipeline.run(
        integer_app_results_resource(last_modified),
        table_name="integer_app_results",
        write_disposition="merge",
    )

    # Notify about schema changes
    notify_schema_changes(load_info)

    return load_info


@asset(
    group_name="raw_data",
    description="Raw datetime app results data from PostgreSQL to DuckDB with schema evolution support",
    io_manager_key="duckdb_io_manager",
)
def raw_datetime_app_results():
    """Load raw datetime app results data from PostgreSQL to DuckDB with schema evolution support"""
    pipeline = dlt.pipeline(
        pipeline_name="health_etl",
        destination="duckdb",
        dataset_name="raw",
        full_refresh=False,
    )

    # Get the last modified time from the destination
    last_modified = get_last_modified_time(pipeline, "datetime_app_results")

    load_info = pipeline.run(
        datetime_app_results_resource(last_modified),
        table_name="datetime_app_results",
        write_disposition="merge",
    )

    # Notify about schema changes
    notify_schema_changes(load_info)

    return load_info


@asset(
    group_name="raw_data",
    description="Raw range app results data from PostgreSQL to DuckDB with schema evolution support",
    io_manager_key="duckdb_io_manager",
)
def raw_range_app_results():
    """Load raw range app results data from PostgreSQL to DuckDB with schema evolution support"""
    pipeline = dlt.pipeline(
        pipeline_name="health_etl",
        destination="duckdb",
        dataset_name="raw",
        full_refresh=False,
    )

    # Get the last modified time from the destination
    last_modified = get_last_modified_time(pipeline, "range_app_results")

    load_info = pipeline.run(
        range_app_results_resource(last_modified),
        table_name="range_app_results",
        write_disposition="merge",
    )

    # Notify about schema changes
    notify_schema_changes(load_info)

    return load_info
