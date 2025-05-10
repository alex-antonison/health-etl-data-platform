import os
from datetime import datetime, timedelta
from typing import Dict, Any

import dlt
from dagster import AssetIn, asset
from dagster_duckdb import DuckDBResource
from dlt.sources.helpers import requests

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


def get_table_schema(pipeline, table_name: str) -> Dict[str, Any]:
    """Get the current schema of a table in DuckDB"""
    try:
        with pipeline.destination.client() as client:
            result = client.execute(f"""
                SELECT column_name, data_type 
                FROM information_schema.columns 
                WHERE table_name = '{table_name}'
            """).fetchall()
            return {row[0]: row[1] for row in result}
    except Exception:
        return {}


def get_source_schema(pipeline, table_name: str) -> Dict[str, Any]:
    """Get the schema of a table in PostgreSQL"""
    try:
        with pipeline.source.client() as client:
            result = client.execute(f"""
                SELECT column_name, data_type 
                FROM information_schema.columns 
                WHERE table_name = '{table_name}'
            """).fetchall()
            return {row[0]: row[1] for row in result}
    except Exception:
        return {}


def get_incremental_query(
    pipeline,
    table_name: str,
    last_modified_time: datetime,
    schema: Dict[str, Any] = None,
) -> str:
    """Generate incremental query based on modified_time and current schema"""
    base_query = f"""
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
        WHERE ar.modified_time > '{last_modified_time}'
    """

    # Add any new columns that exist in source but not in destination
    if schema:
        # Get source schema
        source_schema = get_source_schema(pipeline, table_name)
        source_columns = set(source_schema.keys())
        dest_columns = set(schema.keys())
        new_columns = source_columns - dest_columns

        if new_columns:
            # Add new columns with NULL values
            for col in new_columns:
                base_query = base_query.replace(
                    "SELECT ar.*", f"SELECT ar.*, NULL as {col}"
                )

    return base_query


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

    # Get the last modified time and current schema from the destination
    last_modified = get_last_modified_time(pipeline, "app_results")
    current_schema = get_table_schema(pipeline, "app_results")

    # Load data from PostgreSQL with incremental query and schema evolution support
    load_info = pipeline.run(
        dlt.resources.postgres(
            connection_string=f"postgresql://{POSTGRES_CONNECTION['user']}:{POSTGRES_CONNECTION['password']}@{POSTGRES_CONNECTION['host']}:{POSTGRES_CONNECTION['port']}/{POSTGRES_CONNECTION['database']}",
            query=get_incremental_query(
                pipeline, "app_results", last_modified, current_schema
            ),
        ),
        table_name="app_results",
        write_disposition="merge",
    )

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

    # Get the last modified time and current schema from the destination
    last_modified = get_last_modified_time(pipeline, "integer_app_results")
    current_schema = get_table_schema(pipeline, "integer_app_results")
    source_schema = get_source_schema(pipeline, "integer_app_results")

    # Build query with schema evolution support
    base_query = f"""
        SELECT iar.*
        FROM integer_app_results iar
        JOIN app_results ar ON ar.id = iar.app_result_id
        WHERE ar.modified_time > '{last_modified}'
    """

    # Add any new columns that exist in source but not in destination
    if current_schema:
        source_columns = set(source_schema.keys())
        dest_columns = set(current_schema.keys())
        new_columns = source_columns - dest_columns

        if new_columns:
            for col in new_columns:
                base_query = base_query.replace(
                    "SELECT iar.*", f"SELECT iar.*, NULL as {col}"
                )

    load_info = pipeline.run(
        dlt.resources.postgres(
            connection_string=f"postgresql://{POSTGRES_CONNECTION['user']}:{POSTGRES_CONNECTION['password']}@{POSTGRES_CONNECTION['host']}:{POSTGRES_CONNECTION['port']}/{POSTGRES_CONNECTION['database']}",
            query=base_query,
        ),
        table_name="integer_app_results",
        write_disposition="merge",
    )

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

    # Get the last modified time and current schema from the destination
    last_modified = get_last_modified_time(pipeline, "datetime_app_results")
    current_schema = get_table_schema(pipeline, "datetime_app_results")
    source_schema = get_source_schema(pipeline, "datetime_app_results")

    # Build query with schema evolution support
    base_query = f"""
        SELECT dar.*
        FROM datetime_app_results dar
        JOIN app_results ar ON ar.id = dar.app_result_id
        WHERE ar.modified_time > '{last_modified}'
    """

    # Add any new columns that exist in source but not in destination
    if current_schema:
        source_columns = set(source_schema.keys())
        dest_columns = set(current_schema.keys())
        new_columns = source_columns - dest_columns

        if new_columns:
            for col in new_columns:
                base_query = base_query.replace(
                    "SELECT dar.*", f"SELECT dar.*, NULL as {col}"
                )

    load_info = pipeline.run(
        dlt.resources.postgres(
            connection_string=f"postgresql://{POSTGRES_CONNECTION['user']}:{POSTGRES_CONNECTION['password']}@{POSTGRES_CONNECTION['host']}:{POSTGRES_CONNECTION['port']}/{POSTGRES_CONNECTION['database']}",
            query=base_query,
        ),
        table_name="datetime_app_results",
        write_disposition="merge",
    )

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

    # Get the last modified time and current schema from the destination
    last_modified = get_last_modified_time(pipeline, "range_app_results")
    current_schema = get_table_schema(pipeline, "range_app_results")
    source_schema = get_source_schema(pipeline, "range_app_results")

    # Build query with schema evolution support
    base_query = f"""
        SELECT rar.*
        FROM range_app_results rar
        JOIN app_results ar ON ar.id = rar.app_result_id
        WHERE ar.modified_time > '{last_modified}'
    """

    # Add any new columns that exist in source but not in destination
    if current_schema:
        source_columns = set(source_schema.keys())
        dest_columns = set(current_schema.keys())
        new_columns = source_columns - dest_columns

        if new_columns:
            for col in new_columns:
                base_query = base_query.replace(
                    "SELECT rar.*", f"SELECT rar.*, NULL as {col}"
                )

    load_info = pipeline.run(
        dlt.resources.postgres(
            connection_string=f"postgresql://{POSTGRES_CONNECTION['user']}:{POSTGRES_CONNECTION['password']}@{POSTGRES_CONNECTION['host']}:{POSTGRES_CONNECTION['port']}/{POSTGRES_CONNECTION['database']}",
            query=base_query,
        ),
        table_name="range_app_results",
        write_disposition="merge",
    )

    return load_info
