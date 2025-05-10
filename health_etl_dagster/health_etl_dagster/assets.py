import os
from datetime import datetime
from typing import Dict, Any

import dlt
from dagster import asset, MaterializeResult
from dagster_duckdb import DuckDBResource

# Database connection parameters
POSTGRES_CONNECTION = {
    "host": "localhost",
    "port": 5433,
    "database": "healthetl",
    "user": "healthetl_user",
    "password": "healthetl_password",
}


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
    description="Integer app results data from PostgreSQL to DuckDB with incremental loading",
)
def integer_app_results(duckdb: DuckDBResource):
    """Load integer app results data from PostgreSQL to DuckDB with incremental loading"""
    pipeline = dlt.pipeline(
        pipeline_name="health_etl",
        destination="duckdb",
        dataset_name="raw",
        full_refresh=False,
    )

    # Get the last modified time from the destination
    last_modified = get_last_modified_time(pipeline, "integer_app_results")

    # Load data from PostgreSQL with incremental query
    load_info = pipeline.run(
        dlt.resource(
            f"postgresql://{POSTGRES_CONNECTION['user']}:{POSTGRES_CONNECTION['password']}@{POSTGRES_CONNECTION['host']}:{POSTGRES_CONNECTION['port']}/{POSTGRES_CONNECTION['database']}",
            query=f"""
                SELECT 	
                    iar.app_result_id,
                    ar.content_slug,
                    ar.created_time,
                    ar.modified_time,
                    iar.value
                FROM public.integer_app_results iar
                JOIN public.app_results ar ON ar.id = iar.app_result_id
                WHERE ar.polymorphic_type = 'IntegerAppResult'
                AND ar.modified_time > '{last_modified}'
            """,
        ),
        table_name="integer_app_results",
        write_disposition="merge",
    )

    return MaterializeResult(
        metadata={
            "rows_loaded": load_info.load_packages[0].row_counts.get(
                "integer_app_results", 0
            ),
            "last_modified": last_modified.isoformat(),
        }
    )
