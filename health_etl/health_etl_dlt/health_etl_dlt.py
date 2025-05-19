import dlt
from datetime import datetime
import duckdb
import psycopg2
from psycopg2.extras import RealDictCursor


def get_duckdb_conn():
    return duckdb.connect("healthetl_pipeline.duckdb")


def get_pg_conn():
    return psycopg2.connect(
        host=dlt.secrets["postgres.app"]["host"],
        port=dlt.secrets["postgres.app"]["port"],
        database=dlt.secrets["postgres.app"]["database"],
        user=dlt.secrets["postgres.app"]["username"],
        password=dlt.secrets["postgres.app"]["password"],
    )


def get_last_modified_time(table_name):
    """Get the last modified time from the DuckDB destination"""

    try:
        duckdb_conn = get_duckdb_conn()
        result = duckdb_conn.execute(f"""
            SELECT MAX(modified_time) as last_modified
            FROM {table_name}
        """).fetchone()
        return result[0] if result and result[0] else datetime.min
    except Exception:
        return datetime.min


@dlt.resource(
    table_name="stg_integer_app_results",
    write_disposition="merge",
    primary_key="app_result_id",
)
def load_integer_app_results():
    print("Loading integer app results")

    pg_conn = get_pg_conn()

    last_modified_time = get_last_modified_time("stg_integer_app_results")

    with pg_conn.cursor(cursor_factory=RealDictCursor) as cursor:
        cursor.execute(
            """
            SELECT
                iar.app_result_id,
                ar.content_slug,
                ar.created_time,
                ar.modified_time,
                iar.value
            FROM public.integer_app_results iar
            JOIN public.app_results ar ON ar.id = iar.app_result_id
            WHERE ar.polymorphic_type = 'IntegerAppResult'
            AND ar.modified_time > %s
            """,
            (last_modified_time,),
        )

        results = cursor.fetchall()

    data = [dict(row) for row in results]

    yield data


@dlt.resource(
    table_name="stg_date_time_app_results",
    write_disposition="merge",
    primary_key="app_result_id",
)
def load_date_time_app_results():
    print("Loading date time app results")

    last_modified_time = get_last_modified_time("stg_date_time_app_results")

    pg_conn = get_pg_conn()

    with pg_conn.cursor(cursor_factory=RealDictCursor) as cursor:
        cursor.execute(
            """
            SELECT
                dar.app_result_id,
                ar.content_slug,
                ar.created_time,
                ar.modified_time,
                dar.value
            FROM public.datetime_app_results dar
            JOIN public.app_results ar ON ar.id = dar.app_result_id
            WHERE ar.polymorphic_type = 'DateTimeAppResult'
            AND ar.modified_time > %s
            """,
            (last_modified_time,),
        )

        results = cursor.fetchall()

    data = [dict(row) for row in results]

    yield data


@dlt.resource(
    table_name="stg_range_app_results",
    write_disposition="merge",
    primary_key="app_result_id",
)
def load_range_app_results():
    print("Loading range app results")

    pg_conn = get_pg_conn()

    last_modified_time = get_last_modified_time("stg_range_app_results")

    with pg_conn.cursor(cursor_factory=RealDictCursor) as cursor:
        cursor.execute(
            """
            SELECT
                rar.app_result_id,
                ar.content_slug,
                ar.created_time,
                ar.modified_time,
                rar.from_value,
                rar.to_value
            FROM public.range_app_results rar
            JOIN public.app_results ar ON ar.id = rar.app_result_id
            WHERE ar.polymorphic_type = 'RangeAppResult'
            AND ar.modified_time > %s
            """,
            (last_modified_time,),
        )

        results = cursor.fetchall()

    data = [dict(row) for row in results]

    yield data
