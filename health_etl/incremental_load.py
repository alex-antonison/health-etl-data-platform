import os
from datetime import datetime
import duckdb
import psycopg2
from psycopg2.extras import RealDictCursor

# Database connection parameters
POSTGRES_CONNECTION = {
    "host": "localhost",
    "port": 5433,
    "database": "healthetl",
    "user": "healthetl_user",
    "password": "healthetl_password",
}


def get_last_modified_time(duckdb_conn, table_name):
    """Get the last modified time from the DuckDB destination"""
    try:
        result = duckdb_conn.execute(f"""
            SELECT MAX(modified_time) as last_modified
            FROM {table_name}
        """).fetchone()
        return result[0] if result and result[0] else datetime.min
    except Exception:
        return datetime.min


def load_integer_app_results(duckdb_conn, pg_conn):
    """Load incremental integer app results from PostgreSQL to DuckDB"""
    # Get the last modified time from DuckDB
    last_modified = get_last_modified_time(duckdb_conn, "integer_app_results")

    try:
        # Create a cursor that returns results as dictionaries
        with pg_conn.cursor(cursor_factory=RealDictCursor) as cursor:
            # Execute the query with the last modified time filter
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
                (last_modified,),
            )

            # Fetch all results
            results = cursor.fetchall()

            if results:
                # Convert results to a list of dictionaries
                data = [dict(row) for row in results]

                # Insert data into DuckDB
                duckdb_conn.execute("""
                    CREATE TABLE IF NOT EXISTS integer_app_results (
                        app_result_id INTEGER,
                        content_slug TEXT,
                        created_time TIMESTAMP,
                        modified_time TIMESTAMP,
                        value INTEGER
                    )
                """)

                # Insert the new data using DuckDB's parameter style
                for row in data:
                    duckdb_conn.execute(
                        """
                        INSERT INTO integer_app_results 
                        (app_result_id, content_slug, created_time, modified_time, value)
                        VALUES (?, ?, ?, ?, ?)
                    """,
                        (
                            row["app_result_id"],
                            row["content_slug"],
                            row["created_time"],
                            row["modified_time"],
                            row["value"],
                        ),
                    )

                print(f"Loaded {len(data)} new integer app results")
            else:
                print("No new integer app results to load")

    except Exception as e:
        print(f"Error loading integer app results: {e}")


def load_datetime_app_results(duckdb_conn, pg_conn):
    """Load incremental datetime app results from PostgreSQL to DuckDB"""
    # Get the last modified time from DuckDB
    last_modified = get_last_modified_time(duckdb_conn, "datetime_app_results")

    try:
        # Create a cursor that returns results as dictionaries
        with pg_conn.cursor(cursor_factory=RealDictCursor) as cursor:
            # Execute the query with the last modified time filter
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
                (last_modified,),
            )

            # Fetch all results
            results = cursor.fetchall()

            if results:
                # Convert results to a list of dictionaries
                data = [dict(row) for row in results]

                # Insert data into DuckDB
                duckdb_conn.execute("""
                    CREATE TABLE IF NOT EXISTS datetime_app_results (
                        app_result_id INTEGER,
                        content_slug TEXT,
                        created_time TIMESTAMP,
                        modified_time TIMESTAMP,
                        value TIMESTAMP
                    )
                """)

                # Insert the new data using DuckDB's parameter style
                for row in data:
                    duckdb_conn.execute(
                        """
                        INSERT INTO datetime_app_results 
                        (app_result_id, content_slug, created_time, modified_time, value)
                        VALUES (?, ?, ?, ?, ?)
                    """,
                        (
                            row["app_result_id"],
                            row["content_slug"],
                            row["created_time"],
                            row["modified_time"],
                            row["value"],
                        ),
                    )

                print(f"Loaded {len(data)} new datetime app results")
            else:
                print("No new datetime app results to load")

    except Exception as e:
        print(f"Error loading datetime app results: {e}")


def load_range_app_results(duckdb_conn, pg_conn):
    """Load incremental range app results from PostgreSQL to DuckDB"""
    # Get the last modified time from DuckDB
    last_modified = get_last_modified_time(duckdb_conn, "range_app_results")

    try:
        # Create a cursor that returns results as dictionaries
        with pg_conn.cursor(cursor_factory=RealDictCursor) as cursor:
            # Execute the query with the last modified time filter
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
                (last_modified,),
            )

            # Fetch all results
            results = cursor.fetchall()

            if results:
                # Convert results to a list of dictionaries
                data = [dict(row) for row in results]

                # Insert data into DuckDB
                duckdb_conn.execute("""
                    CREATE TABLE IF NOT EXISTS range_app_results (
                        app_result_id INTEGER,
                        content_slug TEXT,
                        created_time TIMESTAMP,
                        modified_time TIMESTAMP,
                        from_value INTEGER,
                        to_value INTEGER
                    )
                """)

                # Insert the new data using DuckDB's parameter style
                for row in data:
                    duckdb_conn.execute(
                        """
                        INSERT INTO range_app_results 
                        (app_result_id, content_slug, created_time, modified_time, from_value, to_value)
                        VALUES (?, ?, ?, ?, ?, ?)
                    """,
                        (
                            row["app_result_id"],
                            row["content_slug"],
                            row["created_time"],
                            row["modified_time"],
                            row["from_value"],
                            row["to_value"],
                        ),
                    )

                print(f"Loaded {len(data)} new range app results")
            else:
                print("No new range app results to load")

    except Exception as e:
        print(f"Error loading range app results: {e}")


def load_incremental_data():
    """Load incremental data from PostgreSQL to DuckDB"""
    # Connect to DuckDB with the full path
    duckdb_conn = duckdb.connect("duckdb-database/health_etl.duckdb")

    # Connect to PostgreSQL
    pg_conn = psycopg2.connect(
        host=POSTGRES_CONNECTION["host"],
        port=POSTGRES_CONNECTION["port"],
        database=POSTGRES_CONNECTION["database"],
        user=POSTGRES_CONNECTION["user"],
        password=POSTGRES_CONNECTION["password"],
    )

    try:
        # Load all types of app results
        load_integer_app_results(duckdb_conn, pg_conn)
        load_datetime_app_results(duckdb_conn, pg_conn)
        load_range_app_results(duckdb_conn, pg_conn)

    finally:
        # Close connections
        pg_conn.close()
        duckdb_conn.close()


if __name__ == "__main__":
    load_incremental_data()
