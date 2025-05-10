import psycopg2
import random
from datetime import datetime, timedelta
import time
from faker import Faker

# Initialize Faker
fake = Faker()

# Database connection parameters
DB_PARAMS = {
    "dbname": "healthetl",
    "user": "healthetl_user",
    "password": "healthetl_password",
    "host": "localhost",
    "port": "5433",
}


def get_db_connection():
    return psycopg2.connect(**DB_PARAMS)


def insert_app_result(cursor, polymorphic_type, content_slug):
    """Insert a base app result and return its ID"""
    cursor.execute(
        """
        INSERT INTO app_results (polymorphic_type, content_slug)
        VALUES (%s, %s)
        RETURNING id
    """,
        (polymorphic_type, content_slug),
    )
    return cursor.fetchone()[0]


def generate_heart_rate_data(cursor):
    """Generate heart rate data (IntegerAppResult)"""
    app_result_id = insert_app_result(cursor, "IntegerAppResult", "heart_rate")
    heart_rate = random.randint(60, 100)  # Normal resting heart rate range
    cursor.execute(
        """
        INSERT INTO integer_app_results (app_result_id, value)
        VALUES (%s, %s)
    """,
        (app_result_id, heart_rate),
    )


def generate_steps_data(cursor):
    """Generate steps data (IntegerAppResult)"""
    app_result_id = insert_app_result(cursor, "IntegerAppResult", "steps")
    steps = random.randint(0, 100)  # Steps in the last minute
    cursor.execute(
        """
        INSERT INTO integer_app_results (app_result_id, value)
        VALUES (%s, %s)
    """,
        (app_result_id, steps),
    )


def generate_sleep_data(cursor):
    """Generate sleep data (RangeAppResult)"""
    app_result_id = insert_app_result(cursor, "RangeAppResult", "sleep_quality")
    from_value = random.randint(1, 5)  # Sleep quality scale 1-5
    to_value = from_value + random.randint(0, 2)  # Range of quality
    cursor.execute(
        """
        INSERT INTO range_app_results (app_result_id, from_value, to_value)
        VALUES (%s, %s, %s)
    """,
        (app_result_id, from_value, to_value),
    )


def generate_activity_data(cursor):
    """Generate activity timestamp (DateTimeAppResult)"""
    app_result_id = insert_app_result(cursor, "DateTimeAppResult", "activity_start")
    activity_time = datetime.now()
    cursor.execute(
        """
        INSERT INTO datetime_app_results (app_result_id, value)
        VALUES (%s, %s)
    """,
        (app_result_id, activity_time),
    )


def generate_stress_data(cursor):
    """Generate stress level data (IntegerAppResult)"""
    # First, get all existing stress records
    cursor.execute("""
        SELECT iar.id, ar.id 
        FROM integer_app_results iar
        JOIN app_results ar ON ar.id = iar.app_result_id
        WHERE ar.content_slug = 'stress_level'
    """)

    results = cursor.fetchall()

    if results and random.random() < 0.25:
        # Randomly select one record to update
        random_record = random.choice(results)
        stress_level = random.randint(1, 10)  # Stress level scale 1-10
        cursor.execute(
            """
            UPDATE integer_app_results 
            SET value = %s
            WHERE id = %s
        """,
            (stress_level, random_record[0]),
        )
        print(f"Random stress level record updated to {stress_level}")
    else:
        # Create new record if none exists or if we're not updating
        app_result_id = insert_app_result(cursor, "IntegerAppResult", "stress_level")
        stress_level = random.randint(1, 10)  # Stress level scale 1-10
        cursor.execute(
            """
            INSERT INTO integer_app_results (app_result_id, value)
            VALUES (%s, %s)
        """,
            (app_result_id, stress_level),
        )
        print("New stress level data inserted")


def main():
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        print("Starting wearable data simulation...")

        while True:
            # Generate one of each type of data
            generate_heart_rate_data(cursor)
            generate_steps_data(cursor)
            generate_sleep_data(cursor)
            generate_activity_data(cursor)
            generate_stress_data(cursor)  # This will only insert 25% of the time

            # Commit the transaction
            conn.commit()

            print(f"Data inserted at {datetime.now()}")

            # Wait for 1 minute before next insertion
            time.sleep(60)

    except KeyboardInterrupt:
        print("\nStopping data simulation...")
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        if "cursor" in locals():
            cursor.close()
        if "conn" in locals():
            conn.close()


if __name__ == "__main__":
    main()
