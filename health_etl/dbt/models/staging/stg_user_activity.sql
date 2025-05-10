WITH

date_time_app_data AS (
    SELECT
        app_result_id,
        content_slug,
        created_time,
        modified_time,
        cast(value as varchar) as value
    FROM {{ source('app_results', 'datetime_app_results') }}
),

integer_app_data AS (
    SELECT
        app_result_id,
        content_slug,
        created_time,
        modified_time,
        cast(value as varchar) as value
    FROM {{ source('app_results', 'integer_app_results') }}
),

range_app_results AS (
    SELECT
        app_result_id,
        content_slug,
        created_time,
        modified_time,
        cast(from_value AS varchar) || ' - ' || cast(to_value AS varchar) AS value
    FROM {{ source('app_results', 'range_app_results') }}
)

SELECT
    *
FROM date_time_app_data
UNION ALL
SELECT
    *
FROM integer_app_data
UNION ALL
SELECT
    *
FROM range_app_results