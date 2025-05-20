WITH date_time_app_data AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['app_result_id', 'content_slug']) }} AS user_activity_key,
        app_result_id,
        content_slug,
        created_time,
        modified_time,
        'date_time' AS activity_type,
        CAST(
            VALUE AS VARCHAR
        ) AS VALUE
    FROM
        {{ source(
            'app_results',
            'stg_date_time_app_results'
        ) }}

{% if is_incremental() %}
WHERE
    modified_time > (
        SELECT
            MAX(created_time)
        FROM
            {{ this }}
        WHERE
            activity_type = 'date_time'
    )
{% endif %}
),
integer_app_data AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['app_result_id', 'content_slug']) }} AS user_activity_key,
        app_result_id,
        content_slug,
        created_time,
        modified_time,
        'integer' AS activity_type,
        CAST(
            VALUE AS VARCHAR
        ) AS VALUE
    FROM
        {{ source(
            'app_results',
            'stg_integer_app_results'
        ) }}

{% if is_incremental() %}
WHERE
    modified_time > (
        SELECT
            MAX(created_time)
        FROM
            {{ this }}
        WHERE
            activity_type = 'integer'
    )
{% endif %}
),
range_app_results AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['app_result_id', 'content_slug']) }} AS user_activity_key,
        app_result_id,
        content_slug,
        created_time,
        modified_time,
        'range' AS activity_type,
        CAST(
            from_value AS VARCHAR
        ) || ' - ' || CAST(
            to_value AS VARCHAR
        ) AS VALUE
    FROM
        {{ source(
            'app_results',
            'stg_range_app_results'
        ) }}

{% if is_incremental() %}
WHERE
    modified_time > (
        SELECT
            MAX(created_time)
        FROM
            {{ this }}
        WHERE
            activity_type = 'range'
    )
{% endif %}
)
SELECT
    user_activity_key,
    app_result_id,
    content_slug,
    created_time,
    modified_time,
    activity_type,
    VALUE
FROM
    date_time_app_data
UNION ALL
SELECT
    user_activity_key,
    app_result_id,
    content_slug,
    created_time,
    modified_time,
    activity_type,
    VALUE
FROM
    integer_app_data
UNION ALL
SELECT
    user_activity_key,
    app_result_id,
    content_slug,
    created_time,
    modified_time,
    activity_type,
    VALUE
FROM
    range_app_results
