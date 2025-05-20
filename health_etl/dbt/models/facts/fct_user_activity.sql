SELECT
    user_activity_key,
    app_result_id,
    content_slug,
    created_time,
    modified_time,
    VALUE
FROM
    {{ ref('stg_user_activity') }}
{% if is_incremental() %}
    WHERE modified_time > (SELECT MAX(modified_time) FROM {{ this }})
{% endif %}
