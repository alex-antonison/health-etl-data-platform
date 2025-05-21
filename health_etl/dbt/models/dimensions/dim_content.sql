WITH content_slugs AS (
    SELECT
        DISTINCT content_slug
    FROM
        {{ ref('stg_user_activity') }}
)
SELECT
    content_slug,
    CASE
        content_slug
        WHEN 'activity_start' THEN 'Activity Start'
        WHEN 'stress_level' THEN 'Stress Level'
        WHEN 'sleep_quality' THEN 'Sleep Quality'
        WHEN 'steps' THEN 'Steps'
        WHEN 'heart_rate' THEN 'Heart Rate'
        ELSE 'Unknown'
    END AS content_name,
    CASE
        content_slug
        WHEN 'activity_start' THEN 'When user starts activity'
        WHEN 'stress_level' THEN 'User''s reported stress'
        WHEN 'sleep_quality' THEN 'User''s sleep quality score'
        WHEN 'steps' THEN 'Number of steps taken'
        WHEN 'heart_rate' THEN 'User''s heart rate'
        ELSE 'Unknown content type'
    END AS description
FROM
    content_slugs
