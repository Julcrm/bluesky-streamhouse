-- Assert that the incremental Gold hours equal a full recomputation from Silver, read at
-- the snapshot Gold last read Silver at: no hour missed or stale after delete+insert (D19).
-- Checked on the last hours only (var gold_check_window): active accounts need a full
-- count(DISTINCT) per hour, too costly on a whole day every 15 min

{%- set read_at = 0 %}
{%- if execute %}
    {%- set read_at = run_query(
        "SELECT coalesce(min(s), 0) FROM ("
        ~ "SELECT max(silver_snapshot_id) AS s FROM " ~ ref('gold_activity_minute')
        ~ " UNION ALL SELECT max(silver_snapshot_id) FROM " ~ ref('gold_engagement_hour')
        ~ " UNION ALL SELECT max(silver_snapshot_id) FROM " ~ ref('gold_active_users_hour') ~ ")"
    ).columns[0].values()[0] %}
{%- endif %}

WITH window_start AS (
    SELECT date_trunc('hour', now() - {{ var('gold_check_window') }}) AS hour
),

silver_likes AS (
    SELECT date_trunc('hour', event_time) AS hour, count(*) AS likes, count(DISTINCT did) AS likers
    FROM {{ ref('silver_likes') }} AT (VERSION => {{ read_at }})
    WHERE event_time >= (SELECT hour FROM window_start)
    GROUP BY 1
),

silver_posts AS (
    SELECT date_trunc('hour', event_time) AS hour, count(*) AS posts
    FROM {{ ref('silver_posts') }} AT (VERSION => {{ read_at }})
    WHERE event_time >= (SELECT hour FROM window_start) AND operation = 'create'
    GROUP BY 1
),

gold_activity AS (
    SELECT
        date_trunc('hour', minute)                                                  AS hour,
        sum(creates) FILTER (WHERE collection = 'app.bsky.feed.like')               AS likes,
        sum(creates) FILTER (WHERE collection = 'app.bsky.feed.post')               AS posts
    FROM {{ ref('gold_activity_minute') }}
    WHERE minute >= (SELECT hour FROM window_start)
    GROUP BY 1
),

compared AS (
    SELECT
        coalesce(s.hour, a.hour)    AS hour,
        s.likes                     AS silver_likes,
        a.likes                     AS activity_likes,
        e.likes                     AS engagement_likes,
        p.posts                     AS silver_posts,
        a.posts                     AS activity_posts,
        s.likers                    AS silver_likers,
        u.active_accounts           AS gold_likers
    FROM silver_likes AS s
    FULL OUTER JOIN gold_activity AS a USING (hour)
    LEFT JOIN silver_posts AS p ON p.hour = coalesce(s.hour, a.hour)
    LEFT JOIN {{ ref('gold_engagement_hour') }} AS e ON e.hour = coalesce(s.hour, a.hour)
    LEFT JOIN {{ ref('gold_active_users_hour') }} AS u
        ON u.hour = coalesce(s.hour, a.hour) AND u.collection = 'app.bsky.feed.like'
)

SELECT *
FROM compared
WHERE silver_likes IS DISTINCT FROM activity_likes
   OR silver_likes IS DISTINCT FROM engagement_likes
   OR silver_posts IS DISTINCT FROM activity_posts
   OR silver_likers IS DISTINCT FROM gold_likers
