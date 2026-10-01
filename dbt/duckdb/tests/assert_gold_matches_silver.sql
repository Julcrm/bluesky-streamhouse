-- Assert that the incremental Gold hours equal a full recomputation from Silver: no hour
-- missed or stale after delete+insert (D19). Each Gold model is compared with Silver read
-- at its own position (max silver_snapshot_id): models catch up at their own pace under
-- var('gold_max_hours_per_run'), and at that position every hour they hold is final.
-- Hours with no row on one side count as 0 (an engagement hour may hold only deletes).
-- Checked on the last hours only (var gold_check_window): active accounts need a full
-- count(DISTINCT) per hour, too costly on a whole day every 15 min

{%- set gold_models = ['gold_activity_minute', 'gold_engagement_hour', 'gold_active_users_hour'] %}
{%- set read_at = {} %}
{%- for m in gold_models %}
    {%- if execute %}
        {%- do read_at.update({m: run_query(
            "SELECT coalesce(max(silver_snapshot_id), 0) FROM " ~ ref(m)
        ).columns[0].values()[0]}) %}
    {%- else %}
        {%- do read_at.update({m: 0}) %}
    {%- endif %}
{%- endfor %}

WITH window_start AS (
    SELECT date_trunc('hour', now() - {{ var('gold_check_window') }}) AS hour
),

activity AS (
    SELECT
        date_trunc('hour', minute)                                                  AS hour,
        sum(creates) FILTER (WHERE collection = 'app.bsky.feed.like')               AS likes,
        sum(creates) FILTER (WHERE collection = 'app.bsky.feed.post')               AS posts
    FROM {{ ref('gold_activity_minute') }}
    WHERE minute >= (SELECT hour FROM window_start)
    GROUP BY 1
),

activity_silver AS (
    SELECT hour, sum(likes) AS likes, sum(posts) AS posts
    FROM (
        SELECT date_trunc('hour', event_time) AS hour, count(*) AS likes, 0 AS posts
        FROM {{ ref('silver_likes') }} AT (VERSION => {{ read_at['gold_activity_minute'] }})
        WHERE event_time >= (SELECT hour FROM window_start)
        GROUP BY 1
        UNION ALL
        SELECT date_trunc('hour', event_time), 0, count(*)
        FROM {{ ref('silver_posts') }} AT (VERSION => {{ read_at['gold_activity_minute'] }})
        WHERE event_time >= (SELECT hour FROM window_start) AND operation = 'create'
        GROUP BY 1
    )
    GROUP BY 1
),

engagement_silver AS (
    SELECT date_trunc('hour', event_time) AS hour, count(*) AS likes
    FROM {{ ref('silver_likes') }} AT (VERSION => {{ read_at['gold_engagement_hour'] }})
    WHERE event_time >= (SELECT hour FROM window_start)
    GROUP BY 1
),

likers_silver AS (
    SELECT date_trunc('hour', event_time) AS hour, count(DISTINCT did) AS likers
    FROM {{ ref('silver_likes') }} AT (VERSION => {{ read_at['gold_active_users_hour'] }})
    WHERE event_time >= (SELECT hour FROM window_start)
    GROUP BY 1
)

SELECT 'gold_activity_minute' AS model, coalesce(s.hour, g.hour) AS hour,
       s.likes AS silver_value, g.likes AS gold_value
FROM activity_silver AS s
FULL OUTER JOIN activity AS g USING (hour)
WHERE coalesce(s.likes, 0) != coalesce(g.likes, 0) OR coalesce(s.posts, 0) != coalesce(g.posts, 0)

UNION ALL

SELECT 'gold_engagement_hour', coalesce(s.hour, g.hour), s.likes, g.likes
FROM engagement_silver AS s
FULL OUTER JOIN (
    SELECT hour, likes FROM {{ ref('gold_engagement_hour') }}
    WHERE hour >= (SELECT hour FROM window_start)
) AS g USING (hour)
WHERE coalesce(s.likes, 0) != coalesce(g.likes, 0)

UNION ALL

SELECT 'gold_active_users_hour', coalesce(s.hour, g.hour), s.likers, g.active_accounts
FROM likers_silver AS s
FULL OUTER JOIN (
    SELECT hour, active_accounts FROM {{ ref('gold_active_users_hour') }}
    WHERE hour >= (SELECT hour FROM window_start) AND collection = 'app.bsky.feed.like'
) AS g USING (hour)
WHERE coalesce(s.likers, 0) != coalesce(g.active_accounts, 0)
