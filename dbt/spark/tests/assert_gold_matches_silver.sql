-- =============================================================================
-- Assert that recent Gold hours match a recomputation from Silver (D25): likes and
-- posts per hour (gold_activity_minute), likes per hour (gold_engagement_hour) and
-- distinct likers per hour (gold_active_users_hour), over var('gold_check_window').
-- Same test as dbt/duckdb. Silver is read as of the snapshots each Gold model last took
-- into account (meta.gold_progress); hours still pending for that model are skipped,
-- since their Gold rows are known to be behind (gold_incremental.sql).
-- =============================================================================

{{ config(meta={'dagster': {'ref': {'name': 'gold_activity_minute'}}}) }}

{%- set gold_models = ['gold_activity_minute', 'gold_engagement_hour', 'gold_active_users_hour'] %}
{%- set read_at = {} %}
{%- for m in gold_models %}
    {%- set positions = {} %}
    {%- if execute %}
        {%- for r in run_query(
            "SELECT silver_model, silver_snapshot_id FROM (SELECT silver_model, silver_snapshot_id,"
            ~ " row_number() OVER (PARTITION BY silver_model ORDER BY recorded_at DESC) AS rn"
            ~ " FROM " ~ gold_progress_table() ~ " WHERE model = '" ~ m ~ "' AND done) WHERE rn = 1"
        ).rows %}
            {%- do positions.update({r[0]: r[1]}) %}
        {%- endfor %}
    {%- endif %}
    {%- do read_at.update({m: positions}) %}
{%- endfor %}

{#- Silver relation as of each Gold model's positions, and its pending-hours filter -#}
{%- set silver_at = {} %}
{%- for m, positions in read_at.items() %}
    {%- for silver_model in ['silver_likes', 'silver_posts'] %}
        {%- set snapshot = positions.get(silver_model) %}
        {%- do silver_at.update({m ~ ':' ~ silver_model: (
            ref(silver_model) ~ ' VERSION AS OF ' ~ snapshot if snapshot is not none
            else '(SELECT * FROM ' ~ ref(silver_model) ~ ' WHERE false)'
        )}) %}
    {%- endfor %}
{%- endfor %}
{%- set pending = "hour NOT IN (SELECT hour FROM " ~ gold_pending_table() ~ " WHERE model = '" %}

WITH window_start AS (
    SELECT date_trunc('HOUR', now() - {{ var('gold_check_window') }}) AS hour
),

activity AS (
    SELECT
        date_trunc('HOUR', minute)                                                  AS hour,
        sum(creates) FILTER (WHERE collection = 'app.bsky.feed.like')               AS likes,
        sum(creates) FILTER (WHERE collection = 'app.bsky.feed.post')               AS posts
    FROM {{ ref('gold_activity_minute') }}
    WHERE minute >= (SELECT hour FROM window_start)
    GROUP BY 1
),

activity_silver AS (
    SELECT hour, sum(likes) AS likes, sum(posts) AS posts
    FROM (
        SELECT date_trunc('HOUR', event_time) AS hour, count(*) AS likes, 0 AS posts
        FROM {{ silver_at['gold_activity_minute:silver_likes'] }}
        WHERE event_time >= (SELECT hour FROM window_start)
        GROUP BY 1
        UNION ALL
        SELECT date_trunc('HOUR', event_time), 0, count(*)
        FROM {{ silver_at['gold_activity_minute:silver_posts'] }}
        WHERE event_time >= (SELECT hour FROM window_start) AND operation = 'create'
        GROUP BY 1
    )
    GROUP BY 1
),

engagement_silver AS (
    SELECT date_trunc('HOUR', event_time) AS hour, count(*) AS likes
    FROM {{ silver_at['gold_engagement_hour:silver_likes'] }}
    WHERE event_time >= (SELECT hour FROM window_start)
    GROUP BY 1
),

likers_silver AS (
    SELECT date_trunc('HOUR', event_time) AS hour, count(DISTINCT did) AS likers
    FROM {{ silver_at['gold_active_users_hour:silver_likes'] }}
    WHERE event_time >= (SELECT hour FROM window_start)
    GROUP BY 1
)

SELECT * FROM (
    SELECT 'gold_activity_minute' AS model, coalesce(s.hour, g.hour) AS hour,
           s.likes AS silver_value, g.likes AS gold_value
    FROM activity_silver AS s
    FULL OUTER JOIN activity AS g ON s.hour = g.hour
    WHERE coalesce(s.likes, 0) != coalesce(g.likes, 0) OR coalesce(s.posts, 0) != coalesce(g.posts, 0)
) WHERE {{ pending }}gold_activity_minute')

UNION ALL

SELECT * FROM (
    SELECT 'gold_engagement_hour' AS model, coalesce(s.hour, g.hour) AS hour,
           s.likes AS silver_value, g.likes AS gold_value
    FROM engagement_silver AS s
    FULL OUTER JOIN (
        SELECT hour, likes FROM {{ ref('gold_engagement_hour') }}
        WHERE hour >= (SELECT hour FROM window_start)
    ) AS g ON s.hour = g.hour
    WHERE coalesce(s.likes, 0) != coalesce(g.likes, 0)
) WHERE {{ pending }}gold_engagement_hour')

UNION ALL

SELECT * FROM (
    SELECT 'gold_active_users_hour' AS model, coalesce(s.hour, g.hour) AS hour,
           s.likers AS silver_value, g.active_accounts AS gold_value
    FROM likers_silver AS s
    FULL OUTER JOIN (
        SELECT hour, active_accounts FROM {{ ref('gold_active_users_hour') }}
        WHERE hour >= (SELECT hour FROM window_start) AND collection = 'app.bsky.feed.like'
    ) AS g ON s.hour = g.hour
    WHERE coalesce(s.likers, 0) != coalesce(g.active_accounts, 0)
) WHERE {{ pending }}gold_active_users_hour')
