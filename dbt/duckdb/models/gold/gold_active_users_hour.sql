-- =============================================================================
-- Model       : gold_active_users_hour
-- Description : Distinct accounts that created a post, like, repost or follow per
--               UTC hour, per collection and overall ('all'). Exact count(DISTINCT),
--               part of the metric contract shared by both branches (D17).
--               Deletions and post edits do not make an account active.
-- Source      : silver_posts, silver_likes, silver_reposts, silver_follows
--               (hours touched since the last run, D19)
-- Output      : transform.gold.gold_active_users_hour, split by day(hour)
-- =============================================================================

{%- set silver_models = ['silver_posts', 'silver_likes', 'silver_reposts', 'silver_follows'] %}
{%- set last_read, current = gold_snapshot_range() %}
{%- set hours = touched_hours(silver_models, last_read, current) %}

WITH actions AS (
    SELECT 'app.bsky.feed.post' AS collection, event_time, did
    FROM {{ silver_for_hours('silver_posts', hours, current) }}
    WHERE operation = 'create'

    UNION ALL
    SELECT 'app.bsky.feed.like', event_time, did
    FROM {{ silver_for_hours('silver_likes', hours, current) }}

    UNION ALL
    SELECT 'app.bsky.feed.repost', event_time, did
    FROM {{ silver_for_hours('silver_reposts', hours, current) }}

    UNION ALL
    SELECT 'app.bsky.graph.follow', event_time, did
    FROM {{ silver_for_hours('silver_follows', hours, current) }}
),

per_hour AS (
    SELECT
        date_trunc('hour', event_time)          AS hour,
        coalesce(collection, 'all')             AS collection,
        count(DISTINCT did)                     AS active_accounts,
        {{ current }}::BIGINT                   AS silver_snapshot_id
    FROM actions
    GROUP BY GROUPING SETS ((date_trunc('hour', event_time), collection), (date_trunc('hour', event_time)))
)

SELECT * FROM per_hour
