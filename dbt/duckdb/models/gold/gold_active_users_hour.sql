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
{%- set last_read, read_up_to = gold_snapshot_range(silver_models) %}
{%- set ranges = touched_ranges(silver_models, last_read, read_up_to) %}

WITH actions AS (
    SELECT 'app.bsky.feed.post' AS collection, event_time, did
    FROM {{ silver_for_hours('silver_posts', ranges, read_up_to) }}
    WHERE operation = 'create'

    UNION ALL
    SELECT 'app.bsky.feed.like', event_time, did
    FROM {{ silver_for_hours('silver_likes', ranges, read_up_to) }}

    UNION ALL
    SELECT 'app.bsky.feed.repost', event_time, did
    FROM {{ silver_for_hours('silver_reposts', ranges, read_up_to) }}

    UNION ALL
    SELECT 'app.bsky.graph.follow', event_time, did
    FROM {{ silver_for_hours('silver_follows', ranges, read_up_to) }}
),

per_hour AS (
    SELECT
        date_trunc('hour', event_time)          AS hour,
        coalesce(collection, 'all')             AS collection,
        count(DISTINCT did)                     AS active_accounts,
        {{ read_up_to }}::BIGINT                AS silver_snapshot_id
    FROM actions
    GROUP BY GROUPING SETS ((date_trunc('hour', event_time), collection), (date_trunc('hour', event_time)))
)

SELECT * FROM per_hour
