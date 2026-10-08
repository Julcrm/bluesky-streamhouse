{{ config(partition_by=['hours(hour)']) }}

-- =============================================================================
-- Model       : gold_active_users_hour
-- Description : Distinct accounts that created a post, like, repost or follow per
--               UTC hour, per collection and overall ('all'). Exact count(DISTINCT),
--               part of the metric contract shared by both branches (D17).
--               Deletions and post edits do not make an account active.
-- Source      : silver_posts, silver_likes, silver_reposts, silver_follows
--               (hours touched since the last run, D19)
-- Output      : lakekeeper.gold.gold_active_users_hour, partitioned by hours(hour)
-- =============================================================================

{%- set silver_models = ['silver_posts', 'silver_likes', 'silver_reposts', 'silver_follows'] %}
{%- set hours, positions = gold_hours(silver_models) %}

WITH actions AS (
    SELECT 'app.bsky.feed.post' AS collection, event_time, did
    FROM {{ silver_for_hours('silver_posts', hours, positions) }}
    WHERE operation = 'create'

    UNION ALL
    SELECT 'app.bsky.feed.like', event_time, did
    FROM {{ silver_for_hours('silver_likes', hours, positions) }}

    UNION ALL
    SELECT 'app.bsky.feed.repost', event_time, did
    FROM {{ silver_for_hours('silver_reposts', hours, positions) }}

    UNION ALL
    SELECT 'app.bsky.graph.follow', event_time, did
    FROM {{ silver_for_hours('silver_follows', hours, positions) }}
),

per_hour AS (
    SELECT
        date_trunc('HOUR', event_time)                      AS hour,
        coalesce(collection, 'all')                         AS collection,
        count(DISTINCT did)                                 AS active_accounts,
        {{ gold_snapshot_column(silver_models, positions) }} AS silver_snapshot_id
    FROM actions
    GROUP BY GROUPING SETS (
        (date_trunc('HOUR', event_time), collection),
        (date_trunc('HOUR', event_time))
    )
)

SELECT * FROM per_hour
