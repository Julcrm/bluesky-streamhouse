{{ config(partition_by=['hours(minute)']) }}

-- =============================================================================
-- Model       : gold_activity_minute
-- Description : Events per UTC minute and collection: creations and deletions.
--               Gives posts per minute and the like / repost / follow volumes.
--               Post edits (operation = update) are not counted.
-- Source      : silver_posts, silver_likes, silver_reposts, silver_follows,
--               silver_deletes (hours touched since the last run, D19)
-- Output      : lakekeeper.gold.gold_activity_minute, partitioned by hours(minute)
-- =============================================================================

{%- set silver_models = ['silver_posts', 'silver_likes', 'silver_reposts', 'silver_follows', 'silver_deletes'] %}
{%- set hours, positions = gold_hours(silver_models) %}

WITH events AS (
    SELECT 'app.bsky.feed.post' AS collection, event_time, 'create' AS operation
    FROM {{ silver_for_hours('silver_posts', hours, positions) }}
    WHERE operation = 'create'

    UNION ALL
    SELECT 'app.bsky.feed.like', event_time, 'create'
    FROM {{ silver_for_hours('silver_likes', hours, positions) }}

    UNION ALL
    SELECT 'app.bsky.feed.repost', event_time, 'create'
    FROM {{ silver_for_hours('silver_reposts', hours, positions) }}

    UNION ALL
    SELECT 'app.bsky.graph.follow', event_time, 'create'
    FROM {{ silver_for_hours('silver_follows', hours, positions) }}

    UNION ALL
    SELECT collection, event_time, 'delete'
    FROM {{ silver_for_hours('silver_deletes', hours, positions) }}
),

per_minute AS (
    SELECT
        date_trunc('MINUTE', event_time)                    AS minute,
        collection,
        count(*) FILTER (WHERE operation = 'create')        AS creates,
        count(*) FILTER (WHERE operation = 'delete')        AS deletes,
        {{ gold_snapshot_column(silver_models, positions) }} AS silver_snapshot_id
    FROM events
    GROUP BY date_trunc('MINUTE', event_time), collection
)

SELECT * FROM per_minute
