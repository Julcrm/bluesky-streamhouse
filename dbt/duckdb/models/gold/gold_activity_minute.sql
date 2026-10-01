{{ config(
    unique_key='minute',
    partitioned_by=['year(minute)', 'month(minute)', 'day(minute)']
) }}

-- =============================================================================
-- Model       : gold_activity_minute
-- Description : Events per UTC minute and collection: creations and deletions.
--               Gives posts per minute and the like / repost / follow volumes.
--               Post edits (operation = update) are not counted.
-- Source      : silver_posts, silver_likes, silver_reposts, silver_follows,
--               silver_deletes (hours touched since the last run, D19)
-- Output      : transform.gold.gold_activity_minute, split by day(minute)
-- =============================================================================

{%- set silver_models = ['silver_posts', 'silver_likes', 'silver_reposts', 'silver_follows', 'silver_deletes'] %}
{%- set last_read, read_up_to = gold_snapshot_range(silver_models) %}
{%- set ranges = touched_ranges(silver_models, last_read, read_up_to) %}

WITH events AS (
    SELECT 'app.bsky.feed.post' AS collection, event_time, 'create' AS operation
    FROM {{ silver_for_hours('silver_posts', ranges, read_up_to) }}
    WHERE operation = 'create'

    UNION ALL
    SELECT 'app.bsky.feed.like', event_time, 'create'
    FROM {{ silver_for_hours('silver_likes', ranges, read_up_to) }}

    UNION ALL
    SELECT 'app.bsky.feed.repost', event_time, 'create'
    FROM {{ silver_for_hours('silver_reposts', ranges, read_up_to) }}

    UNION ALL
    SELECT 'app.bsky.graph.follow', event_time, 'create'
    FROM {{ silver_for_hours('silver_follows', ranges, read_up_to) }}

    UNION ALL
    SELECT collection, event_time, 'delete'
    FROM {{ silver_for_hours('silver_deletes', ranges, read_up_to) }}
),

per_minute AS (
    SELECT
        date_trunc('minute', event_time)                AS minute,
        collection,
        count(*) FILTER (WHERE operation = 'create')    AS creates,
        count(*) FILTER (WHERE operation = 'delete')    AS deletes,
        {{ read_up_to }}::BIGINT                        AS silver_snapshot_id
    FROM events
    GROUP BY ALL
)

SELECT * FROM per_minute
