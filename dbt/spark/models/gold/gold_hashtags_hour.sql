{{ config(partition_by=['hours(hour)']) }}

-- =============================================================================
-- Model       : gold_hashtags_hour
-- Description : Top 50 hashtags per UTC hour, by number of new posts using them.
--               Ranking lives here, not in the API (business logic in Gold only).
--               Ties are broken alphabetically so the rank is deterministic.
-- Source      : silver_posts (hours touched since the last run, D19)
-- Output      : lakekeeper.gold.gold_hashtags_hour, partitioned by hours(hour)
-- =============================================================================

{%- set silver_models = ['silver_posts'] %}
{%- set hours, positions = gold_hours(silver_models) %}

WITH post_tags AS (
    SELECT
        date_trunc('HOUR', event_time)  AS hour,
        hashtag
    FROM {{ silver_for_hours('silver_posts', hours, positions) }}
    LATERAL VIEW explode(hashtags) tags AS hashtag
    WHERE operation = 'create'
      AND size(hashtags) > 0
),

per_hour AS (
    SELECT hour, hashtag, count(*) AS posts
    FROM post_tags
    GROUP BY hour, hashtag
),

ranked AS (
    SELECT
        hour,
        hashtag,
        posts,
        row_number() OVER (PARTITION BY hour ORDER BY posts DESC, hashtag) AS rank,
        {{ gold_snapshot_column(silver_models, positions) }}               AS silver_snapshot_id
    FROM per_hour
)

-- Spark has no QUALIFY
SELECT * FROM ranked WHERE rank <= {{ var('gold_top_hashtags') }}
