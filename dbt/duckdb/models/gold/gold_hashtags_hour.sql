-- =============================================================================
-- Model       : gold_hashtags_hour
-- Description : Top 50 hashtags per UTC hour, by number of new posts using them.
--               Ranking lives here, not in the API (business logic in Gold only).
--               Ties are broken alphabetically so the rank is deterministic.
-- Source      : silver_posts (hours touched since the last run, D19)
-- Output      : transform.gold.gold_hashtags_hour, split by day(hour)
-- =============================================================================

{%- set silver_models = ['silver_posts'] %}
{%- set last_read, read_up_to = gold_snapshot_range(silver_models) %}
{%- set ranges = touched_ranges(silver_models, last_read, read_up_to) %}

WITH post_tags AS (
    SELECT
        date_trunc('hour', event_time)  AS hour,
        unnest(hashtags)                AS hashtag
    FROM {{ silver_for_hours('silver_posts', ranges, read_up_to) }}
    WHERE operation = 'create'
      AND len(hashtags) > 0
),

per_hour AS (
    SELECT hour, hashtag, count(*) AS posts
    FROM post_tags
    GROUP BY ALL
),

ranked AS (
    SELECT
        hour,
        hashtag,
        posts,
        row_number() OVER (PARTITION BY hour ORDER BY posts DESC, hashtag) AS rank,
        {{ read_up_to }}::BIGINT                                            AS silver_snapshot_id
    FROM per_hour
    QUALIFY rank <= {{ var('gold_top_hashtags') }}
)

SELECT * FROM ranked
