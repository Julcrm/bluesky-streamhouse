-- =============================================================================
-- Model       : gold_langs_hour
-- Description : New posts per UTC hour and language. A post declaring several
--               languages counts once per language; regional variants are folded
--               into the primary subtag (en-US -> en); no language -> 'und'.
-- Source      : silver_posts (hours touched since the last run, D19)
-- Output      : transform.gold.gold_langs_hour, split by day(hour)
-- =============================================================================

{%- set last_read, current = gold_snapshot_range() %}
{%- set hours = touched_hours(['silver_posts'], last_read, current) %}

WITH posts AS (
    SELECT event_time, seq, langs
    FROM {{ silver_for_hours('silver_posts', hours, current) }}
    WHERE operation = 'create'
),

post_langs AS (
    SELECT DISTINCT
        date_trunc('hour', event_time)                              AS hour,
        seq,
        lower(split_part(unnest(coalesce(langs, ['und'])), '-', 1)) AS lang
    FROM posts
),

per_hour AS (
    SELECT
        hour,
        lang,
        count(*)                AS posts,
        {{ current }}::BIGINT   AS silver_snapshot_id
    FROM post_langs
    GROUP BY ALL
)

SELECT * FROM per_hour
