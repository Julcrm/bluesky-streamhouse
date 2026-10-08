{{ config(partition_by=['hours(hour)']) }}

-- =============================================================================
-- Model       : gold_langs_hour
-- Description : New posts per UTC hour and language. A post declaring several
--               languages counts once per language; regional variants are folded
--               into the primary subtag (en-US -> en); no language -> 'und'.
-- Source      : silver_posts (hours touched since the last run, D19)
-- Output      : lakekeeper.gold.gold_langs_hour, partitioned by hours(hour)
-- =============================================================================

{%- set silver_models = ['silver_posts'] %}
{%- set hours, positions = gold_hours(silver_models) %}

WITH posts AS (
    SELECT event_time, seq, langs
    FROM {{ silver_for_hours('silver_posts', hours, positions) }}
    WHERE operation = 'create'
),

post_langs AS (
    SELECT DISTINCT
        date_trunc('HOUR', event_time)              AS hour,
        seq,
        lower(split_part(lang_tag, '-', 1))         AS lang
    FROM posts
    LATERAL VIEW explode(coalesce(langs, array('und'))) tags AS lang_tag
),

per_hour AS (
    SELECT
        hour,
        lang,
        count(*)                                            AS posts,
        {{ gold_snapshot_column(silver_models, positions) }} AS silver_snapshot_id
    FROM post_langs
    GROUP BY hour, lang
)

SELECT * FROM per_hour
