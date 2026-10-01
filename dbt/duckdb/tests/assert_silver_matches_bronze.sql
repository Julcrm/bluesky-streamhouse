-- Assert that no Bronze event is lost by Silver and that Silver holds nothing absent
-- from Bronze, for the events processed in the last var('test_window') (D16, D25).
-- Bronze is read at the snapshot each model has read up to (its position in the
-- progress table, else its rows' max). Duplicates within Silver are the unique tests'.
--
-- Sets of seq, not counts: with a window on processed_at, a replayed duplicate can be
-- in the window while its first copy (the one Silver kept) is older, and counts would
-- differ for nothing. A seq is looked up in the other layer by its event_time range,
-- which every copy of an event shares (and which prunes the day-split files)

{{ config(meta={'dagster': {'ref': {'name': 'silver_posts'}}}) }}

{%- set models = [
    ('silver_posts', "collection = 'app.bsky.feed.post' AND operation IN ('create', 'update')"),
    ('silver_likes', "collection = 'app.bsky.feed.like' AND operation = 'create'"),
    ('silver_reposts', "collection = 'app.bsky.feed.repost' AND operation = 'create'"),
    ('silver_follows', "collection = 'app.bsky.graph.follow' AND operation = 'create'"),
    ('silver_deletes', "operation = 'delete'"),
] %}
{%- set bronze = source('bronze', 'bronze_events') %}

WITH
{%- for model, bronze_filter in models %}
{%- set read_up_to = 0 %}
{%- if execute %}
    {%- set read_up_to = run_query(
        "SELECT coalesce("
        ~ "(SELECT max(bronze_snapshot_id) FROM " ~ silver_progress_table()
        ~ " WHERE model = '" ~ model ~ "' AND done),"
        ~ " (SELECT max(bronze_snapshot_id) FROM " ~ ref(model) ~ "), 0)"
    ).columns[0].values()[0] %}
{%- endif %}

{{ model }}_bronze AS (
    SELECT seq, min(event_time) AS event_time
    FROM {{ bronze }} AT (VERSION => {{ read_up_to }})
    WHERE {{ bronze_filter }}
      AND processed_at >= now() - {{ var('test_window') }}
    GROUP BY seq
),

{{ model }}_silver AS (
    SELECT seq, event_time
    FROM {{ ref(model) }}
    WHERE processed_at >= now() - {{ var('test_window') }}
),

{{ model }}_missing AS (
    SELECT count(*) AS events
    FROM {{ model }}_bronze
    WHERE seq NOT IN (
        SELECT seq
        FROM {{ ref(model) }}
        WHERE event_time BETWEEN (SELECT min(event_time) FROM {{ model }}_bronze)
                             AND (SELECT max(event_time) FROM {{ model }}_bronze)
    )
),

{{ model }}_extra AS (
    SELECT count(*) AS events
    FROM {{ model }}_silver
    WHERE seq NOT IN (
        SELECT seq
        FROM {{ bronze }} AT (VERSION => {{ read_up_to }})
        WHERE {{ bronze_filter }}
          AND event_time BETWEEN (SELECT min(event_time) FROM {{ model }}_silver)
                             AND (SELECT max(event_time) FROM {{ model }}_silver)
    )
){{ "," if not loop.last }}
{%- endfor %}

{% for model, _ in models %}
SELECT '{{ model }}' AS model, 'missing from Silver' AS problem, events
FROM {{ model }}_missing WHERE events > 0
UNION ALL
SELECT '{{ model }}', 'absent from Bronze', events
FROM {{ model }}_extra WHERE events > 0
{% if not loop.last %}UNION ALL{% endif %}
{%- endfor %}
