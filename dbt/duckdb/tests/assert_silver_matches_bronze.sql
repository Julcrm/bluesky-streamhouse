-- Assert that no Bronze event is lost by Silver and that Silver holds nothing absent
-- from Bronze, for the events processed in the last var('test_window') (D16, D25).
-- Bronze is read at the snapshot each model has read up to (its position in the
-- progress table, else its rows' max). Duplicates within Silver are the unique tests'.
--
-- Sets of seq, not counts: with a window on processed_at, a replayed duplicate can be
-- in the window while its first copy (the one Silver kept) is older, and counts would
-- differ for nothing. A seq is looked up in the other layer by its event_time range,
-- which every copy of an event shares (and which prunes the day-split files)
--
-- Same logic as dbt/spark since 2026-10-16: Bronze and Silver are each read once per
-- model, as the union of both checks' rows, and compared in one full outer join
-- (missing = in Bronze's window, absent from Silver; extra = in Silver's window, absent
-- from Bronze). Reading more rows than each check needs cannot hide a problem: they are
-- real rows of the same layer and snapshot

{{ config(meta={'dagster': {'ref': {'name': 'silver_posts'}}}) }}

{%- set models = [
    ('silver_posts', "collection = 'app.bsky.feed.post' AND operation IN ('create', 'update')"),
    ('silver_likes', "collection = 'app.bsky.feed.like' AND operation = 'create'"),
    ('silver_reposts', "collection = 'app.bsky.feed.repost' AND operation = 'create'"),
    ('silver_follows', "collection = 'app.bsky.graph.follow' AND operation = 'create'"),
    ('silver_deletes', "operation = 'delete'"),
] %}
{%- set bronze = source('bronze', 'bronze_events') %}
{%- set since = "now() - " ~ var('test_window') %}

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

{{ model }}_silver_window AS (
    SELECT min(event_time) AS lower, max(event_time) AS upper
    FROM {{ ref(model) }}
    WHERE processed_at >= {{ since }}
),

{{ model }}_bronze AS (
    SELECT
        seq,
        bool_or(processed_at >= {{ since }}) AS in_window,
        min(event_time) FILTER (WHERE processed_at >= {{ since }}) AS window_event_time
    FROM {{ bronze }} AT (VERSION => {{ read_up_to }})
    WHERE {{ bronze_filter }}
      AND (processed_at >= {{ since }}
           OR event_time BETWEEN (SELECT lower FROM {{ model }}_silver_window)
                             AND (SELECT upper FROM {{ model }}_silver_window))
    GROUP BY seq
),

{{ model }}_silver AS (
    SELECT seq, bool_or(processed_at >= {{ since }}) AS in_window
    FROM {{ ref(model) }}
    WHERE processed_at >= {{ since }}
       OR event_time BETWEEN (SELECT min(window_event_time) FROM {{ model }}_bronze)
                         AND (SELECT max(window_event_time) FROM {{ model }}_bronze)
    GROUP BY seq
),

{{ model }}_check AS (
    SELECT
        count(*) FILTER (WHERE b.in_window AND s.seq IS NULL) AS missing,
        count(*) FILTER (WHERE s.in_window AND b.seq IS NULL) AS extra
    FROM {{ model }}_bronze AS b
    FULL OUTER JOIN {{ model }}_silver AS s ON b.seq = s.seq
){{ "," if not loop.last }}
{%- endfor %}

{#- Each check is referenced once (Spark inlines a CTE at every reference: two
    references would read Bronze twice) and turned into one row per problem -#}
SELECT model, problem, events
FROM (
    SELECT
        checks.model,
        problems.problem,
        CASE problems.problem WHEN 'missing from Silver' THEN checks.missing ELSE checks.extra END AS events
    FROM (
        {%- for model, _ in models %}
        SELECT '{{ model }}' AS model, missing, extra FROM {{ model }}_check
        {% if not loop.last %}UNION ALL{% endif %}
        {%- endfor %}
    ) AS checks
    CROSS JOIN (VALUES ('missing from Silver'), ('absent from Bronze')) AS problems (problem)
) AS results
WHERE events > 0
