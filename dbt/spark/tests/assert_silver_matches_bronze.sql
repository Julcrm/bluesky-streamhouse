-- =============================================================================
-- Assert that every Silver model holds exactly the deduplicated Bronze events of its
-- collection and operation, compared as sets of seq (D25): none missing, none extra.
-- Bronze is read as of the snapshot the model has read up to (its latest done position
-- in meta.silver_progress), over the rows processed in the last var('test_window').
-- Same test as dbt/duckdb, with Iceberg time travel (VERSION AS OF) and anti joins.
-- The window start and each side's event_time bounds are computed first and inlined as
-- literals: Spark does not push scalar subquery bounds down to the Iceberg scan, which
-- then read Silver and Bronze whole (up to 15 min per run on 2026-10-08).
-- =============================================================================

{{ config(meta={'dagster': {'ref': {'name': 'silver_posts'}}}) }}

{%- set models = [
    ('silver_posts', "collection = 'app.bsky.feed.post' AND operation IN ('create', 'update')"),
    ('silver_likes', "collection = 'app.bsky.feed.like' AND operation = 'create'"),
    ('silver_reposts', "collection = 'app.bsky.feed.repost' AND operation = 'create'"),
    ('silver_follows', "collection = 'app.bsky.graph.follow' AND operation = 'create'"),
    ('silver_deletes', "operation = 'delete'"),
] %}
{%- set bronze = source('bronze', 'bronze_events') %}
{%- set since = none %}
{%- if execute %}
    {%- set since = "TIMESTAMP '" ~ run_query(
        "SELECT CAST(now() - " ~ var('test_window') ~ " AS STRING)"
    ).rows[0][0] ~ "'" %}
{%- endif %}

WITH
{%- for model, bronze_filter in models %}
{%- set read_up_to = none %}
{%- if execute %}
    {%- set rows = run_query(
        "SELECT bronze_snapshot_id FROM " ~ silver_progress_table()
        ~ " WHERE model = '" ~ model ~ "' AND done ORDER BY recorded_at DESC LIMIT 1"
    ).rows %}
    {%- set read_up_to = rows[0][0] if rows else none %}
{%- endif %}
{%- set bronze_at = bronze ~ " VERSION AS OF " ~ read_up_to if read_up_to is not none else bronze %}
{%- set bronze_lower, bronze_upper = (none, none) %}
{%- set silver_lower, silver_upper = (none, none) %}
{%- if read_up_to is not none %}
    {%- set bronze_lower, bronze_upper = event_time_bounds(
        "SELECT event_time FROM " ~ bronze_at ~ " WHERE " ~ bronze_filter
        ~ " AND processed_at >= " ~ since
    ) %}
{%- endif %}
{%- set silver_lower, silver_upper = event_time_bounds(
    "SELECT event_time FROM " ~ ref(model) ~ " WHERE processed_at >= " ~ since
) %}

{{ model }}_bronze AS (
    SELECT seq, min(event_time) AS event_time
    FROM {{ bronze_at }}
    WHERE {{ bronze_filter }}
      AND processed_at >= {{ since }}
      {%- if read_up_to is none %} AND false{% endif %}
    GROUP BY seq
),

{{ model }}_silver AS (
    SELECT seq, event_time
    FROM {{ ref(model) }}
    WHERE processed_at >= {{ since }}
),

{{ model }}_missing AS (
    SELECT count(*) AS events
    FROM {{ model }}_bronze AS b
    LEFT ANTI JOIN (
        SELECT seq
        FROM {{ ref(model) }}
        {%- if bronze_lower is none %}
        WHERE false
        {%- else %}
        WHERE event_time BETWEEN {{ bronze_lower }} AND {{ bronze_upper }}
        {%- endif %}
    ) AS s ON b.seq = s.seq
),

{{ model }}_extra AS (
    SELECT count(*) AS events
    FROM {{ model }}_silver AS s
    LEFT ANTI JOIN (
        SELECT seq
        FROM {{ bronze_at }}
        WHERE {{ bronze_filter }}
          {%- if silver_lower is none %}
          AND false
          {%- else %}
          AND event_time BETWEEN {{ silver_lower }} AND {{ silver_upper }}
          {%- endif %}
    ) AS b ON s.seq = b.seq
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
