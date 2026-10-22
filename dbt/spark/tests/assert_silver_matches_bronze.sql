-- =============================================================================
-- Assert that every Silver model holds exactly the deduplicated Bronze events of its
-- collection and operation, compared as sets of seq (D25): none missing, none extra.
-- Bronze is read as of the snapshot the model has read up to (its latest done position
-- in meta.silver_progress), over the rows processed in the last var('test_window').
-- Same test as dbt/duckdb, with Iceberg time travel (VERSION AS OF).
--
-- One Bronze read per model (2026-10-16): Bronze and Silver are each read once, as the
-- union of both checks' rows, and compared in one full outer join (missing = in Bronze's
-- window, absent from Silver; extra = in Silver's window, absent from Bronze). The three
-- reads per model it replaced (bounds, missing, extra) made ~60 % of a Silver/Gold pass:
-- ~300 small streaming files read 15 times. Reading more rows than each check needs
-- cannot hide a problem: they are real rows of the same layer and snapshot.
-- The window start and the event_time bounds are computed first and inlined as
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
{%- set read_up_to = {} %}
{%- set bronze_bounds = {} %}
{%- set silver_bounds = {} %}
{%- if execute %}
    {%- set since = "TIMESTAMP '" ~ run_query(
        "SELECT CAST(now() - " ~ var('test_window') ~ " AS STRING)"
    ).rows[0][0] ~ "'" %}
    {#- Latest done position of every model, in one query -#}
    {%- for row in run_query(
        "SELECT model, bronze_snapshot_id FROM ("
        ~ " SELECT model, bronze_snapshot_id,"
        ~ " row_number() OVER (PARTITION BY model ORDER BY recorded_at DESC) AS rn"
        ~ " FROM " ~ silver_progress_table() ~ " WHERE done"
        ~ ") WHERE rn = 1"
    ).rows %}
        {%- do read_up_to.update({row[0]: row[1]}) %}
    {%- endfor %}
    {#- event_time bounds of every model's Bronze window, in one Bronze read. Read at the
        current snapshot: Bronze only gains rows (appends; compaction keeps them), so it
        holds each model's snapshot and its bounds contain theirs: wider bounds only
        prune less. The filters are disjoint, a CASE labels each row with its model -#}
    {%- set label %}
        CASE
        {%- for model, bronze_filter in models %}
            WHEN {{ bronze_filter }} THEN '{{ model }}'
        {%- endfor %}
        END
    {%- endset %}
    {%- for row in run_query(
        "SELECT " ~ label ~ " AS model, CAST(min(event_time) AS STRING), CAST(max(event_time) AS STRING)"
        ~ " FROM " ~ bronze ~ " WHERE processed_at >= " ~ since ~ " GROUP BY 1"
    ).rows %}
        {%- if row[0] is not none and row[1] is not none %}
            {%- do bronze_bounds.update({row[0]: ("TIMESTAMP '" ~ row[1] ~ "'", "TIMESTAMP '" ~ row[2] ~ "'")}) %}
        {%- endif %}
    {%- endfor %}
    {#- event_time bounds of every model's Silver window, in one query -#}
    {%- set silver_query %}
        {%- for model, _ in models %}
        SELECT '{{ model }}' AS model, CAST(min(event_time) AS STRING), CAST(max(event_time) AS STRING)
        FROM {{ ref(model) }} WHERE processed_at >= {{ since }}
        {% if not loop.last %}UNION ALL{% endif %}
        {%- endfor %}
    {%- endset %}
    {%- for row in run_query(silver_query).rows %}
        {%- if row[1] is not none %}
            {%- do silver_bounds.update({row[0]: ("TIMESTAMP '" ~ row[1] ~ "'", "TIMESTAMP '" ~ row[2] ~ "'")}) %}
        {%- endif %}
    {%- endfor %}
{%- endif %}

WITH
{%- for model, bronze_filter in models %}
{%- set snapshot = read_up_to.get(model) %}
{%- set bronze_at = bronze ~ " VERSION AS OF " ~ snapshot if snapshot is not none else bronze %}
{#- Without a read position the model has read nothing yet: no Bronze row is expected in
    Silver (missing side empty), Silver's rows are still checked against Bronze -#}
{%- set in_window = "processed_at >= " ~ since if snapshot is not none else "false" %}
{%- set b_bounds = bronze_bounds.get(model) if snapshot is not none else none %}
{%- set s_bounds = silver_bounds.get(model) %}

{{ model }}_bronze AS (
    SELECT seq, bool_or({{ in_window }}) AS in_window
    FROM {{ bronze_at }}
    WHERE {{ bronze_filter }}
      AND ({{ in_window }}
        {%- if s_bounds is not none %}
           OR event_time BETWEEN {{ s_bounds[0] }} AND {{ s_bounds[1] }}
        {%- endif %})
    GROUP BY seq
),

{{ model }}_silver AS (
    SELECT seq, bool_or(processed_at >= {{ since }}) AS in_window
    FROM {{ ref(model) }}
    WHERE processed_at >= {{ since }}
    {%- if b_bounds is not none %}
       OR event_time BETWEEN {{ b_bounds[0] }} AND {{ b_bounds[1] }}
    {%- endif %}
    GROUP BY seq
),

{{ model }}_check AS (
    SELECT
        count_if(b.in_window AND s.seq IS NULL) AS missing,
        count_if(s.in_window AND b.seq IS NULL) AS extra
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
