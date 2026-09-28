-- Assert that each Silver model holds exactly the distinct Bronze events visible at the
-- snapshot it read up to: no loss and no duplicate across incremental runs (D16).
-- Checked on the last day only (var recent_rows_window)

{%- set models = [
    ('silver_posts', "collection = 'app.bsky.feed.post' AND operation IN ('create', 'update')"),
    ('silver_likes', "collection = 'app.bsky.feed.like' AND operation = 'create'"),
    ('silver_reposts', "collection = 'app.bsky.feed.repost' AND operation = 'create'"),
    ('silver_follows', "collection = 'app.bsky.graph.follow' AND operation = 'create'"),
    ('silver_deletes', "operation = 'delete'"),
] %}

{%- for model, bronze_filter in models %}
{%- set read_up_to = 0 %}
{%- if execute %}
    {%- set read_up_to = run_query(
        "SELECT coalesce(max(bronze_snapshot_id), 0) FROM " ~ ref(model)
    ).columns[0].values()[0] %}
{%- endif %}

SELECT
    '{{ model }}' AS model,
    (
        SELECT count(DISTINCT seq)
        FROM {{ source('bronze', 'bronze_events') }} AT (VERSION => {{ read_up_to }})
        WHERE {{ bronze_filter }}
          AND event_time >= now() - {{ var('recent_rows_window') }}
    ) AS bronze_events,
    (
        SELECT count(*)
        FROM {{ ref(model) }}
        WHERE event_time >= now() - {{ var('recent_rows_window') }}
    ) AS silver_events
WHERE bronze_events IS DISTINCT FROM silver_events
{% if not loop.last %}UNION ALL{% endif %}
{%- endfor %}
