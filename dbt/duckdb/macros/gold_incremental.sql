-- =============================================================================
-- Incremental Gold: rebuild only the hours touched by new Silver rows (decision D19).
--
-- Each Gold row carries `silver_snapshot_id`, the transform catalog snapshot its run read Silver at.
-- The next run lists the hours of the Silver rows inserted after that snapshot (DuckLake
-- change feed, as in D16), recomputes those whole hours from Silver frozen at the current
-- snapshot, and replaces them (delete+insert on the hour or minute key). Late rows, such
-- as the 07:00 catch-up (D10), rebuild their own hours whatever their age.
--
-- A run rebuilds at most var('gold_max_hours_per_run') hours, whole Silver snapshots
-- only and at least one: after a long stop every hour is touched at once, and an exact
-- count(DISTINCT) over them overflows the container (prod, 2026-10-01). The rest waits
-- for the next run. Unlike Silver, keeping the position in the rows is safe: a rebuild
-- that writes no row only rebuilds a few hours again, it never stalls.
-- =============================================================================

-- Lake snapshot range: (snapshot Gold last read Silver at, or none, snapshot to read up
-- to), capped at var('gold_max_hours_per_run') hours touched in `silver_models`
{% macro gold_snapshot_range(silver_models) %}
    {%- if not execute -%}
        {{ return((none, 0)) }}
    {%- endif -%}
    {%- set current = run_query(
        "SELECT id FROM ducklake_current_snapshot('" ~ this.database ~ "')"
    ).columns[0].values()[0] -%}
    {%- set last_read = none -%}
    {%- if is_incremental() -%}
        {%- set last_read = run_query(
            "SELECT max(silver_snapshot_id) FROM " ~ this
        ).columns[0].values()[0] -%}
    {%- endif -%}
    {%- if last_read is none or last_read >= current -%}
        {{ return((last_read, current)) }}
    {%- endif -%}
    {%- set selects = [] -%}
    {%- for m in silver_models -%}
        {%- set rel = ref(m) -%}
        {%- do selects.append(
            "SELECT snapshot_id, date_trunc('hour', event_time) AS hour"
            ~ " FROM ducklake_table_insertions('" ~ rel.database ~ "', '" ~ rel.schema ~ "', '"
            ~ rel.identifier ~ "', " ~ (last_read + 1) ~ ", " ~ current ~ ")"
        ) -%}
    {%- endfor -%}
    {#- Each hour counts once, at the first snapshot touching it. The run reads up to the
        snapshot that would bring the first hour over the cap, excluded: the snapshots in
        between only touch hours already taken (one dbt pass commits each Silver model
        in its own snapshot, often over the same hours). The first snapshot is always
        read, whatever its number of hours #}
    {%- set read_up_to = run_query(
        "WITH first_touch AS ("
        ~ " SELECT hour, min(snapshot_id) AS snapshot_id FROM ("
        ~ selects | join(" UNION ALL ") ~ ") GROUP BY hour"
        ~ "), per_snapshot AS ("
        ~ " SELECT snapshot_id, count(*) AS n FROM first_touch GROUP BY 1"
        ~ "), running AS ("
        ~ " SELECT snapshot_id, sum(n) OVER (ORDER BY snapshot_id) AS total,"
        ~ " snapshot_id = min(snapshot_id) OVER () AS is_first FROM per_snapshot"
        ~ ") SELECT coalesce(min(snapshot_id) - 1, " ~ current ~ ") FROM running"
        ~ " WHERE total > " ~ var('gold_max_hours_per_run') ~ " AND NOT is_first"
    ).columns[0].values()[0] -%}
    {{ return((last_read, read_up_to)) }}
{% endmacro %}


-- UTC hours of the rows inserted into `silver_models` in (last_read, current], merged
-- into ranges of consecutive hours: [(start, end), ...] as TIMESTAMPTZ literals, end
-- excluded; none means "all hours" (first run)
{% macro touched_ranges(silver_models, last_read, current) %}
    {%- if not execute or last_read is none -%}
        {{ return(none) }}
    {%- endif -%}
    {%- if last_read >= current -%}
        {{ return([]) }}
    {%- endif -%}
    {%- set selects = [] -%}
    {%- for m in silver_models -%}
        {%- set rel = ref(m) -%}
        {%- do selects.append(
            "SELECT DISTINCT date_trunc('hour', event_time) AS hour"
            ~ " FROM ducklake_table_insertions('" ~ rel.database ~ "', '" ~ rel.schema ~ "', '"
            ~ rel.identifier ~ "', " ~ (last_read + 1) ~ ", " ~ current ~ ")"
        ) -%}
    {%- endfor -%}
    {#- Gaps and islands: consecutive hours share hour - row_number() hours #}
    {%- set rows = run_query(
        "WITH hours AS (SELECT DISTINCT hour FROM (" ~ selects | join(" UNION ALL ") ~ ")),"
        ~ " islands AS (SELECT hour, hour - to_hours(row_number() OVER (ORDER BY hour)) AS island"
        ~ " FROM hours)"
        ~ " SELECT strftime(min(hour), '%Y-%m-%d %H:00:00+00'),"
        ~ " strftime(max(hour) + INTERVAL 1 HOUR, '%Y-%m-%d %H:00:00+00')"
        ~ " FROM islands GROUP BY island ORDER BY 1"
    ).rows -%}
    {%- set ranges = [] -%}
    {%- for r in rows -%}
        {%- do ranges.append((r[0], r[1])) -%}
    {%- endfor -%}
    {{ return(ranges) }}
{% endmacro %}


-- Silver model frozen at the snapshot this run reads, restricted to the hours to rebuild.
-- One bounded scan per range of hours: DuckLake skips files by their event_time min/max
-- only on a plain range (an OR of ranges, or a lower bound alone, read every file since
-- the oldest hour: 132 s per run on gold_active_users_hour during the 2026-10-01 catch-up)
{% macro silver_for_hours(silver_model, ranges, current) %}
    (
        {%- if ranges is none %}
        SELECT *
        FROM {{ ref(silver_model) }} AT (VERSION => {{ current }})
        {%- elif ranges | length == 0 %}
        SELECT *
        FROM {{ ref(silver_model) }} AT (VERSION => {{ current }})
        WHERE false
        {%- else %}
        {%- for start, end in ranges %}
        SELECT *
        FROM {{ ref(silver_model) }} AT (VERSION => {{ current }})
        WHERE event_time >= TIMESTAMPTZ '{{ start }}' AND event_time < TIMESTAMPTZ '{{ end }}'
        {%- if not loop.last %}
        UNION ALL
        {%- endif %}
        {%- endfor %}
        {%- endif %}
    )
{% endmacro %}
