-- =============================================================================
-- Incremental Gold: rebuild only the hours touched by new Silver rows (decision D19).
--
-- Each Gold row carries `silver_snapshot_id`, the transform catalog snapshot its run read Silver at.
-- The next run lists the hours of the Silver rows inserted after that snapshot (DuckLake
-- change feed, as in D16), recomputes those whole hours from Silver frozen at the current
-- snapshot, and replaces them (delete+insert on the hour or minute key). Late rows, such
-- as the 07:00 catch-up (D10), rebuild their own hours whatever their age.
-- =============================================================================

-- Lake snapshot range: (snapshot Gold last read Silver at, or none, current snapshot)
{% macro gold_snapshot_range() %}
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
    {{ return((last_read, current)) }}
{% endmacro %}


-- Distinct UTC hours of the rows inserted into `silver_models` in (last_read, current],
-- as SQL TIMESTAMPTZ literals; none means "all hours" (first run)
{% macro touched_hours(silver_models, last_read, current) %}
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
    {%- set hours = run_query(
        "SELECT DISTINCT strftime(hour, '%Y-%m-%d %H:00:00+00') FROM ("
        ~ selects | join(" UNION ALL ") ~ ") ORDER BY 1"
    ).columns[0].values() -%}
    {{ return(hours | list) }}
{% endmacro %}


-- Silver model frozen at the snapshot this run reads, restricted to the hours to rebuild.
-- The lower bound is a constant so DuckLake prunes the day-split files
{% macro silver_for_hours(silver_model, hours, current) %}
    (
        SELECT *
        FROM {{ ref(silver_model) }} AT (VERSION => {{ current }})
        {%- if hours is not none %}
        {%- if hours | length == 0 %}
        WHERE false
        {%- else %}
        WHERE event_time >= TIMESTAMPTZ '{{ hours[0] }}'
          AND date_trunc('hour', event_time) IN (
              {%- for h in hours %}TIMESTAMPTZ '{{ h }}'{{ ", " if not loop.last }}{% endfor -%}
          )
        {%- endif %}
        {%- endif %}
    )
{% endmacro %}
