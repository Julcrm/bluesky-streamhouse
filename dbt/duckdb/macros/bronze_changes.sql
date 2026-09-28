-- =============================================================================
-- Incremental reads of Bronze through the DuckLake change feed (decision D16).
--
-- Each Silver row carries `bronze_snapshot_id`, the Bronze snapshot its run read up
-- to. The next run reads only the rows inserted after that snapshot, whatever the
-- inlining, flush or CHECKPOINT that happened in between (checked locally on
-- 2026-09-28). The maintenance must never expire a snapshot newer than the lowest
-- `bronze_snapshot_id` still to be read (guard of decision D21).
-- =============================================================================

-- Bronze snapshot range to read: (last snapshot already read or none, snapshot to read up to).
-- A run reads at most var('silver_max_rows_per_run') Bronze rows, whole snapshots only
-- and at least one: dbt stages the batch in a temp table, so an unbounded batch (the
-- 07:00 catch-up, D10, or a first run on days of Bronze) would overflow the container.
-- A first run counts from the oldest snapshot still kept. Rows written before it (once
-- the maintenance has expired snapshots) are not counted and all come in that first
-- batch: a full refresh after expiry is not capped. The caller repeats the run until
-- `bronze_snapshot_id` reaches the current snapshot
{% macro bronze_snapshot_range() %}
    {%- if not execute -%}
        {{ return((none, 0)) }}
    {%- endif -%}
    {%- set bronze = source('bronze', 'bronze_events') -%}
    {%- set current = run_query(
        "SELECT id FROM ducklake_current_snapshot('" ~ bronze.database ~ "')"
    ).columns[0].values()[0] -%}
    {%- set last_read = none -%}
    {%- if is_incremental() -%}
        {%- set last_read = run_query(
            "SELECT max(bronze_snapshot_id) FROM " ~ this
        ).columns[0].values()[0] -%}
    {%- endif -%}
    {%- if last_read is not none and last_read >= current -%}
        {{ return((last_read, current)) }}
    {%- endif -%}
    {%- if last_read is none -%}
        {%- set read_from = run_query(
            "SELECT min(snapshot_id) FROM ducklake_snapshots('" ~ bronze.database ~ "')"
        ).columns[0].values()[0] -%}
    {%- else -%}
        {%- set read_from = last_read + 1 -%}
    {%- endif -%}
    {%- set read_up_to = run_query(
        "WITH per_snapshot AS ("
        ~ " SELECT snapshot_id, count(*) AS n"
        ~ " FROM ducklake_table_changes('" ~ bronze.database ~ "', '" ~ bronze.schema ~ "', '"
        ~ bronze.identifier ~ "', " ~ read_from ~ ", " ~ current ~ ")"
        ~ " WHERE change_type = 'insert' GROUP BY 1"
        ~ "), running AS ("
        ~ " SELECT snapshot_id, sum(n) OVER (ORDER BY snapshot_id) AS total FROM per_snapshot"
        ~ ") SELECT coalesce("
        ~ " max(snapshot_id) FILTER (WHERE total <= " ~ var('silver_max_rows_per_run') ~ "),"
        ~ " min(snapshot_id), " ~ current ~ ") FROM running"
    ).columns[0].values()[0] -%}
    {{ return((last_read, read_up_to)) }}
{% endmacro %}


-- Bronze rows not read yet, stamped with the snapshot this run reads up to.
-- First run (or empty model): the whole table as of the snapshot this run reads up to.
{% macro bronze_new_rows() %}
    {%- set bronze = source('bronze', 'bronze_events') -%}
    {%- set last_read, read_up_to = bronze_snapshot_range() -%}
    {%- if last_read is none -%}
        (
            SELECT *, {{ read_up_to }}::BIGINT AS bronze_snapshot_id
            FROM {{ bronze }} AT (VERSION => {{ read_up_to }})
        )
    {%- elif last_read >= read_up_to -%}
        (
            SELECT *, {{ read_up_to }}::BIGINT AS bronze_snapshot_id
            FROM {{ bronze }}
            WHERE false
        )
    {%- else -%}
        (
            SELECT *, {{ read_up_to }}::BIGINT AS bronze_snapshot_id
            FROM ducklake_table_insertions(
                '{{ bronze.database }}', '{{ bronze.schema }}', '{{ bronze.identifier }}',
                {{ last_read + 1 }}, {{ read_up_to }}
            )
        )
    {%- endif -%}
{% endmacro %}


-- Rows of `relation` whose `seq` is not in the model yet, one per `seq`.
-- Duplicates come from producer resumes and consumer replays: same event, same
-- event_time, so the lookup in the model only scans the batch's event_time range
{% macro deduplicate_on_seq(relation) %}
    SELECT *
    FROM {{ relation }}
    {%- if is_incremental() %}
    WHERE seq NOT IN (
        SELECT seq
        FROM {{ this }}
        WHERE event_time BETWEEN (SELECT min(event_time) FROM {{ relation }})
                             AND (SELECT max(event_time) FROM {{ relation }})
    )
    {%- endif %}
    QUALIFY row_number() OVER (PARTITION BY seq ORDER BY processed_at) = 1
{% endmacro %}
