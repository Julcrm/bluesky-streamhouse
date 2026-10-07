-- =============================================================================
-- Incremental reads of Bronze through the DuckLake change feed (decision D16).
--
-- Each Silver run reads the Bronze rows inserted after the snapshot its model last read
-- up to, whatever the inlining, flush or CHECKPOINT that happened in between (checked
-- locally on 2026-09-28). That snapshot is kept in `transform.meta.silver_progress`,
-- not derived from the model's own rows: a batch can hold no row for a model (a burst of
-- one account's likes holds no delete), and a position read from the rows would then
-- never move again (prod, 2026-09-28 to 2026-10-01). Rows still carry
-- `bronze_snapshot_id` for lineage. The maintenance must never expire a snapshot newer
-- than the lowest position still to be read (guard of decision D21).
-- =============================================================================

{% macro silver_progress_table() -%}
    transform.meta.silver_progress
{%- endmacro %}


-- on-run-start: the progress table, created once in the transform catalog
{% macro create_silver_progress() %}
    CREATE SCHEMA IF NOT EXISTS transform.meta;
    CREATE TABLE IF NOT EXISTS {{ silver_progress_table() }} (
        invocation_id       VARCHAR,
        model               VARCHAR,
        bronze_snapshot_id  BIGINT,
        done                BOOLEAN,
        recorded_at         TIMESTAMPTZ
    );
{% endmacro %}


-- Bronze snapshot the model has read up to, none if it must read everything (first run,
-- full refresh). Before the progress table existed, the position was the model's
-- max(bronze_snapshot_id): kept as a fallback so the switch needs no migration
{% macro silver_last_read() %}
    {%- if not is_incremental() -%}
        {{ return(none) }}
    {%- endif -%}
    {%- set last_read = run_query(
        "SELECT max(bronze_snapshot_id) FROM " ~ silver_progress_table()
        ~ " WHERE model = '" ~ this.identifier ~ "' AND done"
    ).columns[0].values()[0] -%}
    {%- if last_read is none -%}
        {%- set last_read = run_query(
            "SELECT max(bronze_snapshot_id) FROM " ~ this
        ).columns[0].values()[0] -%}
    {%- endif -%}
    {{ return(last_read) }}
{% endmacro %}


-- Post-hook of every Silver model, in the same transaction as its insert: the range
-- staged at compile time becomes the model's position, even when the batch was empty.
-- A failed model leaves its range pending and reads it again (deduplication on seq)
{% macro mark_silver_progress() %}
    UPDATE {{ silver_progress_table() }}
    SET done = true
    WHERE invocation_id = '{{ invocation_id }}' AND model = '{{ this.identifier }}'
{% endmacro %}

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
    {%- set last_read = silver_last_read() -%}
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
        ~ " FROM ducklake_table_insertions('" ~ bronze.database ~ "', '" ~ bronze.schema ~ "', '"
        ~ bronze.identifier ~ "', " ~ read_from ~ ", " ~ current ~ ")"
        ~ " GROUP BY 1"
        ~ "), running AS ("
        ~ " SELECT snapshot_id, sum(n) OVER (ORDER BY snapshot_id) AS total FROM per_snapshot"
        ~ ") SELECT coalesce("
        ~ " max(snapshot_id) FILTER (WHERE total <= " ~ var('silver_max_rows_per_run') ~ "),"
        ~ " min(snapshot_id), " ~ current ~ ") FROM running"
    ).columns[0].values()[0] -%}
    {{ return((last_read, read_up_to)) }}
{% endmacro %}


-- Stages the range this model reads up to, for mark_silver_progress. Called once per
-- model and dbt invocation, when the model is compiled right before it runs
{% macro stage_silver_progress(read_up_to) %}
    {%- if execute -%}
        {%- do run_query(
            "INSERT INTO " ~ silver_progress_table() ~ " VALUES ('" ~ invocation_id ~ "', '"
            ~ this.identifier ~ "', " ~ read_up_to ~ ", false, now())"
        ) -%}
    {%- endif -%}
{% endmacro %}


-- Bronze rows not read yet, stamped with the snapshot this run reads up to.
-- First run (or empty model): the whole table as of the snapshot this run reads up to.
{% macro bronze_new_rows() %}
    {%- set bronze = source('bronze', 'bronze_events') -%}
    {%- set last_read, read_up_to = bronze_snapshot_range() -%}
    {%- do stage_silver_progress(read_up_to) -%}
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
