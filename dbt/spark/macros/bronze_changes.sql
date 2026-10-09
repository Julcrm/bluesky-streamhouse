-- =============================================================================
-- Incremental reads of Bronze with Iceberg's incremental append scan (phase 5, the
-- counterpart of the DuckDB branch's DuckLake change feed, D16).
--
-- `SELECT ... FROM t WITH ('start-snapshot-id' = a, 'end-snapshot-id' = b)` returns the
-- rows of the files appended after snapshot a up to b; snapshots that are not appends
-- (retention deletes, compaction rewrites) are skipped, so the maintenance never makes
-- Silver read anything twice. Checked locally on 2026-10-06 across delete and replace
-- snapshots.
--
-- Iceberg snapshot ids are random: snapshots are ordered by commit time, never by id.
-- The read position of each model lives in meta.silver_progress, append only: a row
-- staged before the insert (done = false), another one after it (done = true). Unlike
-- DuckLake, Iceberg commits table by table, so the insert and its position are two
-- commits: a crash in between reads the same range again, and the deduplication on seq
-- removes it (the contract already requires it).
-- =============================================================================

{% macro silver_progress_table() -%}
    {{ bench_prefix() }}meta.silver_progress
{%- endmacro %}


-- on-run-start
{% macro create_silver_progress() %}
    CREATE TABLE IF NOT EXISTS {{ silver_progress_table() }} (
        invocation_id       STRING,
        model               STRING,
        bronze_snapshot_id  BIGINT,
        done                BOOLEAN,
        recorded_at         TIMESTAMP
    ) USING iceberg
    TBLPROPERTIES ('format-version' = '2', 'write.parquet.compression-codec' = 'zstd')
{% endmacro %}


-- Current snapshot of an Iceberg table and its commit time
{% macro current_snapshot(relation) %}
    {%- set row = run_query(
        "SELECT h.snapshot_id, CAST(s.committed_at AS STRING) FROM " ~ relation ~ ".history AS h"
        ~ " JOIN " ~ relation ~ ".snapshots AS s ON s.snapshot_id = h.snapshot_id"
        ~ " WHERE h.is_current_ancestor ORDER BY h.made_current_at DESC LIMIT 1"
    ).rows -%}
    {{ return((row[0][0], row[0][1]) if row else (none, none)) }}
{% endmacro %}


-- Commit time of a snapshot still kept (fails clearly when it was expired: the
-- maintenance must never expire a snapshot a reader has not read past)
{% macro snapshot_time(relation, snapshot_id) %}
    {%- set row = run_query(
        "SELECT CAST(committed_at AS STRING) FROM " ~ relation ~ ".snapshots"
        ~ " WHERE snapshot_id = " ~ snapshot_id
    ).rows -%}
    {%- if not row -%}
        {{ exceptions.raise_compiler_error(
            "Snapshot " ~ snapshot_id ~ " of " ~ relation ~ " was expired before "
            ~ this.identifier ~ " read past it"
        ) }}
    {%- endif -%}
    {{ return(row[0][0]) }}
{% endmacro %}


-- Bronze snapshot the model has read up to (latest done position), none if it must
-- read everything (first run, full refresh)
{% macro silver_last_read() %}
    {%- if not is_incremental() -%}
        {{ return(none) }}
    {%- endif -%}
    {%- set row = run_query(
        "SELECT bronze_snapshot_id FROM " ~ silver_progress_table()
        ~ " WHERE model = '" ~ this.identifier ~ "' AND done"
        ~ " ORDER BY recorded_at DESC LIMIT 1"
    ).rows -%}
    {{ return(row[0][0] if row else none) }}
{% endmacro %}


-- (last snapshot read or none, snapshot to read up to). A run reads at most
-- var('silver_max_rows_per_run') Bronze rows, whole append snapshots only and at least
-- one (rows counted from the snapshot summaries, no data read). The caller repeats the
-- run until the position reaches the current snapshot
{% macro bronze_snapshot_range() %}
    {%- if not execute -%}
        {{ return((none, 0)) }}
    {%- endif -%}
    {%- set bronze = source('bronze', 'bronze_events') -%}
    {%- set current, current_time = current_snapshot(bronze) -%}
    {%- set last_read = silver_last_read() -%}
    {%- if current is none or (last_read is not none and last_read == current) -%}
        {{ return((last_read, current)) }}
    {%- endif -%}
    {%- set after = "" -%}
    {%- if last_read is not none -%}
        {%- set after = " AND committed_at > TIMESTAMP '" ~ snapshot_time(bronze, last_read) ~ "'" -%}
    {%- endif -%}
    {%- set rows = run_query(
        "WITH s AS ("
        ~ " SELECT snapshot_id, committed_at,"
        ~ " CAST(coalesce(summary['added-records'], '0') AS BIGINT) AS n"
        ~ " FROM " ~ bronze ~ ".snapshots"
        ~ " WHERE operation = 'append'" ~ after
        ~ " AND committed_at <= TIMESTAMP '" ~ current_time ~ "'"
        ~ "), running AS ("
        ~ " SELECT snapshot_id, committed_at,"
        ~ " sum(n) OVER (ORDER BY committed_at ROWS UNBOUNDED PRECEDING) AS total,"
        ~ " row_number() OVER (ORDER BY committed_at) AS rn FROM s"
        ~ ") SELECT snapshot_id FROM running"
        ~ " WHERE total <= " ~ var('silver_max_rows_per_run') ~ " OR rn = 1"
        ~ " ORDER BY committed_at DESC LIMIT 1"
    ).rows -%}
    {%- set read_up_to = rows[0][0] if rows else current -%}
    {%- if last_read is none -%}
        {#- First run: everything up to the cap, as of that snapshot -#}
        {{ return((none, read_up_to)) }}
    {%- endif -%}
    {{ return((last_read, read_up_to)) }}
{% endmacro %}


-- Stages the position this model reads up to (done = false), right before its insert
{% macro stage_silver_progress(read_up_to) %}
    {%- if execute and read_up_to is not none -%}
        {%- do run_query(
            "INSERT INTO " ~ silver_progress_table() ~ " VALUES ('" ~ invocation_id ~ "', '"
            ~ this.identifier ~ "', " ~ read_up_to ~ ", false, current_timestamp())"
        ) -%}
    {%- endif -%}
{% endmacro %}


-- Post-hook of every Silver model, after its insert committed: the staged position
-- becomes the model's (an empty batch moves it too)
{% macro mark_silver_progress() %}
    INSERT INTO {{ silver_progress_table() }}
    SELECT invocation_id, model, bronze_snapshot_id, true, current_timestamp()
    FROM {{ silver_progress_table() }}
    WHERE invocation_id = '{{ invocation_id }}' AND model = '{{ this.identifier }}' AND NOT done
{% endmacro %}


-- The Bronze rows not read yet, stamped with the snapshot this run reads up to, as
-- {'rows': relation SQL, 'lower': ..., 'upper': ...}: the event_time bounds of those rows
-- as TIMESTAMP literals (none when there is nothing to read). Spark pushes literal bounds
-- down to the Iceberg scan, not scalar subqueries: `BETWEEN (SELECT min(...))` reads the
-- whole table (checked with EXPLAIN on Spark 4.1 / Iceberg 1.11, 2026-10-09). Called once
-- per model: it stages the read position
{% macro bronze_batch() %}
    {%- set bronze = source('bronze', 'bronze_events') -%}
    {%- set last_read, read_up_to = bronze_snapshot_range() -%}
    {%- do stage_silver_progress(read_up_to) -%}
    {%- if read_up_to is none -%}
        {%- set rows -%}
        (SELECT *, CAST(NULL AS BIGINT) AS bronze_snapshot_id FROM {{ bronze }} WHERE false)
        {%- endset -%}
        {{ return({'rows': rows, 'lower': none, 'upper': none}) }}
    {%- elif last_read is none -%}
        {%- set scan = bronze ~ " VERSION AS OF " ~ read_up_to -%}
    {%- elif last_read == read_up_to -%}
        {%- set rows -%}
        (
            SELECT *, CAST({{ read_up_to }} AS BIGINT) AS bronze_snapshot_id
            FROM {{ bronze }} WHERE false
        )
        {%- endset -%}
        {{ return({'rows': rows, 'lower': none, 'upper': none}) }}
    {%- else -%}
        {%- set scan = bronze ~ " WITH ('start-snapshot-id' = '" ~ last_read
            ~ "', 'end-snapshot-id' = '" ~ read_up_to ~ "')" -%}
    {%- endif -%}
    {%- set rows -%}
        (
            SELECT *, CAST({{ read_up_to }} AS BIGINT) AS bronze_snapshot_id
            FROM {{ scan }}
        )
    {%- endset -%}
    {%- set lower, upper = event_time_bounds("SELECT event_time FROM " ~ scan) -%}
    {{ return({'rows': rows, 'lower': lower, 'upper': upper}) }}
{% endmacro %}


-- (min, max) of the event_time column of a query, as TIMESTAMP literals, (none, none)
-- when it has no rows. Inside the session time zone both ways, so the round trip is exact
{% macro event_time_bounds(query) %}
    {%- if not execute -%}
        {{ return((none, none)) }}
    {%- endif -%}
    {%- set row = run_query(
        "SELECT CAST(min(event_time) AS STRING), CAST(max(event_time) AS STRING) FROM ("
        ~ query ~ ")"
    ).rows[0] -%}
    {%- if row[0] is none -%}
        {{ return((none, none)) }}
    {%- endif -%}
    {{ return(("TIMESTAMP '" ~ row[0] ~ "'", "TIMESTAMP '" ~ row[1] ~ "'")) }}
{% endmacro %}


-- Rows of `relation` whose `seq` is not in the model yet, one per `seq` (Spark has no
-- QUALIFY: a ranked subquery; LEFT ANTI JOIN rather than NOT IN, which Spark plans as a
-- null-aware join). Only the event_time range of the `batch` (from bronze_batch) is
-- looked up in the model: duplicates are the same event, so they share its event_time.
-- Models deduplicate their typed rows, not the Bronze ones: the sort by seq then carries
-- no raw record (500 000 Bronze rows with their JSON overflowed a 900 MB heap). Same
-- result, duplicates are the same event
{% macro deduplicate_on_seq(relation, batch) %}
    SELECT ranked.* EXCEPT (_seq_rank)
    FROM (
        SELECT *, row_number() OVER (PARTITION BY seq ORDER BY processed_at) AS _seq_rank
        FROM {{ relation }}
    ) AS ranked
    {%- if is_incremental() and batch.lower is not none %}
    LEFT ANTI JOIN (
        SELECT seq
        FROM {{ this }}
        WHERE event_time BETWEEN {{ batch.lower }} AND {{ batch.upper }}
    ) AS known
        ON ranked.seq = known.seq
    {%- endif %}
    WHERE ranked._seq_rank = 1
{% endmacro %}
