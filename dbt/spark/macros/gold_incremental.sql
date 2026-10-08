-- =============================================================================
-- Incremental Gold: rebuild only the hours touched by new Silver rows (D19, same
-- contract as branch B), with Iceberg's mechanisms.
--
-- Branch B orders every Silver change by the catalog-wide DuckLake snapshot id. Iceberg
-- has one snapshot history per table, so each Gold model keeps, per Silver table it
-- reads, the last Silver snapshot it has taken into account (meta.gold_progress), and
-- the hours it still has to rebuild (meta.gold_pending). Each run:
--   1. adds to gold_pending the hours of the rows appended to its Silver tables since
--      its positions (incremental append scans), then stages the new positions;
--   2. rebuilds at most var('gold_max_hours_per_run') pending hours, oldest first, from
--      Silver as of those positions, with an atomic dynamic overwrite of their
--      partitions (an hour that went past its top 50 loses its old rows too);
--   3. post-hook: marks the positions done and removes the rebuilt hours.
-- A crash between two steps only rebuilds an hour again: every step is idempotent.
-- The cap exists for the same reason as in branch B: an exact count(DISTINCT) over
-- dozens of hours does not fit the container.
-- =============================================================================

{% macro gold_progress_table() -%}
    meta.gold_progress
{%- endmacro %}

{% macro gold_pending_table() -%}
    meta.gold_pending
{%- endmacro %}


-- on-run-start
{% macro create_gold_progress() %}
    CREATE TABLE IF NOT EXISTS {{ gold_progress_table() }} (
        invocation_id       STRING,
        model               STRING,
        silver_model        STRING,
        silver_snapshot_id  BIGINT,
        done                BOOLEAN,
        recorded_at         TIMESTAMP
    ) USING iceberg
    TBLPROPERTIES ('format-version' = '2', 'write.parquet.compression-codec' = 'zstd')
{% endmacro %}

{% macro create_gold_pending() %}
    CREATE TABLE IF NOT EXISTS {{ gold_pending_table() }} (
        model               STRING,
        hour                TIMESTAMP,
        recorded_at         TIMESTAMP
    ) USING iceberg
    TBLPROPERTIES ('format-version' = '2', 'write.parquet.compression-codec' = 'zstd')
{% endmacro %}


-- Silver snapshot this Gold model last took into account for `silver_model`
{% macro gold_last_read(silver_model) %}
    {%- set row = run_query(
        "SELECT silver_snapshot_id FROM " ~ gold_progress_table()
        ~ " WHERE model = '" ~ this.identifier ~ "' AND silver_model = '" ~ silver_model
        ~ "' AND done ORDER BY recorded_at DESC LIMIT 1"
    ).rows -%}
    {{ return(row[0][0] if row else none) }}
{% endmacro %}


-- Steps 1 and 2 of the header. Returns (hours, positions): hours to rebuild (none means
-- every hour: first run) as 'YYYY-MM-DD HH:00:00' strings, and the Silver snapshot read
-- for each Silver model
{% macro gold_hours(silver_models) %}
    {%- if not execute -%}
        {{ return(([], {})) }}
    {%- endif -%}
    {%- set positions = {} -%}
    {%- for m in silver_models -%}
        {%- do positions.update({m: current_snapshot(ref(m))[0]}) -%}
    {%- endfor -%}
    {%- if not is_incremental() -%}
        {%- do stage_gold_progress(positions) -%}
        {{ return((none, positions)) }}
    {%- endif -%}
    {%- for m in silver_models -%}
        {%- set last_read = gold_last_read(m) -%}
        {%- set current = positions[m] -%}
        {%- if current is not none and last_read != current -%}
            {%- if last_read is none -%}
                {%- set appended = ref(m) ~ " VERSION AS OF " ~ current -%}
            {%- else -%}
                {%- set appended = ref(m) ~ " WITH ('start-snapshot-id' = '" ~ last_read
                    ~ "', 'end-snapshot-id' = '" ~ current ~ "')" -%}
            {%- endif -%}
            {%- do run_query(
                "INSERT INTO " ~ gold_pending_table()
                ~ " SELECT DISTINCT '" ~ this.identifier ~ "', date_trunc('HOUR', event_time),"
                ~ " current_timestamp() FROM " ~ appended
            ) -%}
        {%- endif -%}
    {%- endfor -%}
    {%- do stage_gold_progress(positions) -%}
    {{ return((pending_hours(), positions)) }}
{% endmacro %}


-- The oldest pending hours of this model, at most the cap. Run twice per model (the
-- model, then its post-hook): nothing else writes this model's pending hours between
{% macro pending_hours() %}
    {%- set rows = run_query(
        "SELECT DISTINCT date_format(hour, 'yyyy-MM-dd HH:00:00') AS h FROM "
        ~ gold_pending_table() ~ " WHERE model = '" ~ this.identifier ~ "'"
        ~ " ORDER BY h LIMIT " ~ var('gold_max_hours_per_run')
    ).rows -%}
    {%- set hours = [] -%}
    {%- for r in rows -%}
        {%- do hours.append(r[0]) -%}
    {%- endfor -%}
    {{ return(hours) }}
{% endmacro %}


{% macro stage_gold_progress(positions) %}
    {%- set values = [] -%}
    {%- for m, snapshot in positions.items() if snapshot is not none -%}
        {%- do values.append(
            "('" ~ invocation_id ~ "', '" ~ this.identifier ~ "', '" ~ m ~ "', "
            ~ snapshot ~ ", false, current_timestamp())"
        ) -%}
    {%- endfor -%}
    {%- if values -%}
        {%- do run_query(
            "INSERT INTO " ~ gold_progress_table() ~ " VALUES " ~ values | join(", ")
        ) -%}
    {%- endif -%}
{% endmacro %}


-- Post-hook of every Gold model, after its overwrite committed: positions done, rebuilt
-- hours no longer pending
{% macro mark_gold_done() %}
    {%- if execute -%}
        {%- set hours = pending_hours() -%}
        {%- if hours -%}
            {%- set literals = [] -%}
            {%- for h in hours -%}
                {%- do literals.append("TIMESTAMP '" ~ h ~ "'") -%}
            {%- endfor -%}
            {%- do run_query(
                "DELETE FROM " ~ gold_pending_table() ~ " WHERE model = '" ~ this.identifier
                ~ "' AND hour IN (" ~ literals | join(", ") ~ ")"
            ) -%}
        {%- endif -%}
    {%- endif -%}
    INSERT INTO {{ gold_progress_table() }}
    SELECT invocation_id, model, silver_model, silver_snapshot_id, true, current_timestamp()
    FROM {{ gold_progress_table() }}
    WHERE invocation_id = '{{ invocation_id }}' AND model = '{{ this.identifier }}' AND NOT done
{% endmacro %}


-- Silver model as of the snapshot this run read, restricted to the hours to rebuild.
-- One bounded scan per range of consecutive hours (Iceberg prunes the day partitions
-- and the files by their event_time min/max on a plain range)
{% macro silver_for_hours(silver_model, hours, positions) %}
    {%- set relation = ref(silver_model) ~ " VERSION AS OF " ~ positions[silver_model]
        if positions.get(silver_model) is not none else ref(silver_model) -%}
    (
        {%- if hours is none %}
        SELECT * FROM {{ relation }}
        {%- elif hours | length == 0 or positions.get(silver_model) is none %}
        SELECT * FROM {{ ref(silver_model) }} WHERE false
        {%- else %}
        {%- for start, end in hour_ranges(hours) %}
        SELECT * FROM {{ relation }}
        WHERE event_time >= TIMESTAMP '{{ start }}' AND event_time < TIMESTAMP '{{ end }}'
        {%- if not loop.last %}
        UNION ALL
        {%- endif %}
        {%- endfor %}
        {%- endif %}
    )
{% endmacro %}


-- Sorted 'YYYY-MM-DD HH:00:00' hours -> [(start, end)] ranges of consecutive hours,
-- end excluded
{% macro hour_ranges(hours) %}
    {%- set fmt = '%Y-%m-%d %H:%M:%S' -%}
    {%- set one_hour = modules.datetime.timedelta(hours=1) -%}
    {%- set ranges = [] -%}
    {%- set state = {'start': none, 'end': none} -%}
    {%- for h in hours | sort -%}
        {%- set t = modules.datetime.datetime.strptime(h, fmt) -%}
        {%- if state.end is not none and t == state.end -%}
            {%- do state.update({'end': t + one_hour}) -%}
        {%- else -%}
            {%- if state.start is not none -%}
                {%- do ranges.append((state.start.strftime(fmt), state.end.strftime(fmt))) -%}
            {%- endif -%}
            {%- do state.update({'start': t, 'end': t + one_hour}) -%}
        {%- endif -%}
    {%- endfor -%}
    {%- if state.start is not none -%}
        {%- do ranges.append((state.start.strftime(fmt), state.end.strftime(fmt))) -%}
    {%- endif -%}
    {{ return(ranges) }}
{% endmacro %}


-- Silver snapshot recorded in the Gold rows (lineage column of the contract): the one
-- of the model's first Silver table
{% macro gold_snapshot_column(silver_models, positions) %}
    CAST({{ positions.get(silver_models[0]) or 0 }} AS BIGINT)
{%- endmacro %}
