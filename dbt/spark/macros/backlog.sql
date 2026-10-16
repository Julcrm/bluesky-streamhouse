-- =============================================================================
-- What is left to read after a run, logged as one JSON line for the catch-up loop of
-- the Dagster asset (src/dagster/spark_assets.py), the counterpart of the DuckDB branch's
-- src/processing/backlog.py. Measured by the Thrift server (D6 revised), parsed by
-- src/processing/spark/backlog.py: the code server holds no Iceberg client.
--   silver_rows: Bronze rows appended after the least advanced Silver position (from
--                the snapshot summaries, no data read); every Bronze row for a model
--                that never ran
--   gold_hours:  most pending hours of one Gold model
--   positions:   every read position, to check that a pass moved forward
-- =============================================================================

{% macro log_backlog() %}
    {%- if execute -%}
        {%- set bronze = source('bronze', 'bronze_events') -%}
        {%- set positions = {} -%}
        {%- set silver_rows = [] -%}
        {%- for node in graph.nodes.values()
                if node.resource_type == 'model' and node.fqn[1] == 'silver' -%}
            {%- set row = run_query(
                "SELECT bronze_snapshot_id FROM " ~ silver_progress_table()
                ~ " WHERE model = '" ~ node.name ~ "' AND done ORDER BY recorded_at DESC LIMIT 1"
            ).rows -%}
            {%- set appended =
                "SELECT coalesce(sum(CAST(coalesce(summary['added-records'], '0') AS BIGINT)), 0)"
                ~ " FROM " ~ bronze ~ ".snapshots WHERE operation = 'append'" -%}
            {%- if row -%}
                {%- set position = row[0][0] -%}
                {%- set left = run_query(
                    appended ~ " AND committed_at > (SELECT max(committed_at) FROM "
                    ~ bronze ~ ".snapshots WHERE snapshot_id = " ~ position ~ ")"
                ).rows[0][0] -%}
                {%- do positions.update({node.name: position | string}) -%}
            {%- else -%}
                {#- Never ran: the whole Bronze table is ahead of it (first Spark day) -#}
                {%- set left = run_query(appended).rows[0][0] -%}
                {%- do positions.update({node.name: "none"}) -%}
            {%- endif -%}
            {%- do silver_rows.append(left | int) -%}
        {%- endfor -%}
        {%- set gold = run_query(
            "SELECT coalesce(max(n), 0) FROM (SELECT model, count(DISTINCT hour) AS n FROM "
            ~ gold_pending_table() ~ " GROUP BY model)"
        ).rows -%}
        {%- do log("BLUESKY_BACKLOG " ~ tojson({
            "silver_rows": silver_rows | max if silver_rows else 0,
            "gold_hours": gold[0][0] | int,
            "positions": positions,
        }), info=True) -%}
    {%- endif -%}
{% endmacro %}


{#- Standalone measure (`dbt run-operation measure_backlog`): run-operation skips the
    on-run-start hooks, so the meta tables are created first (first run of a branch) -#}
{% macro measure_backlog() %}
    {%- do run_query("CREATE NAMESPACE IF NOT EXISTS " ~ bench_prefix() ~ "meta") -%}
    {%- do run_query(create_silver_progress()) -%}
    {%- do run_query(create_gold_progress()) -%}
    {%- do run_query(create_gold_pending()) -%}
    {%- do log_backlog() -%}
{% endmacro %}
