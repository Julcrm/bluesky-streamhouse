-- =============================================================================
-- Write options of the transform catalog (decision D14).
--
-- DuckLake writes Snappy unless the catalog says otherwise, and the option belongs to
-- each catalog (D22): the Quix sink sets it on Bronze, so dbt sets it on the catalog it
-- writes. Without it Silver was written in Snappy in prod until 2026-10-04 (163 B/row
-- for likes instead of 84 in zstd). set_option is idempotent and adds no snapshot;
-- CHECKPOINT rewrites merged files with the current option.
-- =============================================================================

-- on-run-start: persist the codec before any model writes
{% macro set_write_options() %}
    CALL transform.set_option('parquet_compression', '{{ var("parquet_compression") }}');
{% endmacro %}
