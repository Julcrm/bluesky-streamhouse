-- =============================================================================
-- Controlled test (decision D36): with `--target bench`, every namespace the project
-- reads or writes gets this prefix (bench_bronze, bench_silver, bench_gold, bench_meta).
-- Same SQL, isolated tables: each Iceberg table has its own state, so separate
-- namespaces are enough (the DuckDB project switches catalogs instead).
-- =============================================================================

{% macro bench_prefix() -%}
    {{- 'bench_' if target.name == 'bench' else '' -}}
{%- endmacro %}
