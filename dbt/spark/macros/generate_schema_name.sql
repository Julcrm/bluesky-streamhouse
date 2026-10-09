-- Use the custom schema as is (`silver`, `gold`) instead of dbt's `<target>_<custom>`,
-- prefixed in the controlled test (`bench_silver`, macros/bench.sql)
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ bench_prefix() }}{{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
