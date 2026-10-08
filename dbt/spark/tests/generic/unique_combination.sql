-- Generic test: the combination of `columns` is unique (no dbt_utils dependency, so the
-- image needs no `dbt deps` at build time)
{% test unique_combination(model, columns) %}
SELECT {{ columns | join(', ') }}, count(*) AS rows
FROM {{ model }}
GROUP BY {{ columns | join(', ') }}
HAVING count(*) > 1
{% endtest %}
