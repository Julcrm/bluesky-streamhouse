-- Lowercase with Unicode *simple* case mapping, as DuckDB's lower() in the DuckDB branch.
-- Spark's lower() is Java's String.toLowerCase, which applies the *full* mapping in two
-- places: U+0130 (Turkish capital dotted I) becomes "i" + U+0307 (combining dot), and a
-- word-final capital sigma becomes final sigma. The parity run of 2026-10-06 found 24
-- posts whose Turkish hashtags differed ("i̇stanbul" vs "istanbul"). Mapping these two
-- characters first gives both engines the same tags (the metric contract).
{% macro simple_lower(expression) -%}
    lower(replace(replace({{ expression }}, '\u0130', 'I'), '\u03A3', '\u03C3'))
{%- endmacro %}
