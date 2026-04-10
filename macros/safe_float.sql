/*
  safe_float(col)
  ──────────────
  Casts a TEXT bronze column to DOUBLE PRECISION, returning NULL for
  blank / whitespace-only values instead of raising a cast error.

  Usage:  {{ safe_float('"Tar_AVG"') }}
*/
{% macro safe_float(col) -%}
    cast(nullif(trim({{ col }}), '') as double precision)
{%- endmacro %}
