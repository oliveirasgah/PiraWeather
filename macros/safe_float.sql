-- safe_float(col): cast TEXT to DOUBLE PRECISION.
-- Returns NULL for blank, 'NaN'/'nan', 'None', 'NULL'/'null', and IEEE NaN.
-- Real numeric values (including 0) pass through unchanged.
-- Usage: {{ safe_float('"Tar_AVG"') }}
{% macro safe_float(col) -%}
    nullif(
      case
        when upper(trim({{ col }})) in ('', 'NAN', 'NONE', 'NULL') then null
        else cast(trim({{ col }}) as double precision)
      end,
      'NaN'::double precision
    )
{%- endmacro %}
