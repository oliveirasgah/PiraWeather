-- bounded_float(col, lo, hi): safe_float plus a physical-bounds filter.
-- Values outside [lo, hi] become NULL. Catches numeric source sentinels
-- (-6999, 7999, etc.) that safe_float can't detect because they're valid
-- numbers, not 'NaN'/'None' strings.
-- Usage: {{ bounded_float('"Tar"', -5, 50) }}
{% macro bounded_float(col, lo, hi) -%}
    case
      when {{ safe_float(col) }} between {{ lo }} and {{ hi }}
      then {{ safe_float(col) }}
    end
{%- endmacro %}
