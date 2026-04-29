-- Override default schema concatenation: when a custom schema is set on a model
-- (bronze/silver/gold), use it as the final name instead of {target}_{custom}.
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
