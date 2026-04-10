/*
  Override dbt's default schema name generation.

  By default dbt concatenates the target schema with the custom schema
  (e.g. target=silver + custom=silver → silver_silver). This override makes
  the custom schema the final schema name when one is set, so models land
  in the schema declared in dbt_project.yml (bronze / silver / gold).
*/
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
