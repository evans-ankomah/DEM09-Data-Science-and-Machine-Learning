/*
  Custom schema name macro.
  
  Override dbt's default behavior of prefixing with the profile schema.
  When a model specifies a custom schema (e.g., schema='silver'),
  use EXACTLY that schema name instead of 'public_silver'.
  
  This ensures Bronze → Silver → Gold schemas match what the 
  Airflow DAG validation tasks expect.
*/

{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ default_schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
