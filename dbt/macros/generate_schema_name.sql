{#
  Use the custom schema as-is instead of dbt's default "<target schema>_<custom schema>".
  Without this override, +schema: analytics with target schema "analytics" builds
  "analytics_analytics", while the README and scripts/e2e.sh query "analytics".
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
