{% macro coalesce_modified_at(created_col, updated_col, ingested_col, fallback_col) -%}
  greatest(
    {{ created_col }},
    {{ updated_col }},
    {{ ingested_col }},
    {{ fallback_col }}
  )
{%- endmacro %}

{% macro make_pk(cols) -%}
  -- creates a stable string key for BigQuery merge unique_key
  concat({{ cols|map('string')|join(", '-' ,") }})
{%- endmacro %}
