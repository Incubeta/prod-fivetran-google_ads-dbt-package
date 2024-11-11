{% macro add_custom_fields() %}
  {%- if var('googleads_custom_fields', None) is not none -%}
    {%- set custom_fields = var('googleads_custom_fields').split(',') -%}
    {%- for field in custom_fields -%}
      {%- if field == "conversions_value" -%}
      SAFE_CAST({{ field }} AS STRING) AS Total_conversion_value,
      {%- endif -%}
      SAFE_CAST({{ field }} AS STRING) AS {{ field }},
    {%- endfor -%}
  {%- endif -%}
{% endmacro %}
