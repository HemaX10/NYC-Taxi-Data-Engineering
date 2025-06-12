{% macro safe_integer_cast(field_name) %}
    FLOOR(COALESCE(CAST({{ field_name }} AS numeric), 0))::integer
{% endmacro %}