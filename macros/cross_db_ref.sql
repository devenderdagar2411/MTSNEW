{% macro cross_db_ref(model_name, layer) %}
  {% if layer == 'bronze' %}
    {% set db = var('bronze_database') %}
    {% set schema = var('bronze_schema') %}
  {% elif layer == 'silver' %}
    {% set db = var('silver_database') %}
    {% set schema = var('silver_schema') %}
  {% elif layer == 'gold' %}
    {% set db = var('gold_database') %}
    {% set schema = var('gold_schema') %}
  {% endif %}
  
  {{ db }}.{{ schema }}.{{ model_name }}
{% endmacro %}