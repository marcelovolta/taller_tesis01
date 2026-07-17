{% macro replace_emojis(column) %}

  {# tell dbt about the seed dependency since ref() is inside run_query #}
  -- depends_on: {{ ref('emoji_map') }}

  {% set emoji_query %}
    SELECT emoji, replacement FROM {{ ref('emoji_map') }}
  {% endset %}

  {% if execute %}
    {% set results = run_query(emoji_query) %}
    {% set emojis      = results.columns[0].values() %}
    {% set replacements = results.columns[1].values() %}

    {% set expr = column %}
    {% for i in range(emojis | length) %}
      {% set expr %}
        REPLACE({{ expr }}, '{{ emojis[i] }}', ' {{ replacements[i] }} ')
      {% endset %}
    {% endfor %}

    {{ expr }}

  {% else %}
    {{ column }}
  {% endif %}

{% endmacro %}
