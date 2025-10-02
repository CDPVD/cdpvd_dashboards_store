{% macro get_seed_values(seed_name, column_name, schema_suffix) %}
    -- Verifie si la seed existe dans le schéma voulu
    {%- set seed_relation = adapter.get_relation(
        database=target.database,
        schema=target.schema ~ schema_suffix,
        identifier=seed_name                   
    ) -%}

    {% if seed_relation %}
        {% if execute %}
            {{ log("✅ La seed '" ~ seed_name ~ "' existe", info=True) }}
        {% endif %}

        -- Stocke les valeurs distinctes de la colonne d'interet
        {% set values = dbt_utils.get_column_values(
            table=seed_relation,
            column=column_name
        ) %}

        -- Si get_column_values retourne None (seed vide), renvoie une liste vide
        {% if values is none %}
            {% if execute %}
                {{ log("⚠️ La seed '" ~ seed_name ~ "' est vide", info=True) }}
            {% endif %}
            {{ return([]) }}
        {% else %}
            {{ return(values) }}
        {% endif %}

    {% else %}
        {% if execute %}
            {{ log("🔴 La seed '" ~ seed_name ~ "' n'existe pas", info=True) }}
        {% endif %}
        {{ return([]) }}
    {% endif %}
{% endmacro %}
