{#
CDPVD Dashboards store
Copyright (C) 2024 CDPVD.

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU Affero General Public License as
published by the Free Software Foundation, either version 3 of the
License, or any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU Affero General Public License for more details.

You should have received a copy of the GNU Affero General Public License
along with this program.  If not, see <https://www.gnu.org/licenses/>.
#}
{% macro get_seed_values(seed_name, column_name, schema_suffix) %}
    -- Verifie si la seed existe dans le schéma voulu
    {%- set seed_relation = adapter.get_relation(
        database=target.database,
        schema=target.schema ~ schema_suffix,
        identifier=seed_name,
    ) -%}

    {% if seed_relation %}
        {% if execute %}
            {{ log("✅ La seed '" ~ seed_name ~ "' existe", info=True) }}
        {% endif %}

        -- Stocke les valeurs distinctes de la colonne d'interet
        {% set values = dbt_utils.get_column_values(
            table=seed_relation, column=column_name
        ) %}

        -- Si get_column_values retourne None (seed vide), renvoie une liste vide
        {% if values is none %}
            {% if execute %}
                {{ log("⚠️ La seed '" ~ seed_name ~ "' est vide", info=True) }}
            {% endif %}
            {{ return([]) }}
        {% else %} {{ return(values) }}
        {% endif %}

    {% else %}
        {% if execute %}
            {{ log("🔴 La seed '" ~ seed_name ~ "' n'existe pas", info=True) }}
        {% endif %}
        {{ return([]) }}
    {% endif %}
{% endmacro %}
