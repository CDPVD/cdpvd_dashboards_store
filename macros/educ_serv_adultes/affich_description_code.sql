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
{% macro affich_description_code(nom_table, desc_colonne, code_colonne, val_condition, nom_tableau=null, txt= "Les codes sélectionnés pour le tableau de bord ") %}
    {% if not nom_table or not desc_colonne or not code_colonne or not val_condition %}
        {{ exceptions.raise_compiler_error("Tous les arguments de la macro affich_description_code sont obligatoires sauf le nom du tableau de bord.") }}
    {% endif %}
    {% set where_clause = code_colonne ~ ' in ' ~ val_condition %}
    {% set data = dbt_utils.get_column_values(
        table=ref(nom_table),
        where=where_clause,
        column=desc_colonne
) %}
    {% if data %}
        {{ log(txt ~ nom_tableau ~ " sont les suivants "~ val_condition ~ ":",info=True,) }}
        {% for row in data %} {{ log("Code " ~ row, info=True) }} {% endfor %}
    {% else %} {{ log("Aucune donnée trouvée", info=True) }}
    {% endif %}
{% endmacro %}
