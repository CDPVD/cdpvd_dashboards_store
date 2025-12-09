{#
Dashboards Store - Helping students, one dashboard at a time.
Copyright (C) 2023  Sciance Inc.

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
{#
    This table unionize the always-present DEFAULT table and maybe-present CUSTOM table.
    The default table is defined in the core repo while the custom table, as all the CSS''s specifics table is created in the repo css.

    The code check for the custom table existence and adds it to the default table
    For the CUSTOM table to be detected, the table must be :
        * named 'custom_cibles_indicateurs_annuelles_pevr_css'
        * located in the schema 'dashboard_pevr_seeds'
#}
{{ config(alias="pevr_dim_cible_annuelle_cdpvd") }}

{%- set source_relation_css = adapter.get_relation(
    database=target.database,
    schema=target.schema + "_dashboard_pevr_seeds",
    identifier="custom_cibles_indicateurs_annuelles_pevr_css",
) -%}

-- La relation de la table dimension cibles_annuelles du core
{%- set source_relation_core = adapter.get_relation(
    database=target.database,
    schema=target.schema + "_dashboard_pevr",
    identifier="dim_cibles_annuelles",
) -%}
--

-- A des fin de débogage.
/* {{ log("schema css = " ~ source_relation_css.schema, true) }} 
{{ log("identifier css" ~ source_relation_css.identifier, true) }}
{{ log("database css = " ~ source_relation_css.database, true) }}

{{ log("schema core: " ~ source_relation_core.schema, true) }}
{{ log("identifier core: " ~ source_relation_core.identifier, true) }}
{{ log("database core: " ~ source_relation_core.database, true) }} */

{% set table_exists_css = source_relation_css is not none %}
{% set table_exists_core = source_relation_core is not none %}

{% if table_exists_css %}
    {% if table_exists_core %}
    -- La seed css et la dimension du core existe.
        {% if execute %}
            {{
                log(
                    "✅  La seed css '*_dashboard_pevr_seeds.custom_cibles_indicateurs_annuelles_pevr_css' existe ET la dimension core 'pevr_dim_cibles_annuelles' existe.
                    Une union va donc être fait entre la seed css 'custom_cibles_indicateurs_annuelles_pevr_css' et la dimension core 'pevr_dim_cibles_annuelles' pour créer
                    la dimension cdpvd 'pevr_dim_cible_annuelle_cdpvd'.",
                    true,
                )
            }}
        {% endif %}

        select id_indicateur_meq, id_indicateur_cdpvd, id_indicateur_css, annee_scolaire, cible
        from {{ source_relation_css }} -- La seed custom de la css
        Union
        Select id_indicateur_meq, null as id_indicateur_cdpvd, id_indicateur_css, annee_scolaire, cible
        from {{ source_relation_core}} -- La dimension du core
        
    {% else %}
    -- La seed css existe, mais pas la dimension du core.
        {% if execute %}
            {{
                log(
                    "⚠️  La seed '*_dashboard_pevr_seeds.custom_cibles_indicateurs_annuelles_pevr_css' existe mais la dimension 'pevr_dim_cibles_annuelles' n'existe pas.
                    Par défaut, la dimension 'pevr_dim_cible_annuelle_cdpvd' va prendre les informations de la seed 'custom_cibles_indicateurs_annuelles_pevr_css'.",
                    true,
                )
            }}
        {% endif %}

    select id_indicateur_meq, id_indicateur_cdpvd, id_indicateur_css, annee_scolaire, cible
    from {{ source_relation_css }}  -- La seed custom de la css

    {% endif %}

{% else %}
    {% if table_exists_core %}
    -- La seed CSS n'existe pas, mais la dimension du core existe
        {% if execute %}
            {{
                log(
                    "⚠️  La seed '*_dashboard_pevr_seeds.custom_cibles_indicateurs_annuelles_pevr_css' n'existe pas, mais la dimension 'pevr_dim_cibles_annuelles' existe. Une union va donc avoir lieu entre la seed cdpvd 'cibles_indicateurs_annuelles_pevr_cdpvd' et la dimension core 'pevr_dim_cibles_annuelles' pour créer la dimension cdpvd 'pevr_dim_cible_annuelle_cdpvd'",
                    true,
                )
            }}
        {% endif %}

        select id_indicateur_meq, id_indicateur_cdpvd, id_indicateur_css, annee_scolaire, cible
        from {{ ref("cibles_indicateurs_annuelles_pevr_cdpvd") }}
        Union
        Select id_indicateur_meq, null as id_indicateur_cdpvd, id_indicateur_css, annee_scolaire, cible
        from {{ source_relation_core}} -- La dimension du core

    {% else %}
        -- La seed CSS n'existe pas et la dimensin du core n'existe pas.
        {% if execute %}
            {{
                log(
                    "⚠️  La seed '*_dashboard_pevr_seeds.custom_cibles_indicateurs_annuelles_pevr_css' n'existe pas et la dimension du core 'pevr_dim_cibles_annuelles' existe pas. La dimension cdpvd 'pevr_dim_cible_annuelle_cdpvd' va prendre les données de la seed cdpvd 'cibles_indicateurs_annuelles_pevr_cdpvd'.",
                    true,
                )
            }}
        {% endif %}

    select id_indicateur_meq, id_indicateur_cdpvd, id_indicateur_css, annee_scolaire, cible
    from {{ ref("cibles_indicateurs_annuelles_pevr_cdpvd") }}

    {% endif %}
{% endif %}