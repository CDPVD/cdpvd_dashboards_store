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
-- depends_on: {{ ref('portrait_report_effectif_fp_fga') }}
{{
    config(
        post_hook=[
            core_dashboards_store.stamp_model("dashboard_portrait_css_fpfga")
        ]
    )
}}

-- Définition de la liste des dimensions utilisées pour les agrégations et combinaisons
{% set dims = [
   "programme",
   "organisation_horaire",
   "desc_type_parcours",
   "service_enseignement",
   "nom_centre",
   "genre",
   "desc_raison_depart"
] %}

-- Récupération des critères de réussite depuis les variables dbt_project (ou valeur
-- par défaut)
{% if execute %}
    {% set criteres_reussites = var("dashboards", {}).get("educ_serv_adultes", {}).get("portrait_css_fpfga", {}).get("criteres_reussites", "(22, 12, 4, 2)") %}
    {% set serv_ens = var("dashboards", {}).get("educ_serv_adultes", {}).get("portrait_css_fpfga", {}).get("serv_ens_exclut", "") %}
    {% if serv_ens != '' %}
        {% set serv_ens_exclut = 'and service_enseign not in ' ~ serv_ens %}
        {# Appelle la macro affich_description_code pour afficher les descriptions des codes sélectionnées #}
        {{ affich_description_code("portrait_report_effectif_fp_fga","service_enseignement","service_enseign",serv_ens,"formation des adultes", "Les codes service d'enseignement exlus en réussite pour le tableau de bord ") }}
    {% else %} {% set serv_ens_exclut = '' %}
    {% endif %}

{% endif %}

with
    -- Préparation des données filtrées sur la population et l'état de formation
    agg_filt as (
        select
            année,
            raison_depart,
            -- Remplacement des valeurs nulles ou vides par '-' pour chaque dimension
            {% for dim in dims -%}
                case
                    when {{ dim }} is null or {{ dim }} = '' then '-' else {{ dim }}
                end as {{ dim }}
                {% if not loop.last %},{% endif %}
            {%- endfor -%}
        from {{ ref("portrait_report_effectif_fp_fga") }} as fac
        where
            etat_formation = 'Terminé'
            and population = 'Formation générale des adultes' {{ serv_ens_exclut }}
    ),
    -- Agrégation avec CUBE pour obtenir toutes les combinaisons possibles des
    -- dimensions
    agg_with_taux as (
        select
            année,
            {%- for dim in dims -%}
                coalesce({{ dim }}, 'Tous') as {{ dim }},
            {% endfor -%}
            count(*) as nombre_total,
            sum(
                case when raison_depart in {{ criteres_reussites }} then 1 else 0 end
            ) as nombre_total_by_reussite,
            avg(
                case when raison_depart in {{ criteres_reussites }} then 1. else 0. end
            ) as taux
        from agg_filt
        group by
            année, cube (
                {%- for dim in dims -%}
                    {{ dim }}{% if not loop.last %},{% endif %}
                {% endfor -%}
            )
    )
-- Sélection finale avec génération d'un identifiant unique pour chaque combinaison de
-- dimensions
select *,{{ dbt_utils.generate_surrogate_key(["année"] + dims) }} as id_filtre
from agg_with_taux
where nombre_total_by_reussite > 0
