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
{{
    config(
        post_hook=[
            core_dashboards_store.stamp_model("dashboard_effectif_css_adultes")
        ]
    )
}}
{% set dims = [
   "programme",
   "organisation_horaire",
   "desc_type_parcours",
   "service_enseignement",
   "nom_centre",
   "genre",
   "desc_raison_depart"
] %}
{% if execute %}
    {% set criteres_reussites = var("dashboards")["educ_serv_adultes"]["effectif_css_adultes"]["criteres_reussites"] or "(22, 12, 04, 02)" %}
{% endif %}
with
    agg_filt as (
        select
            année,
            raison_depart,
            {% for dim in dims -%}
                case
                    when {{ dim }} is null or {{ dim }} = '' then '-' else {{ dim }}
                end as {{ dim }}
                {% if not loop.last %},{% endif %}
            {%- endfor -%}
        from {{ ref("eff_report_effectif_css_adultes") }} as fac
        where
            etat_formation = 'Terminé' and population = 'Formation générale des adultes'
    ),
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
select *,{{ dbt_utils.generate_surrogate_key(["année"] + dims) }} as id_filtre
from agg_with_taux
where nombre_total_by_reussite > 0
