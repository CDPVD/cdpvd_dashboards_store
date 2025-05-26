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
{% if execute %}
    {% set criteres_reussites = var("dashboards", {}).get("educ_serv_adultes", {}).get("effectif_css_adultes", {}).get("criteres_reussites", "(22, 12, 4, 2)")%}
    {{ affich_description_code("eff_report_effectif_css_adultes","desc_raison_depart","raison_depart",criteres_reussites,"formation des adultes ") }}
{% endif %}
{% set dims = [
   "programme",
   "groupe_horaire",
   "desc_type_parcours",
   "condition_admission",
   "nom_centre",
   "genre",
   "desc_raison_depart"
] %}
with
    donn_filtr as (
        select
            {% for dim in dims -%}
                case
                    when {{ dim }} is null or {{ dim }} = '' then '-' else {{ dim }}
                end as {{ dim }},
                {{ dim }} as {{ dim }}_bis,
            {%- endfor %}
            freq,
            prenom_nom,
            code_perm,
            fiche,
            population,
            année,
            annee_scolaire,
            eco_cen,
            bat,
            ind_transm,
            ville,
            interv_age,
            interv_age_fp,
            etat_formation,
            langue_maternelle,
            type_diplome,
            descr_raison_grat_scol,
            descr_motif_dep
        from {{ ref("eff_report_effectif_css_adultes") }}
        where
            raison_depart in {{ criteres_reussites }}
            and population = 'Formation professionnelle'
    ),
    all_combinations as (
        {% for i in range(2 ** dims | length) -%}
            select
                {% for dim in dims -%} {{ dim }} as {{ dim }}_bis, {% endfor -%}
                freq,
                prenom_nom,
                code_perm,
                fiche,
                population,
                année,
                annee_scolaire,
                eco_cen,
                bat,
                ind_transm,
                ville,
                interv_age,
                interv_age_fp,
                etat_formation,
                langue_maternelle,
                type_diplome,
                descr_raison_grat_scol,
                descr_motif_dep,
                {%- for j in range(dims | length) -%}
                    {% set col = dims[j] %}
                    {%- if ((i // (2 ** j)) % 2) == 1 -%} 'Tous' as {{ col }}
                    {%- else -%} {{ col }}
                    {%- endif -%}
                    {% if not loop.last %},{%- endif %}
                {%- endfor %}
            from donn_filtr
            {% if not loop.last -%}
                union all
            {% endif %}
        {%- endfor -%}
    )
select *, {{ dbt_utils.generate_surrogate_key(["année"] + dims) }} as id_filtre
from all_combinations
