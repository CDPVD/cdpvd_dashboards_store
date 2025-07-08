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
-- Récupère la variable criteres_reussites depuis les variables dbt_project, avec une
-- valeur par défaut si non définie
{% if execute %}
    {% set criteres_reussites = var("dashboards", {}).get("educ_serv_adultes", {}).get("portrait_css_fpfga", {}).get("criteres_reussites", "(22, 12, 4, 2)") %}
    {#
    Appelle la macro affich_description_code pour afficher les descriptions des raisons de réussite sélectionnées
    #}
    {{ affich_description_code("portrait_report_effectif_fp_fga","desc_raison_depart","raison_depart",criteres_reussites,"portrait_css_fpfga") }}
{% endif %}

-- Définition de la liste des dimensions utilisées pour les combinaisons et l'agrégation
{% set dims = [
   "programme",
   "organisation_horaire",
   "desc_type_parcours",
   "service_enseignement",
   "nom_centre",
   "genre",
   "desc_raison_depart"
] %}

with
    donn_filtr as (
        -- Sélectionne les données filtrées sur la population et les raisons de
        -- réussite,et remplace les valeurs nulles ou vides par '-' pour chaque
        -- dimension
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
        from {{ ref("portrait_report_effectif_fp_fga") }}
        where
            raison_depart in {{ criteres_reussites }}
            and population = 'Formation générale des adultes'
            and etat_formation = 'Terminé'
    ),
    all_combinations as (
        -- Génère toutes les combinaisons possibles des dimensions en remplaçant
        -- dynamiquement certaines par 'Tous'.
        -- Cela simule un GROUP BY CUBE pour obtenir des agrégations à tous les niveaux.
        {% for i in range(2 ** dims | length) -%}
            select
                {% for dim in dims -%} {{ dim }}_bis, {% endfor -%}
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
-- Sélection finale avec génération d'un identifiant unique pour chaque combinaison de
-- dimensions
select
    {% for dim in dims -%} {{ dim }}_bis, {% endfor -%}
    {{ dbt_utils.generate_surrogate_key(["année"] + dims) }} as id_filtre,
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
from all_combinations
