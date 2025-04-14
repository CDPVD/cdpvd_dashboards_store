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

with agg_filt as (
    select 
        nom_centre,
        freq,
        prenom_nom,
        code_perm,
        fiche,
        population,
        année,
        annnee_scolaire,
        eco_cen,
        concat('batisse - ',bat) bat,
        indtransm,
        ville,
        interv_age,
        interv_age_fp,
        coalesce(groupe_horaire,'-') as groupe_horaire,
        condition_admission,
        organisation_horaire,
        etat_formation,
        genre,
        langue_maternelle,
        prog,
        programme,
        type_diplome,
        descr_raison_grat_scol,
        type_parcours,
        desc_type_parcours,
        service_enseignement,
        descr_motif_dep,
        raison_depart,
        desc_raison_depart
    from {{ ref("eff_report_effectif_css_adultes") }} as fac
    where etat_formation = 'Terminé' and population = 'Formation professionnelle'
),
agg_with_taux as (
    select 
        année,
        COALESCE(groupe_horaire, 'Tous') as groupe_horaire,
        COALESCE(programme, 'Tous') as programme,
        COALESCE(desc_type_parcours, 'Tous') as desc_type_parcours,
        COALESCE(desc_raison_depart, 'Tous') as desc_raison_depart,
        COALESCE(condition_admission, 'Tous') as condition_admission,
        COALESCE(genre, 'Tous') as genre,
        COALESCE(nom_centre, 'Tous') as nom_centre,
        count(*) as nombre_total, 
        sum(case when raison_depart in (22, 12, 04, 02) then 1 else 0 end) as nombre_total_by_reussite,
            avg(case when raison_depart in (22, 12, 04, 02) then 1. else 0. end )
         as taux 
    from agg_filt
    group by année, cube (
        groupe_horaire, 
        programme, 
        desc_type_parcours, 
        desc_raison_depart, 
        condition_admission, 
        genre, 
        nom_centre
    )
)
select 
    année,
    groupe_horaire,
    programme,
    desc_type_parcours,
    desc_raison_depart,
    condition_admission,
    genre,
    nom_centre,
    nombre_total,
    nombre_total_by_reussite,
    taux
from agg_with_taux
where nombre_total_by_reussite > 0