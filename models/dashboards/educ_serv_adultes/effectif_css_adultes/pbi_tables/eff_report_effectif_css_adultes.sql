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

with
    agg as (
        select
            fac.code_perm,
            fac.fiche,
            client,
            population,
            annee,
            freq,
            interv_age,
            interv_age_fp,
            age_30_juin,
            age_30_septembre,
            org,
            eco_cen,
            nom_centre,
            bat,
            org_hor,
            descr_org_hor,
            etat_formation,
            el.genre,
            concat(el.prenom, ' ', el.nom) as prenom_nom,
            el.lang_matern,
            el.desc_lang_matern,
            el.ville,
            el.codepost,
            fac.indtransm,
            activform,
            condadmiss,
            descr_condadmiss,
            prog,
            descr_prog,
            type_diplome,
            raison_grat_scol,
            descr_raison_grat_scol,
            type_parcours,
            descr_type_parcours,
            service_enseign,
            descr_service_enseign,
            motif_depart,
            descr_motif_dep,
            raison_depart,
            desc_raison_depart
        from {{ ref("fact_freq_adultes") }} as fac
        inner join {{ ref("dim_eleve_adultes") }} as el on el.code_perm = fac.code_perm
    )
select
    nom_centre,
    client,
    prenom_nom,
    code_perm,
    fiche,
    population,
    annee as année,
    concat(annee, '-', annee + 1) as annnee_scolaire,
    freq,
    age_30_juin,
    age_30_septembre,
    org,
    eco_cen,
    bat,
    org_hor,
    indtransm,
    ville,
    codepost,
    interv_age,
    interv_age_fp,
    activform as groupe_horaire,
    condadmiss,
    concat(condadmiss, ' - ', descr_condadmiss) as condition_admission,
    descr_org_hor as organisation_horaire,
    etat_formation as etat_formation,
    genre as genre,
    lang_matern as lang_matern,
    desc_lang_matern as langue_maternelle,
    prog,
    case
        when descr_prog is null then descr_prog else concat(prog, ' - ', descr_prog)
    end as programme,
    type_diplome,
    raison_grat_scol,
    descr_raison_grat_scol,
    type_parcours,
    concat(type_parcours, ' - ', descr_type_parcours) as desc_type_parcours,
    service_enseign,
    case
        when descr_service_enseign is null
        then descr_service_enseign
        else concat(service_enseign, ' - ', descr_service_enseign)
    end as service_enseignement,
    motif_depart,
    descr_motif_dep,
    raison_depart,
    desc_raison_depart
from agg
