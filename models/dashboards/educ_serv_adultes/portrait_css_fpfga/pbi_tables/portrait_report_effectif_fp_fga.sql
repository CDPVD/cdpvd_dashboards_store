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
            core_dashboards_store.stamp_model("dashboard_portrait_css_fpfga")
        ]
    )
}}

with
    agg as (
        select
            freq.code_perm,
            freq.fiche,
            freq.client,
            freq.population,
            freq.annee,
            freq.freq,
            freq.interv_age,
            freq.interv_age_fp,
            freq.age_30_juin,
            freq.age_30_septembre,
            freq.org,
            freq.eco_cen,
            freq.nom_centre,
            freq.bat,
            freq.org_hor,
            freq.descr_org_hor,
            freq.etat_formation,
            el.genre,
            concat(el.prenom, ' ', el.nom) as prenom_nom,
            el.lang_matern,
            el.desc_lang_matern,
            el.ville,
            el.code_post,
            freq.ind_transm,
            freq.activ_form,
            freq.donpers,
            case
                when freq.activ_form is null or freq.activ_form = ''
                then freq.donpers
                else freq.activ_form
            end as groupe_horaire,
            freq.cond_admiss,
            freq.descr_condadmiss,
            freq.prog,
            freq.descr_prog,
            freq.type_diplome,
            freq.raison_grat_scol,
            freq.descr_raison_grat_scol,
            freq.type_parcours,
            freq.descr_type_parcours,
            freq.service_enseign,
            freq.descr_service_enseign,
            freq.motif_depart,
            freq.descr_motif_dep,
            freq.raison_depart,
            freq.desc_raison_depart
        from {{ ref("fact_freq_adultes") }} as freq
        inner join {{ ref("dim_eleve_adultes") }} as el on el.code_perm = freq.code_perm
    )
select
    nom_centre,
    freq,
    prenom_nom,
    code_perm,
    fiche,
    population,
    annee as année,
    concat(annee, '-', annee + 1) as annee_scolaire,
    eco_cen,
    bat,
    ind_transm,
    ville,
    interv_age,
    interv_age_fp,
    case
        when groupe_horaire = '' or groupe_horaire is null then '-' else groupe_horaire
    end as groupe_horaire,
    concat(cond_admiss, ' - ', descr_condadmiss) as condition_admission,
    descr_org_hor as organisation_horaire,
    etat_formation as etat_formation,
    genre as genre,
    desc_lang_matern as langue_maternelle,
    prog,
    case
        when descr_prog is null then '-' else concat(prog, ' - ', descr_prog)
    end as programme,
    type_diplome,
    raison_grat_scol,
    concat(raison_grat_scol, ' - ', descr_raison_grat_scol) as descr_raison_grat_scol,
    type_parcours,
    concat(type_parcours, ' - ', descr_type_parcours) as desc_type_parcours,
    case
        when descr_service_enseign is null
        then descr_service_enseign
        else concat(service_enseign, ' - ', descr_service_enseign)
    end as service_enseignement,
    service_enseign,
    motif_depart,
    descr_motif_dep,
    raison_depart,
    case
        when desc_raison_depart is null
        then desc_raison_depart
        else concat(raison_depart, ' - ', desc_raison_depart)
    end as desc_raison_depart
from agg
