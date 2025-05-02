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
            create_clustered_index(
                "{{ this }}", ["code_perm", "fiche", "annee", "freq", "population"]
            )
        ]
    )
}}
with
    facsources as (
        select
            pop.code_perm,
            pop.fiche,
            freq.client,
            pop.population,
            pop.annee,
            pop.freq,
            concat('(', freq.eco_cen, ') - ', centre.descr) as nom_centre,
            floor(
                datediff(
                    day,
                    convert(date, el.date_naissance, 112),
                    cast(concat(pop.annee, '0630') as date)
                )
                / 365.25
            ) as age_30_juin,
            /* format 112 = yyyymmdd */
            floor(
                datediff(
                    day,
                    convert(date, el.date_naissance, 112),
                    cast(concat(pop.annee, '0930') as date)
                )
                / 365.25
            ) as age_30_septembre,
            /* format 112 = yyyymmdd */
            freq.org,
            freq.eco_cen,
            freq.bat,
            freq.org_hor,
            org.descr as descr_org_hor,
            freq.date_deb,
            freq.date_fin_sifca,
            freq.activ_form,
            freq.cond_admiss,
            wlcon.cf_descr as descr_condadmiss,
            case
                when freq.date_fin_sifca is null or freq.date_fin_sifca = ''
                then 'En cours'
                else 'Terminé'
            end as etat_formation,
            freq.ind_transm,
            freq.prog,
            prog.descr_prog,
            prog.type_diplome,
            freq.raison_grat_scol,
            wlgratscol.cf_descr as descr_raison_grat_scol,
            freq.type_parcours,
            wlt.cf_descr as descr_type_parcours,
            freq.service_enseign,
            wld.cf_descr as descr_service_enseign,
            freq.motif_depart,
            mot.cf_descr as descr_motif_dep,
            freq.raison_depart,
            wl.cf_descr desc_raison_depart
        from {{ ref("stg_populations_adultes") }} as pop
        join {{ ref("dim_eleve_adultes") }} as el on el.code_perm = pop.code_perm
        join
            {{ ref("i_e_freq_adultes") }} as freq
            on freq.fiche = pop.fiche
            and freq.annee = pop.annee
            and freq.freq = pop.freq
        left join {{ ref("i_t_prog_adultes") }} prog on prog.prog = freq.prog  -- récupère la description des programmes
        left join  -- récupère la description du service d'enseignement                                                              
            {{ ref("i_t_wl_descr_adultes") }} wld
            on wld.code = freq.service_enseign
            and wld.nom_table = 'X_ServiceEnseign'
        left join  -- récupère la description des motifs de départ
            {{ ref("i_t_wl_descr_adultes") }} mot
            on mot.code = freq.motif_depart
            and mot.nom_table = 'X_MotifDep'
        left join  -- récupère la description des raisons de départ
            {{ ref("i_t_wl_descr_adultes") }} wl
            on wl.code = freq.raison_depart
            and wl.nom_table = 'X_RaisonDepart'
        left join  -- récupère la description des type de parcours
            {{ ref("i_t_wl_descr_adultes") }} wlt
            on wlt.code = freq.type_parcours
            and wlt.nom_table = 'X_TypeParcours'
        left join  -- récupère la description des conditions d'admission
            {{ ref("i_t_wl_descr_adultes") }} wlcon
            on wlcon.code = freq.cond_admiss
            and wlcon.nom_table = 'X_CondAdmiss'
        left join  -- récupère la description des raisons de la gratuité scolaire
            {{ ref("i_t_wl_descr_adultes") }} wlgratscol
            on wlgratscol.code = freq.raison_grat_scol
            and wlgratscol.nom_table = 'X_RaisonGratScol'
        left join  -- récupère la description de l'organisation d'horaire
            {{ ref("i_e_o_orghor_adultes") }} org
            on org.eco_cen = freq.eco_cen
            and org.org_hor = freq.org_hor
        inner join  -- récupère les noms des centres Fp et FGA
            {{ ref("i_t_ecocen_adultes") }} as centre on freq.eco_cen = centre.eco_cen
        where pop.annee >= {{ core_dashboards_store.get_current_year() - 5 }}
    )
select
    code_perm,
    fiche,
    client,
    population,
    annee,
    freq,
    nom_centre,
    case
        when age_30_juin < 16
        then '- de 16 ans'
        when age_30_juin between 16 and 17
        then '16-17 ans'
        when age_30_juin between 18 and 21
        then '18-21 ans'
        when age_30_juin between 22 and 25
        then '22-25 ans'
        when age_30_juin >= 26
        then '+ de 26 ans'
        else null
    end as interv_age,
    case
        when age_30_septembre < 16
        then '- de 16 ans'
        when age_30_septembre between 16 and 17
        then '16-17 ans'
        when age_30_septembre between 18 and 21
        then '18-21 ans'
        when age_30_septembre between 22 and 25
        then '22-25 ans'
        when age_30_septembre >= 26
        then '+ de 26 ans'
        else null
    end as interv_age_fp,
    age_30_juin,
    age_30_septembre,
    org,
    eco_cen,
    bat,
    org_hor,
    case
        when ind_transm = 1
        then 'Transmissible'
        when ind_transm = 0
        then 'Non-transmissible'
        else null
    end as ind_transm,
    descr_org_hor,
    date_deb,
    date_fin_sifca,
    activ_form,
    cond_admiss,
    descr_condadmiss,
    etat_formation,
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
from facsources
