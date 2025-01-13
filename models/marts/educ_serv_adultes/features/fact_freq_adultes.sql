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
select
    pop.code_perm,
    pop.fiche,
    freq.client,
    pop.population,
    pop.annee,
    pop.freq,
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
    freq.ActivForm,
    freq.Cohorte,
    freq.DonPers,
    freq.CondAdmiss,
    wlcon.cf_descr as descr_condadmiss,
    case
        when freq.date_fin_sifca is null or freq.date_fin_sifca = '' then 'En cours'
        else 'Terminé'
    END as etat_formation,
    freq.prog,
    prog.prog_meq,
    prog.descr_prog,
    prog.type_diplome,
    freq.type_activ,
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
left join {{ ref("i_t_prog_adultes") }} prog on prog.prog = freq.prog
left join
    {{ ref("i_t_wl_descr_adultes") }} wld
    on wld.code = freq.service_enseign
    and wld.nom_table = 'X_ServiceEnseign'
left join {{ ref("i_t_wl_descr_adultes") }} mot on mot.code = freq.motif_depart 
    and mot.nom_table = 'X_MotifDep'
left join
    {{ ref("i_t_wl_descr_adultes") }} wl
    on wl.code = freq.raison_depart
    and wl.nom_table = 'X_RaisonDepart'
left join
    {{ ref("i_t_wl_descr_adultes") }} wlt
    on wlt.code = freq.type_parcours
    and wlt.nom_table = 'X_TypeParcours'
left join
    {{ ref
("i_t_wl_descr_adultes") }} wlcon
    on wlcon.code = freq.CondAdmiss
    and wlcon.nom_table = 'X_CondAdmiss'
left join
    {{ ref("i_e_o_orghor_adultes") }} org
    on org.eco_cen = freq.eco_cen
    and org.org_hor = freq.org_hor
where pop.annee >=2020