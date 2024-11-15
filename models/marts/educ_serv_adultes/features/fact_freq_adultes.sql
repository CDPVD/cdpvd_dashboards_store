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
  Map each school to it's friendly name.

  Feel free to override me to get your own custom litle mapping.
#}
{{
    config(
        post_hook=[
            create_clustered_index(
                "{{ this }}", ["fiche", "annee", "freq"]
            )
        ]
    )
}}

SELECT 
    pop.code_perm
    , el.fiche
    , freq.client
    , pop.population
    , pop.annee
    , pop.freq
    , floor(datediff(day, convert(date, el.date_naissance, 112), cast(concat(pop.annee, '0630') as date)) / 365.25) AS age_30_juin      -- format 112 = yyyymmdd
    , floor(datediff(day, convert(date, el.date_naissance, 112), cast(concat(pop.annee, '0930') as date)) / 365.25) as age_30_septembre -- format 112 = yyyymmdd
    , freq.org
    , freq.eco_cen
    , freq.bat
    , freq.org_hor
    , org.descr as descr_org_hor
    , freq.date_deb
    , freq.date_fin_sifca
    , freq.prog
    , prog.prog_meq
    , prog.descr_prog
    , prog.type_diplome
    , freq.type_activ
    , freq.service_enseign
    , wld.cf_descr as descr_service_enseign
    , freq.motif_depart
    , mot.descr as descr_motif_dep
    , freq.raison_depart
    , wl.cf_descr desc_raison_depart
FROM {{ ref("stg_populations_adultes") }} AS pop
LEFT JOIN {{ ref("dim_eleve_adultes") }} AS el 
    ON el.code_perm = pop.code_perm AND el.fiche = pop.fiche
LEFT JOIN {{ ref("i_e_freq_adultes") }} AS freq
    ON freq.fiche = el.fiche AND freq.annee = pop.annee AND freq.freq = pop.freq
LEFT JOIN {{ ref("i_t_prog_adultes") }} prog
    ON prog.prog = freq.prog 
LEFT JOIN {{ ref("i_t_wl_descr_adultes") }} wld
    ON wld.code = freq.service_enseign AND wld.nom_table = 'X_ServiceEnseign'
LEFT JOIN {{ ref("i_t_motif_adultes") }} mot
    ON mot.motif = freq.motif_depart 
LEFT JOIN {{ ref("i_t_wl_descr_adultes") }} wl
    ON wl.code = freq.raison_depart AND wl.nom_table = 'X_RaisonDepart'
LEFT JOIN {{ ref("i_e_o_orghor") }} org
    ON org.eco_cen = freq.eco_cen AND org.org_hor = freq.org_hor