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
with
    cte as (
        select
            fac.code_perm,
            fac.fiche,
            fac.annee,
            concat(fac.annee, '-', fac.annee + 1) as annnee_scolaire,
            fac.freq,
            fac.nom_centre,
            facr.mat,
            facr.grp,
            facr.no_seq_mat,
            facr.ind_mat_etei,
            facr.grh,
            facr.disc,
            facr.ord_chrono,
            facr.nb_hres_prev,
            facr.nb_min_rea,
            facr.date_fin,
            facr.statut_profil,
            facr.res_ens,
            facr.date_deb,
            facr.annee_sanct,
            facr.mois_sanct,
            facr.jour_sanct,
            case
                when facr.res = '' or facr.res is null then null
                else
                    case
                        when
                            facr.annee_sanct = ''
                            or facr.annee_sanct is null
                            or facr.mois_sanct = ''
                            or facr.mois_sanct is null
                            or facr.jour_sanct = ''
                            or facr.jour_sanct is null
                        then null
                        else
                            cast(
                                concat(
                                    facr.annee_sanct,
                                    '-',
                                    right(concat('0', facr.mois_sanct), 2),
                                    '-',
                                    right(concat('0', facr.jour_sanct), 2)
                                ) as date
                            )
                    end
            end as date_sanct,
            facr.ind_transm,
            facr.service,
            case
                when facr.res = '' then null
                when try_cast(facr.res as int) >= 60 or facr.res = 'SU' or facr.res = 'CT' then 'SU'
                when try_cast(facr.res as int) < 60 then 'EC'
                else facr.res
            end as sanc,
            facr.res,
            facr.nb_hres_stage,
            concat(mat, ' - ', facr.descr_mat) as descr_mat,
            fac.eco_cen,
            fac.bat,
            fac.client,
            fac.population,
            fac.interv_age,
            fac.interv_age_fp,
            fac.org_hor,
            fac.descr_org_hor,
            fac.activ_form,
            fac.cond_admiss,
            concat(fac.cond_admiss, ' - ', fac.descr_condadmiss) as condition_admission,
            fac.descr_condadmiss,
            fac.etat_formation,
            fac.prog,
            case
                when fac.descr_prog is null
                then fac.descr_prog
                else concat(fac.prog, ' - ', fac.descr_prog)
            end as programme,
            case
                when fac.activ_form is null or fac.activ_form = ''
                then fac.donpers
                else fac.activ_form
            end as groupe_horaire,
            fac.descr_prog,
            fac.type_diplome,
            fac.raison_grat_scol,
            fac.descr_raison_grat_scol,
            fac.type_parcours,
            concat(fac.type_parcours, ' - ', fac.descr_type_parcours) as desc_type_parcours,
            fac.service_enseign,
            case
                when fac.descr_service_enseign is null
                then fac.descr_service_enseign
                else concat(fac.service_enseign, ' - ', fac.descr_service_enseign)
            end as service_enseignement,
            fac.descr_service_enseign,
            fac.motif_depart,
            fac.descr_motif_dep,
            fac.raison_depart,
            fac.desc_raison_depart,
            el.genre,
            concat('(', facr.fiche, ') ', el.prenom, ' ', el.nom) as prenom_nom,
            el.lang_matern,
            el.desc_lang_matern,
            facr.type_profil,
            case when facr.occurrence = 1 then 'Non' else 'Oui' end as "En reprise",
            facr.occurrence,
            facr.nb_mins_l,
            facr.nb_mins_o,
            facr.nb_mins_r,
            facr.typ_mat
        from {{ ref("fact_sanction_adultes") }} facr
        inner join {{ ref("fact_freq_adultes") }} as fac
            on fac.code_perm = facr.code_perm
            and fac.fiche = facr.fiche
            and fac.annee = facr.annee
            and fac.freq = facr.freq
        inner join {{ ref("dim_eleve_adultes") }} as el 
            on el.code_perm = facr.code_perm
    )

select *
from cte
