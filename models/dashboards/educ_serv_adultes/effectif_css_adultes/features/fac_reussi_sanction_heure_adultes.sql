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

with cte as
  (
    SELECT fac.code_perm
      , fac.fiche
      , fac.annee
      , concat(fac.annee, '-', fac.annee + 1)as annnee_scolaire
      , fac.freq
      , fac.nom_centre
      , mat
      , grp
      , noseqmat
      , indmatetei
      , grh
      , disc
      , ordchrono
      , nbhresprev
      , nbminrea
      , facr.date_fin
      , statutprofil
      , resens
      , facr.date_deb
      , annee_sanct
      , mois_sanct
      , jour_sanct
      , case 
        when res = '' or res is null 
        then null 
        else CAST(CONCAT(annee_sanct, '-', 
                RIGHT(CONCAT('0', mois_sanct), 2), '-', 
                RIGHT(CONCAT('0', jour_sanct), 2)) AS DATE)
        end as date_sanct
      , indtransm
      , service
      , case
        when res = '' then null
        when TRY_CAST(res AS INT) >= 60 or res = 'SU' then 'SU'
        when TRY_CAST(res AS INT) < 60 then 'EC'
        else res
       END as sanc
      , res
      , nbhresstage
      , descrmat
      ,fac.eco_cen
      ,fac.bat
      ,fac.client,
      fac.population,
      fac.interv_age,
      fac.interv_age_fp,
      fac.org_hor,
      fac.descr_org_hor,
      fac.activform,
      fac.condadmiss,
      concat (fac.condadmiss, ' - ', fac.descr_condadmiss) as condition_admission,
      fac.descr_condadmiss,
      fac.etat_formation,
      fac.prog,
      case
        when descr_prog is null then descr_prog
        else concat (prog, ' - ', descr_prog )
      end as Programme,
      fac.descr_prog,
      fac.type_diplome,
      fac.raison_grat_scol,
      fac.descr_raison_grat_scol,
      fac.type_parcours,
      concat (type_parcours, ' - ', descr_type_parcours) as desc_type_parcours,
      fac.service_enseign,
      case
        when descr_service_enseign is null then descr_service_enseign
        else concat (service_enseign, ' - ', descr_service_enseign )
      end as service_enseignement,
      fac.descr_service_enseign,
      fac.motif_depart,
      fac.descr_motif_dep,
      fac.raison_depart,
      fac.desc_raison_depart,
      el.genre,
      concat('(',facr.fiche,') ',el.prenom, ' ', el.nom) as prenom_nom,
      el.lang_matern,
      el.desc_lang_matern
    FROM {{ ref("fact_reussite_adultes") }} facr
    inner join {{ ref("fact_freq_adultes") }} as fac
      on fac.code_perm = facr.code_perm
      and fac.fiche = facr.fiche
      and fac.annee = facr.annee
      and fac.freq = facr.freq
    inner join {{ ref("dim_eleve_adultes") }} as el on el.code_perm = facr.code_perm
    -- where res !=''
  )
select *
from cte