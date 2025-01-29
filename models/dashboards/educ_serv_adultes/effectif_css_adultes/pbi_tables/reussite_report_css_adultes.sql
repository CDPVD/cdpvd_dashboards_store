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
{{ config(post_hook=[core_dashboards_store.stamp_model("dashboard_effectif_css_adultes")]) }}

with
  agg as (
    select
      fac.code_perm,
      matfpfga.fiche,
      matfpfga.annee,
      matfpfga.freq,
      matfpfga.mat,
      matfpfga.grp,
      fac.eco_cen,
      matfpfga.noseqmat,
      matfpfga.indmatetei,
      matfpfga.grh,
      matfpfga.disc,
      matfpfga.ordchrono,
      matfpfga.nbhresprev,
      matfpfga.nbminrea,
      matfpfga.date_fin,
      matfpfga.statutprofil,
      matfpfga.resens,
      matele.date_deb,
      matele.annee_sanct,
      matele.mois_sanct,
      matele.jour_sanct,
      matele.indtransm,
      matele.service,
      matele.res,
      matele.nbhresstage,
      fac.client,
      fac.population,
      fac.interv_age,
      fac.interv_age_fp,
      fac.org_hor,
      fac.descr_org_hor,
      fac.activform,
      fac.condadmiss,
      fac.descr_condadmiss,
      fac.etat_formation,
      fac.prog,
      fac.descr_prog,
      fac.type_diplome,
      fac.raison_grat_scol,
      fac.descr_raison_grat_scol,
      fac.type_parcours,
      fac.descr_type_parcours,
      fac.service_enseign,
      fac.descr_service_enseign,
      fac.motif_depart,
      fac.descr_motif_dep,
      fac.raison_depart,
      fac.desc_raison_depart,
      matd.descrmat
  from {{ ref("fact_freq_adultes") }} as fac
  inner join {{ ref("dim_eleve_adultes") }} as el on el.code_perm = fac.code_perm
  )
select
  [Nom du centre],
  client,
  prenom_nom,
  code_perm,
  fiche,
  population,
  annee as Année,
  concat(annee, '-', annee + 1)as [Année scolaire],
  freq,
  age_30_juin,
  age_30_septembre,
  org,
  eco_cen,
  bat,
  org_hor,
  interv_age,
  interv_age_fp,
  ActivForm [Groupe horaire],
  CondAdmiss,
  concat (CondAdmiss, ' - ', descr_condadmiss) as [Condition d'admission],
  descr_org_hor as [Organisation horaire],
  etat_formation as [Etat de la Formation],
  genre as Genre,
  lang_matern as lang_matern,
  desc_lang_matern as [Langue maternelle],
  prog,
  case
    when descr_prog is null then descr_prog
    else concat (prog, ' - ', descr_prog )
  end as Programme,
  type_diplome as [Type de diplôme],
  raison_grat_scol,
  descr_raison_grat_scol as [Raison de gratuité scolaire],
  type_parcours,
  concat (type_parcours, ' - ', descr_type_parcours) as [Type de parcours],
  service_enseign,
  case
    when descr_service_enseign is null then descr_service_enseign
    else concat (service_enseign, ' - ', descr_service_enseign )
  end as [Service enseignement],
  motif_depart,
  descr_motif_dep as [Motif de départ],
  raison_depart,
  desc_raison_depart as [Raison de départ]
from agg