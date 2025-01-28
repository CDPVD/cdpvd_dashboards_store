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
      [Nom du centre],
      bat,
      org_hor,
      descr_org_hor,
      etat_formation,
      el.genre,
      concat(el.prenom, ' ', el.nom) as prenom_nom,
      el.lang_matern,
      el.desc_lang_matern,
      ActivForm,
      CondAdmiss,
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