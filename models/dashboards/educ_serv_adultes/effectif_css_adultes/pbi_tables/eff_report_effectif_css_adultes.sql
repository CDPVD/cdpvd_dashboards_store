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
{{ config(post_hook=[stamp_model("dashboard_effectif_css_adultes")]) }}

with
  agg as (
    select
      client,
      population,
      annee,
      freq,
      age_30_juin,
      age_30_septembre,
      org,
      eco_cen,
      bat,
      org_hor,
      descr_org_hor,
      case
      when date_fin_sifca is null or date_fin_sifca = '' then 'en cours'
        else 'terminé'
      END as etat_formation,
      genre,
      prog,
      prog_meq,
      descr_prog,
      type_diplome,
      type_activ,
      descr_type_parcours,
      descr_service_enseign,
      motif_depart,
      descr_motif_dep,
      raison_depart,
      desc_raison_depart,
      count(code_perm) as total_ele
  from {{ ref("fact_freq_adultes") }}
  group by
    client,
    population,
    annee,
    freq,
    age_30_juin,
    age_30_septembre,
    org,
    eco_cen,
    bat,
    org_hor,
    descr_org_hor,
    case
    when date_fin_sifca is null or date_fin_sifca = '' then 'en cours'
      else 'terminé'
    END,
    genre,
    prog,
    prog_meq,
    descr_prog,
    type_diplome,
    type_activ,
    descr_type_parcours,
    descr_service_enseign,
    motif_depart,
    descr_motif_dep,
    raison_depart,
    desc_raison_depart
  )
select
  centre.descr as nom_centre,
  client,
  population,
  annee,
  freq,
  age_30_juin,
  age_30_septembre,
  org,
  agg.eco_cen,
  bat,
  org_hor,
  descr_org_hor,
  etat_formation,
  genre,
  prog,
  prog_meq,
  descr_prog,
  type_diplome,
  type_activ,
  descr_type_parcours,
  descr_service_enseign,
  motif_depart,
  descr_motif_dep,
  raison_depart,
  desc_raison_depart,
  total_ele
from agg
inner join
  {{ ref("i_t_ecocen_adultes") }} as centre
  on agg.eco_cen = centre.eco_cen