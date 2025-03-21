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
      "{{ this }}", ["code_perm","fiche", "annee", "freq", "population", "mat", "grp"]),
    create_nonclustered_index(
      "{{ this }}", ["noseqmat"])
      ])
}}

select
    pop.code_perm,
    pop.population,
    matfpfga.fiche,
    matfpfga.annee,
    matfpfga.freq,
    matfpfga.mat,
    matfpfga.grp,
    -- fac.eco_cen,
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
    -- fac.client,
    -- fac.population,
    -- fac.interv_age,
    -- fac.interv_age_fp,
    -- fac.org_hor,
    -- fac.descr_org_hor,
    -- fac.activform,
    -- fac.condadmiss,
    -- fac.descr_condadmiss,
    -- fac.etat_formation,
    -- fac.prog,
    -- fac.descr_prog,
    -- fac.type_diplome,
    -- fac.raison_grat_scol,
    -- fac.descr_raison_grat_scol,
    -- fac.type_parcours,
    -- fac.descr_type_parcours,
    -- fac.service_enseign,
    -- fac.descr_service_enseign,
    -- fac.motif_depart,
    -- fac.descr_motif_dep,
    -- fac.raison_depart,
    -- fac.desc_raison_depart,
    matd.descrmat
from {{ ref("i_e_elematfpfga_adultes") }} as matfpfga
inner join
    {{ ref("stg_populations_adultes") }} as pop
    on pop.fiche = matfpfga.fiche
    and pop.annee = matfpfga.annee
    and pop.freq = matfpfga.freq
inner join
    {{ ref("i_e_matele_adultes") }} as matele on matfpfga.noseqmat = matele.noseqmat
inner join {{ ref("i_t_mat_adultes") }} as matd on matd.mat = matfpfga.mat
where matfpfga.annee >= 2020
