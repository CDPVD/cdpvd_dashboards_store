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
                "{{ this }}",
                ["code_perm", "fiche", "annee", "freq", "population", "mat", "grp"],
            ),
            create_nonclustered_index("{{ this }}", ["noseqmat"]),
        ]
    )
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
    matd.descrmat,
    promat.typeprofil,
    promat.ordchrono ord_chrono,
    promat.occurrence,
    promat.noseqmat noseq_mat,
    promat.statutprofil statut_profil,
    promat.nbminsl,
    promat.nbminso,
    promat.nbminsr,
    promat.nbminrea nbmin_rea,
    promat.datesanct,
    promat.res resultat,
    promat.typmat
from {{ ref("i_e_elematfpfga_adultes") }} as matfpfga
inner join
    {{ ref("stg_populations_adultes") }} as pop
    on pop.fiche = matfpfga.fiche
    and pop.annee = matfpfga.annee
    and pop.freq = matfpfga.freq
inner join
    {{ ref("i_e_matele_adultes") }} as matele on matfpfga.noseqmat = matele.noseqmat and matfpfga.freq = matele.freq and matfpfga.fiche = matele.fiche
inner join {{ ref("i_t_mat_adultes") }} as matd on matd.mat = matfpfga.mat
inner join
    {{ ref("i_e_promat_adultes") }} as promat
    on promat.mat = matfpfga.mat
    and promat.noseqmat = matfpfga.noseqmat
    and promat.fiche = matfpfga.fiche
    and promat.ordchrono = matfpfga.ordchrono
where matfpfga.annee >= {{ core_dashboards_store.get_current_year() - 5 }} and matele.res !='' 