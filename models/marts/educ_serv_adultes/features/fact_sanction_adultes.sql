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
            create_nonclustered_index("{{ this }}", ["no_seq_mat"]),
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
    matfpfga.no_seq_mat,
    matfpfga.ind_mat_etei,
    matfpfga.grh,
    matfpfga.disc,
    matfpfga.ord_chrono,
    matfpfga.nb_hres_prev,
    matfpfga.nb_min_rea,
    matfpfga.date_fin,
    matfpfga.statut_profil,
    matfpfga.res_ens,
    matele.date_deb,
    matele.annee_sanct,
    matele.mois_sanct,
    matele.jour_sanct,
    matele.ind_transm,
    matele.service,
    matele.res,
    matele.nb_hres_stage,
    matd.descr_mat,
    promat.type_profil,
    promat.occurrence,
    promat.nb_mins_l,
    promat.nb_mins_o,
    promat.nb_mins_r,
    promat.date_sanct,
    promat.typ_mat
from {{ ref("i_e_elematfpfga_adultes") }} as matfpfga
inner join {{ ref("stg_populations_adultes") }} as pop
    on pop.fiche = matfpfga.fiche and pop.annee = matfpfga.annee and pop.freq = matfpfga.freq
inner join {{ ref("i_e_matele_adultes") }} as matele
    on matfpfga.no_seq_mat = matele.no_seq_mat and matfpfga.freq = matele.freq and matfpfga.fiche = matele.fiche
inner join {{ ref("i_t_mat_adultes") }} as matd 
    on matd.mat = matfpfga.mat
inner join {{ ref("i_e_promat_adultes") }} as promat
    on promat.mat = matfpfga.mat and promat.no_seq_mat = matfpfga.no_seq_mat and promat.fiche = matfpfga.fiche and promat.ord_chrono = matfpfga.ord_chrono
where
    matfpfga.annee >= {{ core_dashboards_store.get_current_year() - 5 }}
    and matele.res != ''