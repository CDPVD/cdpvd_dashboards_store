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
{{ config(post_hook=[create_clustered_index("{{ this }}", ["code_perm"])]) }}

select
    pop.code_perm,
    max(el.nom) as nom,
    max(el.prenom) as prenom,
    concat(
        max(el.nom), ', ', max(el.prenom), ' (', pop.code_perm, ' )'
    ) as nom_prenom_code_perm,
    max(el.date_naissance) as date_naissance,
    max(el.ind_lieu_naissance_n) as ind_lieu_naissance_n,
    max(el.lang_matern) as lang_matern,
    max(wlt.cf_descr) as desc_lang_matern,
    max(adr.ville) as ville,
    max(adr.code_post) as code_post,
    max(
        case
            when el.genre = 'F'
            then 'Fille'
            when el.genre = 'M'
            then 'Garçon'
            else el.genre
        end
    ) as genre
from {{ ref("stg_populations_adultes") }} as pop
inner join {{ ref("i_e_ele_adultes") }} as el on pop.code_perm = el.code_perm
left join
    {{ ref("i_e_adr_adultes") }} as adr
    on pop.fiche = adr.fiche
    and adr.envoimeq = '1'
    and adr.date_fin = ''
left join
    {{ ref("i_t_wl_descr_adultes") }} wlt
    on wlt.code = el.lang_matern
    and wlt.nom_table = 'X_Lang'
group by pop.code_perm