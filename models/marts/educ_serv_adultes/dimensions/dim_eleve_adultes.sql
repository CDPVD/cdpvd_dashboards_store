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
group by pop.code_perm
