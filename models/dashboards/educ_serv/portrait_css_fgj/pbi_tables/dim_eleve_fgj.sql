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
            core_dashboards_store.create_clustered_index(
                "{{ this }}", ["fiche", "code_perm"]
            ),
        ]
    )
}}

with spine_keys as (select distinct code_perm, fiche from {{ ref("spine") }})

select
    spi.code_perm,
    spi.fiche,
    max(nom) as nom,
    max(pnom) as prenom,
    concat(max(nom), ', ', max(pnom), ' (', spi.fiche, ')') as nom_prenom_fiche,
    max(date_naissance) as date_naissance,
    max(code_lieu_naiss_ele) as code_lieu_naiss_ele,
    max(wltc.cf_descr_abreg) as desc_code_lieu_naiss_ele,
    max(lieu_naiss) as lieu_naiss,
    max(langue_mater) as langue_mater,
    max(langue_maison) as langue_maison,
    max(wlt.cf_descr_abreg) as desc_langue_mater,
    max(code_post) as code_post,
    max(ville) as ville,
    max(tel) as tel,
    max(
        case
            when el.sexe = 'F'
            then 'Fille'
            when el.sexe = 'M'
            then 'Garçon'
            else el.sexe
        end
    ) as genre
from spine_keys as spi
inner join {{ ref("i_gpm_e_ele") }} as el on spi.fiche = el.fiche
left join
    {{ ref("i_wl_descr") }} wlt
    on wlt.code = el.langue_mater
    and wlt.nom_table = 'LANGUE'
left join
    {{ ref("i_wl_descr") }} wltc
    on wltc.code = el.code_lieu_naiss_ele
    and wltc.nom_table = 'CODE_LIEU_NAISS'
left join
    {{ ref("i_gpm_e_adr") }} as adr
    on spi.fiche = adr.fiche and envoi_meq = '1' and nullif(date_fin_adr, '') is null
group by spi.code_perm, spi.fiche
