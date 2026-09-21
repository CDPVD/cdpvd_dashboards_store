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
{{ config(tags=["rf", "car_trop_percu"], schema="rf_staging") }}

-- pour chaque code_perm, fiche, indiquer les infos des parents sur une meme ligne
with
    wide as (
        select
            ele.code_perm,
            ele.fiche,
            ele.nom,
            ele.pnom as prenom,
            case when adr.type_contact = '3' then adr.nom end as nom_mere,
            case when adr.type_contact = '3' then adr.pnom end as pnom_mere,
            case when adr.type_contact = '2' then adr.nom end as nom_pere,
            case when adr.type_contact = '2' then adr.pnom end as pnom_pere,
            case when adr.type_contact = '4' then adr.nom end as nom_tuteur,
            case when adr.type_contact = '4' then adr.pnom end as pnom_tuteur,
            adr.type_contact as type_adr,
            ele.date_maj,
            ltrim(
                rtrim(
                    isnull (nullif(adr.app, '') + '-', '')
                    + isnull (nullif(adr.no_civ, '') + ' ', '')
                    + isnull (nullif(adr.orient_rue, '') + ' ', '')
                    + isnull (nullif(adr.genre_rue, '') + ' ', '')
                    + isnull (nullif(adr.rue, '') + ', ', '')
                    + isnull (nullif(adr.ville, '') + ', ', '')
                    + isnull (nullif(adr.code_post, ''), '')
                )
            ) as adresse
        from {{ ref("i_sdg_e_ele") }} ele
        left join {{ ref("i_sdg_e_contact") }} adr on adr.fiche = ele.fiche
        where ele.code_perm is not null
    )

-- semi-agrégation
select
    code_perm,
    fiche,
    nom,
    prenom,
    max(nom_mere) as nom_mere,
    max(pnom_mere) as pnom_mere,
    max(nom_pere) as nom_pere,
    max(pnom_pere) as pnom_pere,
    max(nom_tuteur) as nom_tuteur,
    max(pnom_tuteur) as pnom_tuteur,
    max(adresse) as adresse,
    max(date_maj) as date_maj
from wide
group by code_perm, fiche, nom, prenom
