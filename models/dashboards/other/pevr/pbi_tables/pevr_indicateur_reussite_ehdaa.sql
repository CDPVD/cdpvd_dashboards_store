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
{{ config(alias="indicateur_reussite_ehdaa") }}

with
    src as (
        select
            case
                when ind.id_indicateur_css is null
                then ind.id_indicateur_cdpvd  -- Permet d'utiliser l'indicateur défaut de la CDPVD
                else ind.id_indicateur_css
            end as id_indicateur,
            ind.description_indicateur,
            pevr_charl.annee_scolaire,
            pevr_charl.cohorte,
            CONCAT(pevr_charl.annee_scolaire, '%', CHAR(10), ' (', pevr_charl.cohorte, ') ') as an_sco_cohorte,
            pevr_charl.taux,
            pevr_charl.cible,
            cast(left(pevr_charl.annee_scolaire, 4) as int) as annee,
            LAG(pevr_charl.taux) OVER (PARTITION BY ind.id_indicateur_cdpvd ORDER BY cast(left(pevr_charl.annee_scolaire, 4) as int)) as taux_previous_year
        from {{ ref("pevr_dim_indicateurs") }} as ind
        inner join
            {{ ref("indicateur_pevr_charl") }} as pevr_charl
            on ind.id_indicateur_cdpvd = pevr_charl.id_indicateur_cdpvd
        where ind.id_indicateur_cdpvd = '3' --  3 - Indicateur du taux des EHDAA.
    ),

_variation as (
    select
        id_indicateur,
        description_indicateur,
        annee_scolaire,
        cohorte,
        an_sco_cohorte,
        taux,
        cible,
        annee,
        CASE 
            WHEN (taux >= cible ) THEN 2 -- Vert
            WHEN ( (taux < taux_previous_year) AND (taux > cible) ) THEN 2 -- Vert
            WHEN ( (taux > taux_previous_year) AND (taux < cible) ) THEN 1 -- Jaune
            WHEN (taux < cible ) THEN 0 -- Rouge
        END AS variation
    from src
)

select 
    id_indicateur,
    description_indicateur,
    annee_scolaire,
    cohorte,
    an_sco_cohorte,
    taux,
    cible,
    annee,
    variation
from _variation