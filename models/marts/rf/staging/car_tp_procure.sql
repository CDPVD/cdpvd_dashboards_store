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

with
-- Gerer le format de la fiche
    el_cast as (
        select
            code_perm,
            case
                when charindex('_', fiche) > 0 then right('0000000' + left(fiche, charindex('_', fiche) - 1), 7)
                else right('0000000' + cast(fiche as varchar(7)), 7)
            end as fiche
        from {{ ref("i_e_ele_adultes") }}

-- CAR PROCURE
), car as (
    select
        case
            when charindex('_', code_emprunt) > 0 
                then right('0000000' + left(code_emprunt, charindex('_', code_emprunt) - 1), 7)
            else right('0000000' + cast(code_emprunt as varchar(7)), 7)
        end as fiche_key,
        eco_cen,
        annee,
        solde 
    from {{ ref("i_pro_art_emprunt") }}
    where statut != 15

-- aggreger CAR par fiche, annee, eco_cen
), car_agg as (
    select 
        fiche_key as fiche,
        eco_cen,
        annee,
        sum(solde) as solde
    from car
    group by 
        fiche_key,
        eco_cen,
        annee

-- TP PROCURE
), tp as (
    select
        case
            when charindex('_', code_emprunt) > 0 
                then right('0000000' + left(code_emprunt, charindex('_', code_emprunt) - 1), 7)
            else right('0000000' + cast(code_emprunt as varchar(7)), 7)
        end as fiche_key,
        case
            when month(date_paiemnt) < 7 then year(date_paiemnt) - 1
            else year(date_paiemnt)
        end as annee,
        eco_cen,
        mont_non_repart
    from {{ ref("i_pro_paiemnt") }}
    where type_emprunt = '1'
      and date_annul is null
      and type_paiemnt = '4'

-- aggreger TP par fiche, annee, eco_cen
), tp_agg as (
    select
        fiche_key as fiche,
        eco_cen,
        annee,
        sum(mont_non_repart) as mont_non_repart
    from tp
    group by 
        tp.fiche_key,
        tp.eco_cen,
        tp.annee

-- CAR-TP
), car_tp as (

    select 
        coalesce(c.fiche, t.fiche) as fiche,
        coalesce(c.annee, t.annee) as annee,
        coalesce(c.eco_cen, t.eco_cen) as eco_cen,
        isnull(c.solde, 0) as car_proc,
        isnull(t.mont_non_repart, 0) as trp_proc
    from car_agg as c
    full join tp_agg as t 
        on c.fiche = t.fiche 
        and c.annee = t.annee 
        and c.eco_cen = t.eco_cen
)

-- REQUETE FINALE
select
    el.code_perm,
    ct.fiche,
    ct.annee,
    ct.eco_cen as eco,
    ct.car_proc,
    ct.trp_proc
from car_tp as ct
inner join el_cast as el
    on el.fiche = ct.fiche
