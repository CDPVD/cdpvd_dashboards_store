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
-- CAR PROCURE
car_agg as (
    select 
        el.code_perm,
        case
            when charindex('_', car.code_emprunt) > 0 
                then right('0000000' + left(code_emprunt, charindex('_', code_emprunt) - 1), 7)
            else right('0000000' + cast(car.code_emprunt as varchar(7)), 7)
        end as fiche,
        car.eco_cen,
        car.annee,
        sum(car.solde) as solde
    from {{ ref("i_pro_art_emprunt") }} as car
    left join {{ ref("i_e_ele_adultes") }} as el 
			on el.fiche = car.code_emprunt
    where 
        car.statut != 15
        and el.code_perm is not null
    group by 
        el.code_perm,
        case
            when charindex('_', car.code_emprunt) > 0 
                then right('0000000' + left(car.code_emprunt, charindex('_', car.code_emprunt) - 1), 7)
            else right('0000000' + cast(car.code_emprunt as varchar(7)), 7)
        end,
        car.eco_cen,
        car.annee
),
-- TP PROCURE
tp_agg as (
    select
        el.code_perm,
        case
            when charindex('_', tp.code_emprunt) > 0 
                then right('0000000' + left(tp.code_emprunt, charindex('_', tp.code_emprunt) - 1), 7)
            else right('0000000' + cast(tp.code_emprunt as varchar(7)), 7)
        end as fiche,
        tp.eco_cen,
        case
            when month(tp.date_paiemnt) < 7 then year(tp.date_paiemnt) - 1 
            else year(tp.date_paiemnt)
        end as annee,
        sum(tp.mont_non_repart) as mont_non_repart
    from {{ ref("i_pro_paiemnt") }} as tp
        left join {{ ref("i_e_ele_adultes") }} as el 
			on el.fiche = tp.code_emprunt
    where 
        tp.type_emprunt = '1'
        and tp.date_annul is null
        and tp.type_paiemnt = '4'
        and el.code_perm is not null
    group by 
        el.code_perm,
        case
            when charindex('_', tp.code_emprunt) > 0 
                then right('0000000' + left(tp.code_emprunt, charindex('_', tp.code_emprunt) - 1), 7)
            else right('0000000' + cast(tp.code_emprunt as varchar(7)), 7)
        end,
        tp.eco_cen,
        case
            when month(tp.date_paiemnt) < 7 then year(tp.date_paiemnt) - 1 
            else year(tp.date_paiemnt)
        end
)

-- REQUETE FINALE : joindre CAR et TP par fiche + annee + eco_cen
select
    coalesce(c.code_perm, t.code_perm) as code_perm,
    coalesce(c.fiche, t.fiche) as fiche,
    coalesce(c.annee, t.annee) as annee,
    coalesce(c.eco_cen, t.eco_cen) as eco,
    isnull(c.solde, 0) as car_proc,
    isnull(t.mont_non_repart, 0) as trp_proc
from car_agg as c
full outer join tp_agg as t
    on t.code_perm = c.code_perm
    and t.fiche = c.fiche
    and t.annee = c.annee
    and t.eco_cen = c.eco_cen
