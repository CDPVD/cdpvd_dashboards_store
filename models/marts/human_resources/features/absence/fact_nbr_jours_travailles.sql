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
{{ config(alias="fact_abs_cal") }}

select 
    [an_budg] AS annee, -- Année budgétaire
    [gr_paie], -- Groupe de paie | Pour sélectionner les employés selon leur type d'emploi 
    SUM(
        CASE 
            WHEN an_budg = {{ core_dashboards_store.get_current_year() }}{{ core_dashboards_store.get_current_year() + 1 }} -- Si Année courante
            AND DATE_JOUR <= GETDATE() THEN 1 -- Calculer le nombre de jours en fonction de la date
                ELSE 
            CASE 
                WHEN DATE_JOUR < GETDATE() THEN 1 -- Si la date est antérieure à aujourd'hui
                ELSE 0 -- Sinon, retourner 0
            END-- Si non, calcule le nombre de jours total des années précédentes
        END
    ) AS jour_trav
from {{ ref("i_pai_tab_cal_jour") }}
where
    type_jour != 'C'        -- Type_jour C => Congé | On ne le prend pas en compte
    and type_jour != 'E'    -- Type_jour E => Été | On ne le prend pas en compte
    and jour_sem != 0       -- jour_sem 0 => Dimanche | On ne le prend pas en compte
    and jour_sem != 6       -- jour_sem 6 => Samedi | On ne le prend pas en compte
    and an_budg >= {{ core_dashboards_store.get_current_year() - 5 }}{{ core_dashboards_store.get_current_year() - 4 }} -- Retour 5 ans derrière
group by an_budg, gr_paie