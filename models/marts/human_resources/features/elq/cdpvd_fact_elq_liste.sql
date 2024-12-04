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

select
    util.matr,                          -- Matricule
    emp.etat as etat_empl,              -- Code de létat demploi
    emp.lieu_trav as workplace,         -- Code du lieu de travail
    emp.stat_eng,                       -- Code du statut dengagement
    emp.corp_empl,                      -- Corp demploi
    MIN(qa.type_qualif) AS type_qualif  -- Qualification / Certification
        
from {{ ref("dim_employees") }} as util
inner join {{ ref("i_pai_dos_empl") }} as emp on util.matr = emp.matr
inner join {{ ref("etat_empl") }}  as etat on emp.etat = etat.etat_empl
inner join {{ ref("fact_activity_current") }} as ca on util.matr = ca.matr -- Employé actif ds paie

-- LEFT JOIN requis pour assurer une bonne représentation de la population
left join {{ ref("i_pai_qualif") }} as qa on util.matr = qa.matr

where
    etat.etat_actif = 1                 -- Si l'employé est actif
    and emp.ind_empl_princ = 1          -- Prendre en considération uniquement sont emploi principal
    and emp.corp_empl like '3%'         -- Enseignant(e)

group by util.matr, emp.etat, emp.lieu_trav, emp.stat_eng, emp.corp_empl, qa.type_qualif    