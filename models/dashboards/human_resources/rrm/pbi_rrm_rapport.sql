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

SELECT 
    TYPE_RRM,
    EMP.legal_name,
    CORP_EMPL,
    LIEU.workplace_name,
    DATE_RRM,
    PERIODE,
    ANNEE,
    MOIS,
    SEMAINE
 FROM {{ ref("fact_rrm_consolide")}} AS RRM
inner join {{ ref("dim_employees") }} as emp on RRM.matr = emp.matr
inner join {{ ref("dim_mapper_workplace") }} lieu on RRM.LIEU_TRAV = lieu.workplace