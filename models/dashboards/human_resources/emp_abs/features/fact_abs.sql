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
{{ config(alias="emp_abs_fact_abs") }}

select
    annee,
    al.matricule,
    al.corp_empl,
    ref_empl,
    categories,
    reg_abs,
    lieu_trav,
    birth_date,
    emp.sex_friendly_name as genre,
    startdate,
    enddate,
    duree,
    pourc_sal
from {{ ref("fact_absence_consecutive") }} as al

inner join
    {{ ref("i_pai_tab_cal_jour") }} as cal
    on cal.date_jour between startdate and enddate

inner join {{ ref("dim_employees") }} as emp on al.matricule = emp.matr
where cal.jour_sem not in (6, 0)  -- Exclude weekends    

group by
    annee,
    matricule,
    ref_empl,
    categories,
    lieu_trav,
    startdate,
    enddate,
    al.corp_empl,
    al.gr_paie,
    emp.birth_date,
    emp.sex_friendly_name,
    reg_abs,
    birth_date,
    duree,
    pourc_sal
