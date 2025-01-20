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
{{ config(alias="emp_abs_report_absence") }}

WITH age AS (
    SELECT 
        ac.annee,
        ac.matricule,
        ac.categories,
        ac.lieu_trav,
        ac.corp_empl,
        ac.group_id,
        CASE
            WHEN DATEDIFF(year, emp.birth_date, CAST(LEFT(ac.annee, 4) + '-07-01' AS DATE)) < 25 THEN '24 ans et moins'
            WHEN DATEDIFF(year, emp.birth_date, CAST(LEFT(ac.annee, 4) + '-07-01' AS DATE)) BETWEEN 25 AND 34 THEN '25 à 34 ans'
            WHEN DATEDIFF(year, emp.birth_date, CAST(LEFT(ac.annee, 4) + '-07-01' AS DATE)) BETWEEN 35 AND 44 THEN '35 à 44 ans'
            WHEN DATEDIFF(year, emp.birth_date, CAST(LEFT(ac.annee, 4) + '-07-01' AS DATE)) BETWEEN 45 AND 54 THEN '45 à 54 ans'
            WHEN DATEDIFF(year, emp.birth_date, CAST(LEFT(ac.annee, 4) + '-07-01' AS DATE)) BETWEEN 55 AND 64 THEN '55 à 64 ans'
            ELSE '65 ans et plus'
        END AS tranche_age
    FROM {{ref("fact_absence_consecutive")}} AS ac
    INNER JOIN {{ ref("dim_employees") }} AS emp 
        ON ac.matricule = emp.matr
    INNER JOIN {{ ref("i_pai_tab_cal_jour") }} AS cal 
        ON cal.date_jour BETWEEN ac.startdate AND ac.enddate 
    GROUP BY         
        ac.annee,
        ac.matricule,
        ac.ref_empl,
        ac.categories,
        ac.lieu_trav,
        ac.group_id,
        ac.reg_abs,
        ac.gr_paie,
        ac.corp_empl,
        birth_date
)

select 
    LEFT(abs.annee,4) as annee,
    abs.matricule,
    emp.first_name + ' ' + emp.last_name as nom,
    abs.corp_empl,
    emp.sex_friendly_name AS genre,
    jc.descr as lieu_trav,
    sec.secteur_Descr as secteur,
    jg.job_group_category as cat_emp,
    abs.startdate,
    abs.enddate,
    --abs.group_id,
    abs.categories,
    a.tranche_age,
    absence_jours,
    jour_trav,
    absence_jour_duree,
    --absence_jour_duree_normalisee,
    taux,
    --taux_normalise,
    lundi, 
    mardi, 
    mercredi, 
    jeudi, 
    vendredi  
from {{ ref("fact_absence_consecutive") }} as abs
    
    INNER JOIN {{ ref("dim_employees") }} AS emp 
        ON abs.matricule = emp.matr
inner join {{ ref("secteur") }} as sec on abs.lieu_trav = sec.ua_id
inner join {{ ref("dim_mapper_job_group") }} as jg on abs.corp_empl = jg.job_group
inner join {{ ref("dim_mapper_job_class")}} as jc on abs.corp_empl = jc.corp_empl

LEFT JOIN age AS a
    ON 
abs.annee = a.annee and
abs.matricule = a.matricule and
abs.categories = a.categories and
abs.lieu_trav = a.lieu_trav and
abs.corp_empl = a.corp_empl and
abs.group_id = a.group_id