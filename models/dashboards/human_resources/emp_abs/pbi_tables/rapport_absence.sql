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
        CASE
            WHEN DATEDIFF(year, emp.birth_date, CAST(LEFT(ac.annee, 4) + '-07-01' AS DATE)) < 25 THEN '24 ans et moins'
            WHEN DATEDIFF(year, emp.birth_date, CAST(LEFT(ac.annee, 4) + '-07-01' AS DATE)) BETWEEN 25 AND 34 THEN '25 à 34 ans'
            WHEN DATEDIFF(year, emp.birth_date, CAST(LEFT(ac.annee, 4) + '-07-01' AS DATE)) BETWEEN 35 AND 44 THEN '35 à 44 ans'
            WHEN DATEDIFF(year, emp.birth_date, CAST(LEFT(ac.annee, 4) + '-07-01' AS DATE)) BETWEEN 45 AND 54 THEN '45 à 54 ans'
            WHEN DATEDIFF(year, emp.birth_date, CAST(LEFT(ac.annee, 4) + '-07-01' AS DATE)) BETWEEN 55 AND 64 THEN '55 à 64 ans'
            ELSE '65 ans et plus'
        END AS tranche_age,
        ROUND(COALESCE(SUM(CAST((pourc_sal * duree) AS FLOAT) / 10000.0) / 7, 0), 4) AS duree_abs, /* MODIFIER*/
        CAST(ROUND(SUM(CASE WHEN cal.jour_sem = 1 THEN 1.0 / 7 ELSE 0 END), 0) AS INT) AS lundi,    
        CAST(ROUND(SUM(CASE WHEN cal.jour_sem = 2 THEN 1.0 / 7 ELSE 0 END), 0) AS INT) AS mardi,    
        CAST(ROUND(SUM(CASE WHEN cal.jour_sem = 3 THEN 1.0 / 7 ELSE 0 END), 0) AS INT) AS mercredi,
        CAST(ROUND(SUM(CASE WHEN cal.jour_sem = 4 THEN 1.0 / 7 ELSE 0 END), 0) AS INT) AS jeudi,    
        CAST(ROUND(SUM(CASE WHEN cal.jour_sem = 5 THEN 1.0 / 7 ELSE 0 END), 0) AS INT) AS vendredi,
        CAST(ROUND(SUM(CASE WHEN cal.jour_sem !=6 AND cal.jour_sem !=0 THEN 1.0 / 7 ELSE 0 END), 0) AS INT) AS nbr_abs
    FROM {{ref("fact_absence_consecutive")}} AS ac
    INNER JOIN {{ ref("dim_employees") }} AS emp 
        ON ac.matricule = emp.matr
    INNER JOIN {{ ref("i_pai_tab_cal_jour") }} AS cal 
        ON cal.date_jour BETWEEN ac.startdate AND ac.enddate        
            WHERE cal.jour_sem NOT IN (6, 0)
    GROUP BY         
        ac.annee,
        ac.matricule,
        ac.ref_empl,
        ac.categories,
        ac.lieu_trav,
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
    abs.lieu_trav,
    sec.secteur_Descr as secteur,
    jg.job_group_category as cat_emp,
    abs.nombre_jours,
    abs.startdate,
    abs.enddate,
    abs.categories,
    a.tranche_age,
    lundi, mardi, mercredi, jeudi, vendredi,nbr_abs,
    {{
        dbt_utils.generate_surrogate_key(
["abs.annee", "abs.lieu_trav", "abs.corp_empl", "genre","sec.secteur_Id", "tranche_age", "jg.job_group_category", "abs.categories"]
        )
    }} as filter_key    
from {{ ref("fact_absence_consecutive") }} as abs
    
    INNER JOIN {{ ref("dim_employees") }} AS emp 
        ON abs.matricule = emp.matr
inner join {{ ref("secteur") }} as sec on abs.lieu_trav = sec.ua_id
inner join {{ ref("dim_mapper_job_group") }} as jg on abs.corp_empl = jg.job_group

LEFT JOIN age AS a
    ON 
abs.annee = a.annee and
abs.matricule = a.matricule and
abs.categories = a.categories and
abs.lieu_trav = a.lieu_trav and
abs.corp_empl = a.corp_empl