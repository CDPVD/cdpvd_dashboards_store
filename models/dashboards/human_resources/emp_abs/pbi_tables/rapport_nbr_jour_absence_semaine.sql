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
{{ config(alias="emp_abs_report_jour_semaine") }}

with age AS (
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
        END AS tranche_age
    FROM {{ref("fact_absence_consecutive")}} AS ac    
    INNER JOIN {{ ref("dim_employees") }} AS emp 
        ON ac.matricule = emp.matr
	group by [annee]
      ,[matricule]
      ,[corp_empl]
      ,[lieu_trav]
      ,[pourc_sal]
      ,[categories]
      , birth_date
)

select
    LEFT(abs.annee,4) as annee,
    abs.matricule,
    abs.corp_empl,
    emp.sex_friendly_name AS genre,
    a.tranche_age,
    abs.lieu_trav,
    sec.secteur_Descr as secteur,
    jg.job_group_category as cat_emp,
    abs.categories,
    lundi, mardi, mercredi, jeudi, vendredi,
    nbr_abs,
    taux,
    duree_abs,
    {{
    dbt_utils.generate_surrogate_key(
        ["abs.annee", "abs.lieu_trav", "abs.corp_empl", "genre", "sec.secteur_Id", "tranche_age", "jg.job_group_category", "abs.categories"])
    }} as filter_key    
from {{ ref("fact_nbr_jour_absence_semaine") }} as abs
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
