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


select 
	concat(LEFT(abs.annee,4),'-',LEFT(abs.annee,4) + 1) as annee,
	abs.matricule,
	emp.first_name + ' ' + emp.last_name as nom,
	emp.sex_friendly_name AS genre,
	CASE
		WHEN DATEDIFF(year, emp.birth_date, CAST(LEFT(annee, 4) + '-07-01' AS DATE)) < 25 
			THEN '24 ans et moins'
		WHEN DATEDIFF(year, emp.birth_date, CAST(LEFT(annee, 4) + '-07-01' AS DATE)) BETWEEN 25 AND 34 
			THEN '25 à 34 ans'
		WHEN DATEDIFF(year, emp.birth_date, CAST(LEFT(annee, 4) + '-07-01' AS DATE)) BETWEEN 35 AND 44 
			THEN '35 à 44 ans'
		WHEN DATEDIFF(year, emp.birth_date, CAST(LEFT(annee, 4) + '-07-01' AS DATE)) BETWEEN 45 AND 54 
			THEN '45 à 54 ans'
		WHEN DATEDIFF(year, emp.birth_date, CAST(LEFT(annee, 4) + '-07-01' AS DATE)) BETWEEN 55 AND 64 
			THEN '55 à 64 ans'
		ELSE '65 ans et plus'
	END AS tranche_age,    
	DATEDIFF(year, emp.birth_date, CAST(LEFT(annee, 4) + '-07-01' AS DATE)) AS age,
	jc.code_job_name AS corp_empl,
	wp.workplace_name as lieu_trav,
	sec.secteur_Descr as secteur,
	jg.job_group_category as cat_emp,
	abs.categories,
	jour_trav,
	duree_abs,
	taux,
	lundi, 
	mardi, 
	mercredi, 
	jeudi, 
	vendredi,
	evenements,
	abs
from {{ ref("fact_absence") }} as abs

INNER JOIN {{ ref("dim_employees") }} AS emp 
ON abs.matricule = emp.matr

inner join {{ ref("secteur") }} as sec 
on abs.lieu_trav = sec.ua_id

inner join {{ ref("dim_mapper_job_group") }} as jg 
on abs.corp_empl = jg.job_group

inner join {{ ref("dim_mapper_job_class") }} as jc 
on abs.corp_empl = jc.code_job

inner join {{ ref("dim_mapper_workplace") }} as wp 
on abs.lieu_trav = wp.workplace