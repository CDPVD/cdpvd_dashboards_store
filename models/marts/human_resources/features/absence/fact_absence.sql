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
{{ config(alias="fact_absence") }}

with
----------------------------------------------------------------------------------------------------
-- ÉTAPE 0 
----------------------------------------------------------------------------------------------------
e0 AS (
	select
		case -- Année format des calendriers ** Meilleure approche?
		-- Si mois courant entre janvier et juin inclusivement
		when month(absence. [date]) >= 1 and month(absence. [date]) < 7 
		then concat(year(absence. [date]) - 1, '', year(absence. [date]))
		else concat(year(absence. [date]), '', year(absence. [date]) + 1)
		end as [annee], -- Année format des calendriers
		absence. [matr] as [matricule], -- Matricule
		absence. [date], -- Date d'absence
		absence.mot_abs as motif_abs, -- Motif d'absence
		absence.lieu_trav as lieu_trav, -- Lieu de travail
		emp.pourc_sal, -- Pourcentage du salaire
		emp.gr_paie, -- Groupe de paie
		absence.corp_empl,
		emp.stat_eng, -- Statut d'engagement
		dure,
	CASE 
			WHEN absence.code_pmnt = 103525 or etat.duree = 1 THEN 1 
			ELSE 0 
	END AS dl,
	CASE 
			WHEN absence.code_pmnt != 103525 AND etat.duree != 1 THEN 1 
			ELSE 0 
	END AS cl
		from {{ ref("i_pai_habs") }} as absence

		inner join {{ ref("i_paie_hemp") }} as emp -- Historique des emplois
			on absence.matr = emp.matr
				and absence.corp_empl = emp.corp_empl
				and absence.date between emp.date_eff and emp.date_fin
				and absence.sect = emp.sect
				and absence.ref_empl = emp.ref_empl
		inner join {{ ref("etat_empl")}} as etat 
			on emp.etat = etat.etat_empl
),

----------------------------------------------------------------------------------------------------
-- ÉTAPE 1
----------------------------------------------------------------------------------------------------

e1 as(
	select 
		e0.*, -- Tous les champs de fact_liste_absence
		jour_sem, -- Jour de la semaine (0,1,2,3,4,5,6)
		ta.categories
	from e0

	inner join {{ ref("i_pai_tab_cal_jour") }} as cal
	on e0.annee = cal.an_budg
		and e0.gr_paie = cal.gr_paie
		and e0.date = cal.date_jour

	inner join {{ ref("type_absence") }} as ta  -- À modifier
		on e0.motif_abs = ta.motif_id 

	where ta.Statut = 0
),

----------------------------------------------------------------------------------------------------
-- ÉTAPE 1.1
----------------------------------------------------------------------------------------------------

e1_1 as (
	SELECT 
		*
	FROM e1
	UNPIVOT (
		valeur FOR type_duree IN (dl, cl)
	) AS unpvt
	where valeur != 0
),

----------------------------------------------------------------------------------------------------
-- ÉTAPE 2
----------------------------------------------------------------------------------------------------

e2 as ( 
	select 
		annee,
		matricule,
		corp_empl,
		e1_1.gr_paie,
		lieu_trav,
		pourc_sal,
		categories,
		dure,
		stat_eng,
		type_duree,
		SUM(
			CASE WHEN cal.jour_sem = 1 
			THEN ((pourc_sal * dure ) /100 ) / 7 
			ELSE 0 
		END) AS lundi,
		SUM(
			CASE WHEN cal.jour_sem = 2 
			THEN ((pourc_sal * dure ) /100 ) / 7 
			ELSE 0 
		END) AS mardi,
		SUM(
			CASE WHEN cal.jour_sem = 3 
			THEN ((pourc_sal * dure ) /100 ) / 7 
			ELSE 0 
		END) AS mercredi,
		SUM(
			CASE WHEN cal.jour_sem = 4 
			THEN ((pourc_sal * dure ) /100 ) / 7 
			ELSE 0 
		END) AS jeudi,
		SUM(
			CASE WHEN cal.jour_sem = 5 
			THEN ((pourc_sal * dure ) /100 ) / 7 
			ELSE 0 
		END) AS vendredi,
		CAST(
			ROUND(
				SUM(
						CASE 
							WHEN cal.jour_sem != 0 AND cal.jour_sem != 6 
							THEN 1.0 / 7 
							ELSE 0 
						END
					), 0
			) AS INT
		) AS nbr_jours		
	from e1_1

	INNER JOIN {{ ref("i_pai_tab_cal_jour") }} AS cal 
	ON cal.date_jour = date

	group by
	annee,
	matricule,
	corp_empl,
	e1_1.gr_paie,
	stat_eng,
	type_duree,
	lieu_trav,
	pourc_sal,
	categories,
	dure
),

----------------------------------------------------------------------------------------------------
-- ÉTAPE 3
----------------------------------------------------------------------------------------------------

e3 as (
	SELECT
		annee,
		matricule,
		corp_empl,
		gr_paie,
		lieu_trav,
		categories,
		nbr_jours,
		lundi,
		mardi,
		mercredi,
		jeudi,
		vendredi,
		pourc_sal,
		type_duree,
		COUNT(*) as events,
		SUM(dure) AS total_dure,
		CAST(((pourc_sal * SUM(dure)) * nbr_jours) AS FLOAT) / 100.0 AS duree_abs
	FROM e2

	GROUP BY 
	annee,
	matricule,
	corp_empl,
	gr_paie,
	lieu_trav,
	categories,
	type_duree,
	nbr_jours,
	lundi,
	mardi,
	mercredi,
	jeudi,
	vendredi,
	pourc_sal
)

----------------------------------------------------------------------------------------------------
-- ÉTAPE 4 
----------------------------------------------------------------------------------------------------

select 
	e3.annee,
	matricule,
	corp_empl,
	lieu_trav,
	jour_trav,
	categories,
	lundi,
	mardi,
	mercredi,
	jeudi,
	vendredi,
	(pourc_sal * total_dure * nbr_jours) / jour_trav / 100 AS taux,
	duree_abs, 
	events AS evenements,
	(pourc_sal * total_dure * nbr_jours) / 100 AS abs,
	CASE 
		WHEN type_duree = 'dl' and duree_abs >= 30 and events = 1 then 1 
		WHEN type_duree = 'dl' and duree_abs < 30 then 0 
		WHEN 
		--categories = 'Maladie' and 
		duree_abs >= 30 and events = 1 then 1
		WHEN type_duree = 'cl' then 0 
	END AS duree_type,
	type_duree	
from e3

INNER JOIN {{ ref("fact_nbr_jours_travailles") }} AS jr_tr 
	ON e3.annee = jr_tr.annee AND e3.gr_paie = jr_tr.gr_paie

group by
e3.annee,
matricule,
corp_empl,
lieu_trav,
type_duree,
categories,
jour_trav,
lundi,
mardi,
mercredi,
jeudi,
vendredi,
duree_abs,
events,
nbr_jours,
total_dure,
pourc_sal