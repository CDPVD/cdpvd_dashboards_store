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
		[dure] * emp.pourc_sal as [duree], -- Durée en fonction de la tâche de travail
		absence.lieu_trav as lieu_trav, -- Lieu de travail
		emp.pourc_sal, -- Pourcentage du salaire
		emp.gr_paie, -- Groupe de paie
		absence.corp_empl,
		dure
	from {{ ref("i_pai_habs") }} as absence

	inner join {{ ref("i_paie_hemp") }} as emp -- Historique des emplois
		on absence.matr = emp.matr
			and absence.corp_empl = emp.corp_empl
			and absence.date between emp.date_eff and emp.date_fin
			and absence.sect = emp.sect

	left join {{ ref("type_absence") }} as type_abs  -- À modifier
		on absence.mot_abs = type_abs.motif_id      

	where year(absence.date) >= {{ get_current_year() - 4 }} -- Retour 5 ans en arrière
		and dure != 0 -- Durée non égale à 0
		and emp.pourc_sal != 0 -- Pour_sal non égale à 0
		and type_abs.statut = 0
),

----------------------------------------------------------------------------------------------------
-- ÉTAPE 1
----------------------------------------------------------------------------------------------------

e1 as (
	select distinct -- ** Vérifier si important
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
),

----------------------------------------------------------------------------------------------------
-- ÉTAPE 2
----------------------------------------------------------------------------------------------------

e2 as ( 
	select 
		annee,
		matricule,
		corp_empl,
		e1.gr_paie,
		lieu_trav,
		pourc_sal,
		categories,
		dure,
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
	from e1

	INNER JOIN {{ ref("i_pai_tab_cal_jour") }} AS cal 
	ON cal.date_jour = date

	group by
	annee,
	matricule,
	corp_empl,
	e1.gr_paie,
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
		COUNT(*) as events,
		SUM(dure) AS total_dure,
		CAST(((pourc_sal * SUM(dure)) * nbr_jours) AS FLOAT) / 100.0 AS duree_abs
	FROM e2

	inner join {{ ref('heures_travaillees') }} as ht
		ON ht.cat_emploi = left(e2.corp_empl,1)

	GROUP BY 
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
	ht.heures
)

----------------------------------------------------------------------------------------------------
-- ÉTAPE 4 
----------------------------------------------------------------------------------------------------

select 
	e3.annee,
	matricule,
	corp_empl,
	lieu_trav,
	jr_tr.jour_trav,
	categories,
	AVG(total_dure) as absence_duree,
	SUM(nbr_jours) as absence_jours,
	SUM(lundi) as lundi_total, 
	SUM(mardi) as mardi_total, 
	SUM(mercredi) as mercredi_total, 
	SUM(jeudi) as jeudi_total, 
	SUM(vendredi) as vendredi_total,
	(SUM(pourc_sal * total_dure * nbr_jours) / jour_trav) / 100 AS taux,
	SUM(duree_abs) AS absence_jour_duree, 
	SUM(duree_abs) / SUM(events) as moyenne,
	SUM(events) AS evenements,
	SUM(pourc_sal * total_dure * nbr_jours) AS abs
from e3

INNER JOIN {{ ref("fact_nbr_jours_travailles") }} AS jr_tr 
	ON e3.annee = jr_tr.an_budg AND e3.gr_paie = jr_tr.gr_paie

group by
e3.annee,
e3.matricule,
e3.corp_empl,
e3.lieu_trav,
e3.categories,
jr_tr.jour_trav