{#
CDPVD Dashboards store
Copyright (C) 2024 CDPVD.

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU Affero General Public License as
published by the Free Software Foundation, either version 3 of the
License, or any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
GNU Affero General Public License for more details.

You should have received a copy of the GNU Affero General Public License
along with this program. If not, see <https://www.gnu.org/licenses/>.
#}

WITH CTE AS
(
	SELECT 
		MATR,
		CORP_EMPL,
		LIEU_TRAV,
		DATE_ENTR,
		DATE_DERN_JR_TRAV,
		ETAT,
		-- Récupère la dernière DATE_EFF existante avant la ligne courante
		-- Permet de détecter les chevauchements de périodes d'emploi
		MAX(DATE_EFF) OVER
		(
			PARTITION BY MATR
			ORDER BY DATE_ENTR
			ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
		) AS DateEffPrecedente

	FROM {{ ref("i_pai_dos_empl") }}
),
CTE2 AS
(
	SELECT 
		*,

		-- Conserve uniquement les dates d'entrée qui ne chevauchent
		-- aucune période d'emploi précédente
		CASE
			WHEN DateEffPrecedente IS NULL
				 OR DateEffPrecedente <= DATE_ENTR
			THEN DATE_ENTR

			ELSE NULL
		END AS NouvelleDateEntr

	FROM CTE
)

SELECT 
	-- Type de RRM (valeur fixe pour cette extraction)
	0 AS TYPE_RRM,
	MATR,
	CORP_EMPL,
	LIEU_TRAV,

	-- Date de début du RRM
	DATE_ENTR AS DATE_RRM,
		-- Catégorie de période depuis la date d'entrée
	CASE
		WHEN DATE_ENTR >= DATEADD(MONTH, -3, GETDATE())
		THEN 0

		WHEN DATE_ENTR >= DATEADD(MONTH, -5, GETDATE())
			 AND DATE_ENTR < DATEADD(MONTH, -3, GETDATE())
		THEN 1

		WHEN DATE_ENTR >= DATEADD(YEAR, -1, GETDATE())
			 AND DATE_ENTR < DATEADD(MONTH, -5, GETDATE())
		THEN 2

		WHEN DATE_ENTR >= DATEADD(YEAR, -2, GETDATE())
			 AND DATE_ENTR < DATEADD(YEAR, -1, GETDATE())
		THEN 3

		WHEN DATE_ENTR >= DATEADD(YEAR, -3, GETDATE())
			 AND DATE_ENTR < DATEADD(YEAR, -2, GETDATE())
		THEN 4

		WHEN DATE_ENTR >= DATEADD(YEAR, -4, GETDATE())
			 AND DATE_ENTR < DATEADD(YEAR, -3, GETDATE())
		THEN 5

		WHEN DATE_ENTR >= DATEADD(YEAR, -5, GETDATE())
			 AND DATE_ENTR < DATEADD(YEAR, -4, GETDATE())
		THEN 6

		WHEN DATE_ENTR < DATEADD(YEAR, -5, GETDATE())
		THEN 7
	END AS PERIODE,
	-- Année de la date d'entrée
	YEAR(DATE_ENTR) AS ANNEE,

	-- Mois à partir du mois de juillet
	((MONTH(DATE_ENTR) + 5) % 12) + 1 AS MOIS,

	-- Semaine à partir du mois de juillet
	DATEDIFF(
		WEEK, 
		DATEFROMPARTS(
			YEAR(DATE_ENTR) - CASE WHEN MONTH(DATE_ENTR) < 7 THEN 1 ELSE 0 END, 7, 1
		), DATE_ENTR) + 1 AS SEMAINE





FROM CTE2
-- Élimine les emplois qui chevauchent une période existante
WHERE NouvelleDateEntr IS NOT NULL
