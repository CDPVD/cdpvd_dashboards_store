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
	{{ informations_date("DATE_ENTR") }}

FROM CTE2
-- Élimine les emplois qui chevauchent une période existante
WHERE NouvelleDateEntr IS NOT NULL
