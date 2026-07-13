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

SELECT
    1 AS TYPE_RRM,
    MATR,
    CORP_EMPL,
    LIEU_TRAV,
    DEMISSION_DATE AS DATE_RRM,
    CASE
        WHEN DEMISSION_DATE >= DATEADD(MONTH, -3, GETDATE()) THEN 0

        WHEN DEMISSION_DATE >= DATEADD(MONTH, -5, GETDATE())
         AND DEMISSION_DATE < DATEADD(MONTH, -3, GETDATE()) THEN 1

        WHEN DEMISSION_DATE >= DATEADD(YEAR, -1, GETDATE())
         AND DEMISSION_DATE < DATEADD(MONTH, -5, GETDATE()) THEN 2

        WHEN DEMISSION_DATE >= DATEADD(YEAR, -2, GETDATE())
         AND DEMISSION_DATE < DATEADD(YEAR, -1, GETDATE()) THEN 3

        WHEN DEMISSION_DATE >= DATEADD(YEAR, -3, GETDATE())
         AND DEMISSION_DATE < DATEADD(YEAR, -2, GETDATE()) THEN 4

        WHEN DEMISSION_DATE >= DATEADD(YEAR, -4, GETDATE())
         AND DEMISSION_DATE < DATEADD(YEAR, -3, GETDATE()) THEN 5

        WHEN DEMISSION_DATE >= DATEADD(YEAR, -5, GETDATE())
         AND DEMISSION_DATE < DATEADD(YEAR, -4, GETDATE()) THEN 6

        WHEN DEMISSION_DATE < DATEADD(YEAR, -5, GETDATE()) THEN 7
    END AS PERIODE,
    	-- Année de la date d'entrée
	YEAR(DEMISSION_DATE) AS ANNEE,

	-- Mois à partir du mois de juillet
	((MONTH(DEMISSION_DATE) + 5) % 12) + 1 AS MOIS,

	-- Semaine à partir du mois de juillet
	DATEDIFF(
		WEEK, 
		DATEFROMPARTS(
			YEAR(DEMISSION_DATE) - CASE WHEN MONTH(DEMISSION_DATE) < 7 THEN 1 ELSE 0 END, 7, 1
		), DEMISSION_DATE) + 1 AS SEMAINE



FROM {{ ref("fact_resignation") }}


WHERE DEMISSION_DATE >= DATEADD(YEAR, -10, GETDATE());