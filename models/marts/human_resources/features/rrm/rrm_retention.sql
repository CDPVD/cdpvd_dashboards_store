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

SELECT 
	DOS.MATR, 
	DOS.DATE_ENG,
	EMPL.LIEU_TRAV,
	EMPL.CORP_EMPL,    
    CASE
        WHEN DATE_ENG >= DATEADD(MONTH, -3, GETDATE()) THEN 0

        WHEN DATE_ENG >= DATEADD(MONTH, -5, GETDATE())
        AND DATE_ENG <  DATEADD(MONTH, -3, GETDATE()) THEN 1

        WHEN DATE_ENG >= DATEADD(YEAR, -1, GETDATE())
        AND DATE_ENG <  DATEADD(MONTH, -5, GETDATE()) THEN 2

        WHEN DATE_ENG >= DATEADD(YEAR, -2, GETDATE())
        AND DATE_ENG <  DATEADD(YEAR, -1, GETDATE()) THEN 3

        WHEN DATE_ENG >= DATEADD(YEAR, -3, GETDATE())
        AND DATE_ENG <  DATEADD(YEAR, -2, GETDATE()) THEN 4

        WHEN DATE_ENG >= DATEADD(YEAR, -4, GETDATE())
        AND DATE_ENG <  DATEADD(YEAR, -3, GETDATE()) THEN 5

        WHEN DATE_ENG >= DATEADD(YEAR, -5, GETDATE())
        AND DATE_ENG <  DATEADD(YEAR, -4, GETDATE()) THEN 6

        WHEN DATE_ENG < DATEADD(YEAR, -5, GETDATE()) THEN 7
    END AS PÉRIODE
	from {{ ref("i_pai_dos") }} AS DOS
    INNER JOIN {{ ref("i_pai_dos_empl") }} AS EMPL ON
    DOS.MATR = EMPL.MATR
    AND EMPL.IND_EMPL_PRINC = 1

    WHERE DATE_ENG >= DATEADD(YEAR, -10, GETDATE());