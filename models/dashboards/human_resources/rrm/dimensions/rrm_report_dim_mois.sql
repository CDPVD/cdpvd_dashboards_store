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

{{ config(alias="rrm_report_dim_mois") }}

/*
    Dimension Mois (axe des graphiques mensuels) : mois de
    int_rrm_effectif_mensuel. Mois = libellé AAAA-MM, trié par fin_mois.
    Relation Power BI (fin_mois) : rrm_report_periode_mensuel.
*/

SELECT DISTINCT
    fin_mois,
    FORMAT(fin_mois, 'yyyy-MM') AS Mois
FROM {{ ref("int_rrm_effectif_mensuel") }}
