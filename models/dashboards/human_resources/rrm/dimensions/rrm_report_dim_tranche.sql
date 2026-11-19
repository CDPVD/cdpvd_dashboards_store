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

{{ config(alias="rrm_report_dim_tranche") }}

/*
    Dimension Tranche d'ancienneté (dim_rrm_tranche_duree), triée par ordre.
    Relations Power BI (anciennete_tranche) : rrm_report_periode_faits,
    rrm_report_periode_mensuel.
*/

SELECT
    tranche AS anciennete_tranche,
    ordre
FROM {{ ref("dim_rrm_tranche_duree") }}
