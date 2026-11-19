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

{{ config(alias="rrm_report_dim_categorie") }}

/*
    Dimension Catégorie d'emploi du tableau de bord RRM : catégories présentes
    dans les tables de faits. Relations Power BI (cat_emp) :
    rrm_report_experience_categorie, rrm_report_entonnoir_categorie,
    rrm_report_periode_faits, rrm_report_periode_mensuel.
*/

SELECT cat_emp FROM {{ ref("rrm_report_experience_categorie") }} WHERE cat_emp IS NOT NULL
UNION
SELECT cat_emp FROM {{ ref("int_rrm_effectif_mensuel") }} WHERE cat_emp IS NOT NULL
UNION
SELECT cat_emp FROM {{ ref("int_rrm_evenements") }} WHERE cat_emp IS NOT NULL
