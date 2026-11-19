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

{{ config(alias="rrm_report_employe") }}

/*
    Dimension Employé(e) : une ligne par employé du RRM (matr).

      Nom complet   : segment de recherche de la liste nominative.
      statut_emploi : statut au CSS (fact_rrm_retention) : Actif, Inactif
                      (encore un emploi actif, sans paie régulière) ou
                      Démission (plus aucun emploi actif). Sert à l'icône
                      de rrm_report_periode_emplois.

    Relations Power BI : rrm_report_experience_categorie,
    rrm_report_entonnoir_categorie, rrm_report_periode_emplois (matr).
*/

SELECT
    S.matr,
    CONCAT(emp.last_name, ', ', emp.first_name) AS [Nom complet],
    S.statut_emploi

-- Période la plus récente du matricule (fact_rrm_retention)
FROM {{ ref("fact_rrm_retention") }} AS S

INNER JOIN {{ ref("dim_employees") }} AS emp
    ON S.matr = emp.matr

WHERE S.est_periode_recente_matr = 1
