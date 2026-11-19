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

{{ config(alias="rrm_report_periode") }}

/*
    Périodes du segment « Période » du tableau de bord RRM : fenêtres
    glissantes de nb_mois mois qui se terminent au dernier mois de
    int_rrm_effectif_mensuel (mois courant).

    date_debut : fin du mois précédant la fenêtre (photo de l'effectif initial).
    date_fin   : dernier mois (photo de l'effectif final).
    Les événements comptés sont ceux dont fin_mois est dans ]date_debut, date_fin].

    Facteur d'annualisation des taux : 12 / nb_mois.
*/

WITH FIN AS (
    SELECT MAX(fin_mois) AS date_fin
    FROM {{ ref("int_rrm_effectif_mensuel") }}
)

SELECT
    P.periode AS [Période],
    P.nb_mois AS Mois,
    EOMONTH(F.date_fin, -P.nb_mois) AS date_debut,
    F.date_fin,
    CAST(12 AS FLOAT) / P.nb_mois AS facteur_annualisation
FROM (VALUES
    ('3 derniers mois', 3),
    ('5 derniers mois', 5),
    ('12 derniers mois', 12),
    ('24 derniers mois', 24),
    ('36 derniers mois', 36),
    ('48 derniers mois', 48),
    ('60 derniers mois', 60),
    ('120 derniers mois', 120)
) AS P (periode, nb_mois)
CROSS JOIN FIN AS F
