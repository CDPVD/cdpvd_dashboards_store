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

{{ config(alias="rrm_report_periode_mensuel") }}

/*
    Séries mensuelles du tableau de bord RRM (graphiques par mois), limitées
    aux mois de chaque période de rrm_report_periode : fin_mois dans
    ]date_debut, date_fin].

    Grain : une ligne par période x mois x employé x type_ligne :
      - Effectif         : en emploi en fin de mois (effectif = 1,
                           effectif_moyen = 0,5).
      - Effectif mois précédent : en emploi à la fin du mois précédent
                           (effectif_moyen = 0,5, attributs du mois précédent).
      - Embauche / Départ / Mouvement vertical : événements du mois.

    Colonnes additives (Power BI : SUM) : effectif, effectif_moyen
    (= (effectif du mois + effectif du mois précédent) / 2), embauche,
    depart, mouvement.
    Taux de roulement du mois = SUM(depart) / SUM(effectif_moyen).
*/

WITH P AS (
    SELECT * FROM {{ ref("rrm_report_periode") }}
),

EM AS (
    SELECT * FROM {{ ref("int_rrm_effectif_mensuel") }}
),

LIGNES AS (
    SELECT
        P.[Période] AS periode,
        EM.fin_mois,
        EM.matr,
        'Effectif' AS type_ligne,
        EM.cat_emp,
        EM.corp_empl,
        EM.lieu_trav,
        EM.anciennete_tranche,
        1 AS effectif,
        CAST(0.5 AS FLOAT) AS effectif_moyen,
        0 AS embauche,
        0 AS depart,
        0 AS mouvement
    FROM P
    INNER JOIN EM
        ON EM.fin_mois > P.date_debut
        AND EM.fin_mois <= P.date_fin

    UNION ALL

    SELECT
        P.[Période],
        EOMONTH(EM.fin_mois, 1),
        EM.matr,
        'Effectif mois précédent',
        EM.cat_emp,
        EM.corp_empl,
        EM.lieu_trav,
        EM.anciennete_tranche,
        0,
        CAST(0.5 AS FLOAT),
        0, 0, 0
    FROM P
    INNER JOIN EM
        ON EOMONTH(EM.fin_mois, 1) > P.date_debut
        AND EOMONTH(EM.fin_mois, 1) <= P.date_fin

    UNION ALL

    SELECT
        P.[Période],
        EV.fin_mois,
        EV.matr,
        EV.type_evenement,
        EV.cat_emp,
        EV.corp_empl,
        EV.lieu_trav,
        EV.anciennete_tranche,
        0,
        CAST(0 AS FLOAT),
        CASE WHEN EV.type_evenement = 'Embauche' THEN 1 ELSE 0 END,
        CASE WHEN EV.type_evenement = 'Départ' THEN 1 ELSE 0 END,
        CASE WHEN EV.type_evenement = 'Mouvement vertical' THEN 1 ELSE 0 END
    FROM P
    INNER JOIN {{ ref("int_rrm_evenements") }} AS EV
        ON EV.fin_mois > P.date_debut
        AND EV.fin_mois <= P.date_fin
)

SELECT *
FROM LIGNES
