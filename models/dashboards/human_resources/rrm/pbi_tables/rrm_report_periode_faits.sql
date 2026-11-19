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

{{ config(alias="rrm_report_periode_faits") }}

/*
    Indicateurs de période du tableau de bord RRM (page Mouvements du personnel),
    calculés d'avance pour chaque période de rrm_report_periode.

    Grain : une ligne par période x employé x type_ligne :
      - Effectif initial : en emploi à date_debut (attributs à date_debut).
            retenu = 1 si toujours en emploi au CSS à date_fin, peu importe
            la catégorie ou l'unité (taux de rétention).
      - Effectif final   : en emploi à date_fin (attributs à date_fin).
      - Embauche / Départ / Mouvement vertical : événement de
            int_rrm_evenements dont fin_mois est dans ]date_debut, date_fin]
            (attributs à la date de l'événement).

    Colonnes additives (Power BI : SUM) :
      effectif_initial, effectif_final, effectif_moyen (= (initial + final) / 2),
      embauche, depart, mouvement, retenu,
      depart_annualise / mouvement_annualise (x 12 / nb_mois).
    Taux de roulement = SUM(depart_annualise) / SUM(effectif_moyen), etc.
*/

WITH P AS (
    SELECT * FROM {{ ref("rrm_report_periode") }}
),

EM AS (
    SELECT * FROM {{ ref("int_rrm_effectif_mensuel") }}
),

-- Employés en emploi à la fin de chaque période
FINAUX AS (
    SELECT DISTINCT P.[Période] AS periode, EM.matr
    FROM P
    INNER JOIN EM ON EM.fin_mois = P.date_fin
),

LIGNES AS (
    -- Effectif initial
    SELECT
        P.[Période] AS periode,
        EM.matr,
        'Effectif initial' AS type_ligne,
        EM.cat_emp,
        EM.corp_empl,
        EM.lieu_trav,
        EM.anciennete_tranche,
        1 AS effectif_initial,
        0 AS effectif_final,
        0 AS embauche,
        0 AS depart,
        0 AS mouvement,
        CASE WHEN FI.matr IS NOT NULL THEN 1 ELSE 0 END AS retenu,
        CAST(NULL AS NVARCHAR(10)) AS motif_depart_code,
        CAST(NULL AS NVARCHAR(200)) AS motif_depart,
        P.facteur_annualisation
    FROM P
    INNER JOIN EM ON EM.fin_mois = P.date_debut
    LEFT JOIN FINAUX AS FI
        ON FI.periode = P.[Période]
        AND FI.matr = EM.matr

    UNION ALL

    -- Effectif final
    SELECT
        P.[Période],
        EM.matr,
        'Effectif final',
        EM.cat_emp,
        EM.corp_empl,
        EM.lieu_trav,
        EM.anciennete_tranche,
        0, 1, 0, 0, 0, 0,
        NULL,
        NULL,
        P.facteur_annualisation
    FROM P
    INNER JOIN EM ON EM.fin_mois = P.date_fin

    UNION ALL

    -- Événements de la période
    SELECT
        P.[Période],
        EV.matr,
        EV.type_evenement,
        EV.cat_emp,
        EV.corp_empl,
        EV.lieu_trav,
        EV.anciennete_tranche,
        0, 0,
        CASE WHEN EV.type_evenement = 'Embauche' THEN 1 ELSE 0 END,
        CASE WHEN EV.type_evenement = 'Départ' THEN 1 ELSE 0 END,
        CASE WHEN EV.type_evenement = 'Mouvement vertical' THEN 1 ELSE 0 END,
        0,
        EV.motif_depart_code,
        EV.motif_depart,
        P.facteur_annualisation
    FROM P
    INNER JOIN {{ ref("int_rrm_evenements") }} AS EV
        ON EV.fin_mois > P.date_debut
        AND EV.fin_mois <= P.date_fin
)

SELECT
    periode,
    matr,
    type_ligne,
    cat_emp,
    corp_empl,
    lieu_trav,
    anciennete_tranche,
    effectif_initial,
    effectif_final,
    CAST((effectif_initial + effectif_final) / 2.0 AS FLOAT) AS effectif_moyen,
    embauche,
    depart,
    mouvement,
    retenu,
    CAST(depart * facteur_annualisation AS FLOAT) AS depart_annualise,
    CAST(mouvement * facteur_annualisation AS FLOAT) AS mouvement_annualise,
    motif_depart_code,
    motif_depart
FROM LIGNES
