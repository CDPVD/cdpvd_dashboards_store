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

{{ config(alias="rrm_report_cohorte_embauche") }}

{% set nb_mois_suivi = 36 %}

/*
    Cohortes d'embauche du tableau de bord RRM (page Intégration des
    nouveaux employés) : proportion des personnes embauchées qui sont encore
    en emploi régulier au CSS k mois après leur embauche.

    Embauche : événement Embauche de int_rrm_evenements (début de la
    première période régulière du matricule), dont le mois est couvert par
    int_rrm_effectif_mensuel (sinon la présence ne peut pas être vérifiée).

    Grain : une ligne par embauche x mois_depuis_embauche (0 à {{ nb_mois_suivi }}),
    limitée aux mois déjà écoulés (EOMONTH(mois d'embauche, k) <= dernier
    mois de int_rrm_effectif_mensuel) : une cohorte récente n'est observée
    que sur les mois disponibles.

      observe = 1 (dénominateur)
      present = 1 si l'employé est dans int_rrm_effectif_mensuel à la fin
                du mois k, peu importe la catégorie ou l'unité
      Taux de survie = SUM(present) / SUM(observe)

    Attributs à l'embauche : catégorie, corps, unité (filtres du rapport).
    annee_embauche : année scolaire (juillet à juin) du mois d'embauche.
    est_jalon : 1 pour les mois 3, 6, 12, 24 et 36 (tableau des cohortes).
*/

WITH EM AS (
    SELECT matr, fin_mois FROM {{ ref("int_rrm_effectif_mensuel") }}
),

BORNES AS (
    SELECT MIN(fin_mois) AS mois_min, MAX(fin_mois) AS mois_max FROM EM
),

EMBAUCHES AS (
    SELECT
        EV.matr,
        EV.date_evenement AS date_embauche,
        EV.fin_mois AS fin_mois_embauche,
        EV.cat_emp,
        EV.corp_empl,
        EV.lieu_trav
    FROM {{ ref("int_rrm_evenements") }} AS EV
    CROSS JOIN BORNES AS B
    WHERE EV.type_evenement = 'Embauche'
      AND EV.fin_mois >= B.mois_min
      AND EV.fin_mois <= B.mois_max
),

N AS (
    SELECT mois FROM (VALUES
        {%- for i in range(nb_mois_suivi + 1) %}
        ({{ i }}){{ "," if not loop.last }}
        {%- endfor %}
    ) AS V (mois)
)

SELECT
    E.matr,
    E.date_embauche,
    CASE
        WHEN MONTH(E.date_embauche) >= 7
            THEN CONCAT(YEAR(E.date_embauche), '-', YEAR(E.date_embauche) + 1)
        ELSE CONCAT(YEAR(E.date_embauche) - 1, '-', YEAR(E.date_embauche))
    END AS annee_embauche,
    E.cat_emp,
    E.corp_empl,
    E.lieu_trav,
    N.mois AS mois_depuis_embauche,
    CASE WHEN N.mois IN (3, 6, 12, 24, 36) THEN 1 ELSE 0 END AS est_jalon,
    1 AS observe,
    CASE WHEN EM.matr IS NOT NULL THEN 1 ELSE 0 END AS present
FROM EMBAUCHES AS E
CROSS JOIN N
CROSS JOIN BORNES AS B
LEFT JOIN EM
    ON EM.matr = E.matr
    AND EM.fin_mois = EOMONTH(E.fin_mois_embauche, N.mois)
WHERE EOMONTH(E.fin_mois_embauche, N.mois) <= B.mois_max
