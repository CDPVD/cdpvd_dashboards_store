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

{{ config(alias="rrm_report_experience_categorie") }}

/*
    Une ligne par employé et catégorie d'emploi (matr, cat_empl).

      experience_tranche : tranche de l'expérience dans la catégorie (somme
                           des périodes, interruptions exclues ;
                           mois = jours / 30.4375)
      anciennete_tranche : ancienneté au CSS de l'employé, de date_engagement
                           (premier DATE_ENG) à aujourd'hui (actif) ou à la
                           fin de la dernière période (départ)
      date_debut_cat     : entrée dans la catégorie (cohorte de l'entonnoir)

    Indicateurs 0/1 :
      est_categorie_actuelle : catégorie actuelle de l'employé (ou dernière
                               pour un départ)
    Issue de la catégorie, exclusive (en_cours + depart + sortie_autre_cat = 1) :
      en_cours           : l'employé est dans cette catégorie aujourd'hui
      depart             : dernière catégorie de l'employé, qui a quitté le CSS
                           (statut Démission : plus aucun emploi actif)
      sortie_autre_cat   : l'employé a quitté cette catégorie pour une autre,
                           ou a quitté le régulier en gardant un emploi actif
                           (statut Inactif)

    Source de rrm_report_entonnoir_categorie. Relation : rrm_report_employe (matr).
*/

WITH PERIODES AS (
    SELECT
        R.*,
        MAX(R.date_fin) OVER (PARTITION BY R.matr) AS date_fin_suivi,
        -- Période la plus récente de la catégorie (corps -> catégorie)
        ROW_NUMBER() OVER (
            PARTITION BY R.matr, R.cat_empl
            ORDER BY R.date_fin DESC, R.date_debut DESC
        ) AS rang_recent_cat
    FROM {{ ref("fact_rrm_retention") }} AS R
),

CATEGORIES AS (
    SELECT
        P.matr,
        P.cat_empl,
        MIN(P.date_fin_suivi) AS date_fin_suivi,
        MIN(P.date_debut) AS date_debut_cat,
        SUM(P.duree_emploi_jours) AS experience_jours,
        MAX(CASE WHEN P.rang_recent_cat = 1 THEN P.ce END) AS ce,
        MAX(CASE WHEN P.statut_emploi = 'Démission' THEN 1 ELSE 0 END) AS est_demission,
        -- Période la plus récente de l'employé, toutes catégories (fact_rrm_retention)
        MAX(P.est_periode_recente_matr) AS est_derniere_cat,
        MAX(
            CASE WHEN P.statut_emploi = 'Actif' AND P.date_fin >= P.date_reference THEN 1 ELSE 0 END
        ) AS est_ouverte
    FROM PERIODES AS P
    GROUP BY
        P.matr,
        P.cat_empl
)

SELECT
    C.matr,
    C.cat_empl,
    jg.job_group_category AS cat_emp,
    C.date_debut_cat,
    T.tranche AS experience_tranche,
    T.ordre AS experience_tranche_ordre,
    C.est_ouverte AS en_cours,
    CASE WHEN C.est_ouverte = 0 AND C.est_derniere_cat = 1 AND C.est_demission = 1 THEN 1 ELSE 0 END AS depart,
    CASE
        WHEN C.est_ouverte = 0 AND (C.est_derniere_cat = 0 OR C.est_demission = 0) THEN 1
        ELSE 0
    END AS sortie_autre_cat,
    C.est_derniere_cat AS est_categorie_actuelle,
    TA.tranche AS anciennete_tranche,
    TA.ordre AS anciennete_tranche_ordre

FROM CATEGORIES AS C

CROSS APPLY (
    SELECT CAST(FLOOR(C.experience_jours / 30.4375) AS INT) AS experience_mois
) AS M

INNER JOIN {{ ref("dim_rrm_tranche_duree") }} AS T
    ON M.experience_mois >= T.mois_min
    AND (T.mois_max IS NULL OR M.experience_mois < T.mois_max)

INNER JOIN {{ ref("stg_rrm_dossiers") }} AS D
    ON C.matr = D.matr

CROSS APPLY (
    -- Mois complets : 15 janv. -> 14 mars = 1 mois, 15 janv. -> 15 mars = 2 mois
    SELECT
        DATEDIFF(MONTH, D.date_eng, C.date_fin_suivi)
            - CASE WHEN DAY(C.date_fin_suivi) < DAY(D.date_eng) THEN 1 ELSE 0 END
            AS anciennete_mois
) AS A

INNER JOIN {{ ref("dim_rrm_tranche_duree") }} AS TA
    ON A.anciennete_mois >= TA.mois_min
    AND (TA.mois_max IS NULL OR A.anciennete_mois < TA.mois_max)

INNER JOIN {{ ref("dim_mapper_job_group") }} AS jg
    ON C.ce = jg.job_group
