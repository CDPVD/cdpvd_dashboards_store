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

{{ config(alias="rrm_report_periode_emplois") }}

/*
    Liste nominative des emplois par période du tableau de bord RRM
    (page Mouvements du personnel), calculée d'avance pour chaque période
    de rrm_report_periode.

    Source : int_rrm_emplois (périodes d'emploi de pai_hemp).

    Grain : une ligne par période x matricule x emploi (ref_empl), pour les
    emplois liés à la période :
      - une ligne active (ETAT A, P, S ou C10) qui chevauche
        ]date_debut, date_fin] de la période, ou
      - une fin d'emploi (ETAT C, sauf C10) commencée dans la période.

    L'emploi est résumé à la fin de la période :
      situation  : motif de fin (dernière ligne = ETAT C), sinon Terminé
                   (dernière ligne active terminée) ou En cours.
      date_debut : première entrée en service (ligne active) dans l'emploi.
      date_fin   : fin de la dernière ligne active (veille de l'ETAT C si la
                   ligne active est restée ouverte) ; NULL si en cours.
      catégorie, corps, lieu, statut, % du poste, heures : dernière ligne
                   active (filtres Catégorie, Corps d'emploi, Unité).
      anciennete_tranche : ancienneté au CSS à la fin de la période
                   (filtre Tranche d'ancienneté).
      etat       : ETAT de la dernière ligne.

    employe : icône de statut au CSS (rrm_report_employe.statut_emploi)
      + nom + matricule, pour la ligne repliée de la matrice.
*/

WITH P AS (
    SELECT * FROM {{ ref("rrm_report_periode") }}
),

E AS (
    -- est_fin : ETAT de fin d'emploi (int_rrm_emplois, macro rrm_est_etat_actif)
    SELECT EM.*
    FROM {{ ref("int_rrm_emplois") }} AS EM
),

-- Lignes commencées au plus tard à la fin de la période
LIGNES AS (
    SELECT
        P.[Période] AS periode,
        P.date_fin AS p_fin,
        E.*,
        CASE
            WHEN E.est_fin = 1 AND E.date_debut > P.date_debut THEN 1
            WHEN E.est_fin = 0 AND (E.date_fin IS NULL OR E.date_fin > P.date_debut) THEN 1
            ELSE 0
        END AS dans_periode,
        -- ordre = 1 : ligne la plus récente
        ROW_NUMBER() OVER (
            PARTITION BY P.[Période], E.matr, E.ref_empl ORDER BY E.ordre
        ) AS rang_dernier,
        ROW_NUMBER() OVER (
            PARTITION BY P.[Période], E.matr, E.ref_empl, E.est_fin ORDER BY E.ordre
        ) AS rang_type
    FROM P
    INNER JOIN E
        ON E.date_debut <= P.date_fin
),

-- Résumé de l'emploi porté sur chaque ligne (dernière ligne active)
RESUME AS (
    SELECT
        L.*,
        MAX(L.dans_periode) OVER (PARTITION BY L.periode, L.matr, L.ref_empl) AS emploi_dans_periode,
        MIN(CASE WHEN L.est_fin = 0 THEN L.date_debut END) OVER (PARTITION BY L.periode, L.matr, L.ref_empl) AS debut_emploi,
        MAX(CASE WHEN L.est_fin = 0 THEN 1 ELSE 0 END) OVER (PARTITION BY L.periode, L.matr, L.ref_empl) AS a_actif,
        MAX(CASE WHEN L.est_fin = 0 AND L.rang_type = 1 THEN L.date_fin END) OVER (PARTITION BY L.periode, L.matr, L.ref_empl) AS act_date_fin,
        MAX(CASE WHEN L.est_fin = 0 AND L.rang_type = 1 THEN L.cat_emp END) OVER (PARTITION BY L.periode, L.matr, L.ref_empl) AS act_cat_emp,
        MAX(CASE WHEN L.est_fin = 0 AND L.rang_type = 1 THEN L.corp_empl END) OVER (PARTITION BY L.periode, L.matr, L.ref_empl) AS act_corp_empl,
        MAX(CASE WHEN L.est_fin = 0 AND L.rang_type = 1 THEN L.lieu_trav END) OVER (PARTITION BY L.periode, L.matr, L.ref_empl) AS act_lieu_trav,
        MAX(CASE WHEN L.est_fin = 0 AND L.rang_type = 1 THEN L.stat_eng END) OVER (PARTITION BY L.periode, L.matr, L.ref_empl) AS act_stat_eng,
        MAX(CASE WHEN L.est_fin = 0 AND L.rang_type = 1 THEN L.pourc_post END) OVER (PARTITION BY L.periode, L.matr, L.ref_empl) AS act_pourc_post,
        MAX(CASE WHEN L.est_fin = 0 AND L.rang_type = 1 THEN L.nb_hre_sem END) OVER (PARTITION BY L.periode, L.matr, L.ref_empl) AS act_nb_hre_sem
    FROM LIGNES AS L
)

SELECT
    R.periode,
    R.matr,
    CONCAT(
        CASE EMP.statut_emploi
            WHEN 'Actif' THEN N'🟢 '
            WHEN 'Inactif' THEN N'🟡 '
            WHEN 'Démission' THEN N'🔴 '
            ELSE ''
        END,
        R.[Nom complet], ' (', R.matr, ')'
    ) AS employe,
    R.ref_empl,
    CASE
        WHEN R.est_fin = 1
            THEN COALESCE(NULLIF(LTRIM(SUBSTRING(R.etat, CHARINDEX(' - ', R.etat) + 3, 200)), ''), R.etat)
        WHEN R.act_date_fin <= R.p_fin THEN 'Terminé'
        ELSE 'En cours'
    END AS situation,
    R.debut_emploi AS date_debut,
    COALESCE(
        R.act_date_fin,
        CASE WHEN R.est_fin = 1 AND R.a_actif = 1 THEN DATEADD(DAY, -1, R.date_debut) END
    ) AS date_fin,
    R.act_cat_emp AS cat_emp,
    R.act_corp_empl AS corp_empl,
    R.act_lieu_trav AS lieu_trav,
    R.act_stat_eng AS stat_eng,
    R.etat,
    R.act_pourc_post AS pourc_post,
    R.act_nb_hre_sem AS nb_hre_sem,
    -- Ancienneté au CSS de l'employé à la fin de la période
    T.tranche AS anciennete_tranche

FROM RESUME AS R

LEFT JOIN {{ ref("stg_rrm_dossiers") }} AS D
    ON R.matr = D.matr

OUTER APPLY (
    -- Mois complets : 15 janv. -> 14 mars = 1 mois, 15 janv. -> 15 mars = 2 mois
    SELECT
        DATEDIFF(MONTH, D.date_eng, R.p_fin)
        - CASE WHEN DAY(R.p_fin) < DAY(D.date_eng) THEN 1 ELSE 0 END
        AS anciennete_mois
) AS A

LEFT JOIN {{ ref("dim_rrm_tranche_duree") }} AS T
    ON A.anciennete_mois >= T.mois_min
    AND (T.mois_max IS NULL OR A.anciennete_mois < T.mois_max)

LEFT JOIN {{ ref("rrm_report_employe") }} AS EMP
    ON R.matr = EMP.matr

-- Dernière ligne de l'emploi, emplois liés à la période seulement
WHERE R.rang_dernier = 1
    AND R.emploi_dans_periode = 1
