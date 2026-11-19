{{ config(materialized='table') }}

/*
    Périodes d'historique de l'emploi principal (emplois réguliers seulement),
    avec leur fin effective.

    Grain : une ligne par période d'historique (stg_main_job_history).

    Étapes :
      1. Périodes de l'emploi principal régulier, postérieures à l'engagement,
         + dernier paiement d'un emploi régulier de la même catégorie
      2. Fin effective de chaque période : aujourd'hui si la période est
         ouverte (ou payée jusqu'à aujourd'hui), sinon le dernier paiement

    Seuls les paiements d'emplois à statut d'engagement régulier
    (stat_eng.is_reg = 1) sont pris en compte : un employé payé uniquement
    sur un emploi non régulier (suppléance, occasionnel) n'est pas
    considéré comme en emploi dans sa période régulière.
*/

-- 1. Périodes d'historique + dernier paiement régulier de la même catégorie
WITH COLLECT_INFO AS (
    SELECT
        HIS.MATR AS matr,
        EMPL.cat_empl,
        CAST(HIS.VALID_FROM AS DATE) AS valid_from,
        CAST(HIS.VALID_UNTIL AS DATE) AS valid_until,
        EMPL.ce,
        EMPL.lt,
        PMNT.dernier_paiement,
        P.aujourdhui,
        EMPL.date_dern_jr_trav
    FROM {{ ref("stg_main_job_history") }} AS HIS
    CROSS JOIN {{ ref("stg_rrm_parametres") }} AS P
    INNER JOIN {{ ref("stg_rrm_dossiers") }} AS DOS
        ON HIS.MATR = DOS.matr
        AND HIS.VALID_FROM >= DOS.date_eng
    INNER JOIN {{ ref("stg_rrm_emplois") }} AS EMPL
        ON HIS.MATR = EMPL.matr
        AND HIS.MAIN_JOB = EMPL.ref_empl
        AND EMPL.is_reg = 1
    CROSS APPLY (
        -- Seuls les paiements d'emplois réguliers prolongent la période.
        -- int_rrm_paiements_reguliers est indexée sur (matr, cat_empl, date_cheq) :
        -- recherche d'index au lieu d'un parcours de la table de paie.
        SELECT MAX(X.date_cheq) AS dernier_paiement
        FROM {{ ref("int_rrm_paiements_reguliers") }} AS X
        WHERE X.matr = HIS.MATR
          AND X.cat_empl = EMPL.cat_empl
          AND X.date_cheq >= HIS.VALID_FROM
          AND X.date_cheq <= DATEADD(
                DAY, 14, COALESCE(HIS.VALID_UNTIL, P.aujourdhui)
              )
    ) AS PMNT
    WHERE HIS.VALID_FROM IS NOT NULL
      AND EMPL.cat_empl IS NOT NULL
      AND PMNT.dernier_paiement IS NOT NULL
)

-- 2. Fin effective de chaque période (on garde aussi le vrai dernier paiement)
SELECT
    CI.matr,
    CI.cat_empl,
    CI.ce,
    CI.lt,
    CI.valid_from AS debut,
    CASE
        WHEN CI.valid_until IS NULL
          OR CI.dernier_paiement >= CI.aujourdhui
            THEN CI.aujourdhui
        ELSE CAST(CI.dernier_paiement AS DATE)
    END AS fin,
    CAST(CI.dernier_paiement AS DATE) AS dernier_paiement,
    CI.date_dern_jr_trav
FROM COLLECT_INFO AS CI
