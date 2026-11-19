{{ config(materialized='table') }}

/*
    Périodes d'emploi continues (fusion des périodes d'historique qui se
    chevauchent ou se touchent), par matricule et catégorie d'emploi.

    Grain : une ligne par période continue (matr / cat_empl / no_periode).

    Méthode « gaps & islands » :
      3. Repérer le début de chaque nouvel îlot : une période qui commence
         plus d'un jour après la fin la plus tardive des précédentes
      4. Numéroter les îlots (somme cumulative des débuts d'îlot)
      5. Bornes de chaque îlot
      6. Corps d'emploi / lieu de travail de la période la plus récente
*/

-- 3. Repérer le début de chaque nouvel îlot
WITH PERIODES_MARQUEES AS (
    SELECT
        *,
        CASE
            WHEN debut <= DATEADD(
                DAY, 1,
                MAX(fin) OVER (
                    PARTITION BY matr, cat_empl
                    ORDER BY debut, fin
                    ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
                )
            )
                THEN 0
            ELSE 1
        END AS nouvelle_periode
    FROM {{ ref("stg_rrm_periodes_historique") }}
    WHERE fin >= debut
),

-- 4. Numéroter les îlots
PERIODES_GROUPEES AS (
    SELECT
        *,
        SUM(nouvelle_periode) OVER (
            PARTITION BY matr, cat_empl
            ORDER BY debut, fin
            ROWS UNBOUNDED PRECEDING
        ) AS no_periode
    FROM PERIODES_MARQUEES
),

-- 5. Bornes de chaque période continue
ILOTS AS (
    SELECT
        matr,
        cat_empl,
        no_periode,
        MIN(debut) AS date_debut,
        MAX(fin) AS date_fin,
        MAX(dernier_paiement) AS dernier_paiement
    FROM PERIODES_GROUPEES
    GROUP BY
        matr,
        cat_empl,
        no_periode
),

-- 6. Corps d'emploi / lieu de travail de la période la plus récente de l'îlot
DERNIER_POSTE AS (
    SELECT
        matr,
        cat_empl,
        no_periode,
        ce,
        lt,
        ROW_NUMBER() OVER (
            PARTITION BY matr, cat_empl, no_periode
            ORDER BY debut DESC, fin DESC
        ) AS rn
    FROM PERIODES_GROUPEES
)

SELECT
    I.matr,
    I.cat_empl,
    I.no_periode,
    DP.ce,
    DP.lt,
    I.date_debut,
    I.date_fin,
    I.dernier_paiement
FROM ILOTS AS I
INNER JOIN DERNIER_POSTE AS DP
    ON I.matr = DP.matr
    AND I.cat_empl = DP.cat_empl
    AND I.no_periode = DP.no_periode
    AND DP.rn = 1
