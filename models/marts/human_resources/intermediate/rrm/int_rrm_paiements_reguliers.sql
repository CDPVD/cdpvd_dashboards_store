{{
    config(
        materialized='table',
        post_hook=[
            core_dashboards_store.create_clustered_index(
                "{{ this }}", ["matr", "cat_empl", "date_cheq"], unique=True
            )
        ]
    )
}}

/*
    Dates de paiement d'emplois réguliers (stat_eng.is_reg = 1), par
    matricule et catégorie d'emploi.

    Pré-agrégation de i_pai_hchq_pmnt (plusieurs millions de lignes) pour
    stg_rrm_periodes_historique : l'index cluster (matr, cat_empl, date_cheq) permet
    de trouver le dernier paiement d'une période par simple recherche d'index
    plutôt que de relire toute la table de paie pour chaque période.

    La catégorie d'un emploi suit la même règle que dans stg_rrm_emplois :
    LEFT(MAX(corp_empl), 1) par matricule / emploi.
*/

WITH EMPL_REG AS (
    SELECT
        E.matr,
        E.ref_empl,
        E.cat_empl
    FROM (
        SELECT
            matr,
            ref_empl,
            LEFT(MAX(corp_empl), 1) AS cat_empl,
            MAX(stat_eng) AS stat_eng
        FROM {{ ref("i_pai_dos_empl") }}
        GROUP BY
            matr,
            ref_empl
    ) AS E
    INNER JOIN {{ ref("stat_eng") }} AS ETA
        ON E.stat_eng = ETA.stat_eng
    WHERE ETA.is_reg = 1
      AND E.cat_empl IS NOT NULL
)

SELECT
    X.matr,
    E.cat_empl,
    CAST(X.date_cheq AS DATE) AS date_cheq
FROM {{ ref("i_pai_hchq_pmnt") }} AS X
INNER JOIN EMPL_REG AS E
    ON X.matr = E.matr
    AND X.ref_empl = E.ref_empl
WHERE X.date_cheq IS NOT NULL
GROUP BY
    X.matr,
    E.cat_empl,
    CAST(X.date_cheq AS DATE)
