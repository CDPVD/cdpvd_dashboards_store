{{ config(materialized='table') }}

/*
    Emplois : une ligne par matricule / emploi, avec la catégorie d'emploi
    et l'indicateur de statut d'engagement régulier (stat_eng.is_reg).

    NB : corp_empl et stat_eng sont pris par MAX sur toutes les lignes de
    l'emploi ; sans conséquence tant qu'un REF_EMPL ne change pas de
    catégorie ni de statut au fil du temps.
*/

SELECT
    E.matr,
    E.ref_empl,
    E.ce,
    E.cat_empl,
    E.lt,
    E.stat_eng,
    E.date_dern_jr_trav,
    COALESCE(ETA.is_reg, 0) AS is_reg
FROM (
    SELECT
        MATR AS matr,
        REF_EMPL AS ref_empl,
        MAX(corp_empl) AS ce,
        LEFT(MAX(corp_empl), 1) AS cat_empl,
        MAX(lieu_trav) AS lt,
        MAX(stat_eng) AS stat_eng,
        MAX(date_dern_jr_trav) AS date_dern_jr_trav
    FROM {{ ref("i_pai_dos_empl") }}
    GROUP BY
        MATR,
        REF_EMPL
) AS E
LEFT JOIN {{ ref("stat_eng") }} AS ETA
    ON E.stat_eng = ETA.stat_eng
