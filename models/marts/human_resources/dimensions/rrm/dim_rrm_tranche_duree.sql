{{ config(materialized='table') }}

/*
    Tranches de durée d'emploi, en mois complets : [mois_min, mois_max[
    Source unique des tranches, utilisée par fact_rrm_retention et par
    les tables Power BI du tableau de bord RRM.
*/

SELECT
    T.ordre,
    T.tranche,
    T.mois_min,
    T.mois_max
FROM (VALUES
    (1, '0-3 mois',    0,  3),
    (2, '3-5 mois',    3,  5),
    (3, '5-12 mois',   5, 12),
    (4, '1-2 ans',    12, 24),
    (5, '2-3 ans',    24, 36),
    (6, '3-4 ans',    36, 48),
    (7, '4-5 ans',    48, 60),
    (8, '5 ans et +', 60, NULL)
) AS T (ordre, tranche, mois_min, mois_max)
