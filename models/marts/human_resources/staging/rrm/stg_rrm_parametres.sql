{{ config(materialized='view') }}

/*
    Paramètres du RRM (une seule ligne), partagés par les modèles de rétention.

    Matérialisé en vue : la date du jour est évaluée au moment où
    fact_rrm_retention est construite, et non figée à la construction
    de ce modèle.
*/

SELECT
    -- Pour rejouer une date passée : CAST('2026-06-30' AS DATE)
    CAST(GETDATE() AS DATE) AS aujourdhui,
    -- Sans fin de service et dernier paiement il y a X jours ou plus = inactif
    -- (statut_emploi de fact_rrm_retention)
    14 AS seuil_inactivite_jours,
    -- Entonnoir : actifs d'aujourd'hui + démissions des X derniers mois
    120 AS fenetre_observation_mois,
    -- rrm_report_entonnoir_categorie : première année scolaire d'entrée dans la catégorie retenue
    2015 AS annee_cohorte_min
