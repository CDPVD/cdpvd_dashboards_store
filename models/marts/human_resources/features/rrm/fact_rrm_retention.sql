{{ config(materialized='table') }}

/*
    Ancienneté et rétention par tranche de durée d'emploi.

    Grain : une ligne par période d'emploi continue (matricule / catégorie).

    Chaîne de construction (staging/rrm) :
      stg_rrm_parametres          date de référence et seuils
      stg_rrm_dossiers            une ligne par matricule (engagement, fin de service)
      stg_rrm_emplois             une ligne par matricule / emploi (catégorie, is_reg)
      stg_rrm_periodes_historique périodes de l'emploi principal régulier + fin effective
      stg_rrm_periodes_continues  fusion en périodes continues (gaps & islands)
      fact_rrm_retention          statut, indicateurs et tranche de durée (ce modèle)

    Statut d'un matricule (statut_emploi) :
      - Démission : départ du CSS = plus aucun emploi actif. Emploi actif =
                    ligne pai_hemp dont l'ETAT n'est pas C (A, P, S) ou est
                    C10 (mise à pied cyclique). date_depart = fin du dernier
                    emploi actif. Sans ligne active dans pai_hemp : repli
                    sur DATE_FIN_SERV (pai_dos) atteinte.
      - Inactif   : encore un emploi actif (pas un départ), mais aucun
                    paiement régulier depuis seuil_inactivite_jours jours ou
                    plus (a quitté le régulier, ou dossier dormant)
      - Actif     : encore un emploi actif et payé récemment

    Lecture « entonnoir » dans Power BI, sur est_derniere_periode = 1 :
    dernière période des actifs d'aujourd'hui + des démissions survenues
    dans les fenetre_observation_mois derniers mois (les démissions plus
    anciennes et les inactifs en sont exclus).
    La durée utilisée est duree_emploi_mois_complets.
*/

-- 7a. Fin du dernier emploi actif (tous emplois confondus, pai_hemp)
WITH FIN_ACTIF AS (
    SELECT
        H.matr,
        MAX(CAST(H.date_fin AS DATE)) AS fin_dernier_actif
    FROM {{ ref("i_pai_hemp") }} AS H
    WHERE {{ rrm_est_etat_actif("H.etat") }}
    GROUP BY H.matr
),

-- 7. Informations au niveau du matricule : fin de service, dernier paiement
--    toutes périodes confondues, rang de chaque période
STATUT AS (
    SELECT
        PC.matr,
        PC.cat_empl,
        PC.no_periode,
        PC.ce,
        PC.lt,
        PC.date_debut,
        PC.date_fin,
        PC.dernier_paiement,
        D.date_fin_serv,
        FA.fin_dernier_actif,
        MAX(PC.dernier_paiement) OVER (PARTITION BY PC.matr) AS dernier_paiement_matr,
        ROW_NUMBER() OVER (
            PARTITION BY PC.matr
            ORDER BY PC.date_debut DESC, PC.date_fin DESC
        ) AS rang_recent,
        P.aujourdhui,
        P.seuil_inactivite_jours,
        P.fenetre_observation_mois
    FROM {{ ref("stg_rrm_periodes_continues") }} AS PC
    CROSS JOIN {{ ref("stg_rrm_parametres") }} AS P
    INNER JOIN {{ ref("stg_rrm_dossiers") }} AS D
        ON PC.matr = D.matr
    LEFT JOIN FIN_ACTIF AS FA
        ON PC.matr = FA.matr
),

-- 8. Statut du matricule
AVEC_STATUT AS (
    SELECT
        S.*,
        CASE
            WHEN S.fin_dernier_actif IS NOT NULL
             AND S.fin_dernier_actif < S.aujourdhui
                THEN S.fin_dernier_actif
            WHEN S.fin_dernier_actif IS NULL
             AND S.date_fin_serv IS NOT NULL
             AND S.date_fin_serv <= S.aujourdhui
                THEN S.date_fin_serv
        END AS date_depart,
        CASE
            WHEN S.fin_dernier_actif IS NOT NULL
             AND S.fin_dernier_actif < S.aujourdhui
                THEN 'Démission'
            WHEN S.fin_dernier_actif IS NULL
             AND S.date_fin_serv IS NOT NULL
             AND S.date_fin_serv <= S.aujourdhui
                THEN 'Démission'
            WHEN S.dernier_paiement_matr IS NULL
              OR DATEDIFF(DAY, S.dernier_paiement_matr, S.aujourdhui) >= S.seuil_inactivite_jours
                THEN 'Inactif'
            ELSE 'Actif'
        END AS statut_emploi
    FROM STATUT AS S
),

-- 9. Indicateurs et date de fin retenue : un actif est en emploi jusqu'à
--    aujourd'hui sur sa période la plus récente
INDICATEURS AS (
    SELECT
        A.*,
        CASE WHEN A.statut_emploi = 'Démission' THEN 1 ELSE 0 END AS est_demission,
        CASE WHEN A.statut_emploi = 'Actif' THEN 1 ELSE 0 END AS est_en_emploi,
        CASE
            WHEN A.statut_emploi = 'Actif' AND A.rang_recent = 1 THEN 1
            ELSE 0
        END AS est_periode_courante,
        -- Population de l'entonnoir : dernière période des actifs
        -- + des démissions survenues dans la fenêtre d'observation
        CASE
            WHEN A.rang_recent = 1
             AND (
                    A.statut_emploi = 'Actif'
                 OR (
                        A.statut_emploi = 'Démission'
                    AND A.date_depart > DATEADD(MONTH, -A.fenetre_observation_mois, A.aujourdhui)
                    )
                 )
                THEN 1
            ELSE 0
        END AS est_derniere_periode,
        CASE
            WHEN A.statut_emploi = 'Actif' AND A.rang_recent = 1
                THEN A.aujourdhui
            ELSE A.date_fin
        END AS date_fin_retenue
    FROM AVEC_STATUT AS A
)

-- 10. Résultat final : une ligne par période continue.
--     duree_emploi_tranche = tranche atteinte par la durée réelle :
--     tranche actuelle pour un actif, tranche de départ pour une démission.
SELECT
    IND.matr,
    IND.cat_empl,
    IND.no_periode,
    IND.ce,
    IND.lt,
    IND.date_debut,
    IND.date_fin_retenue AS date_fin,
    IND.dernier_paiement,
    IND.dernier_paiement_matr,
    IND.date_fin_serv,
    IND.date_depart,
    IND.statut_emploi,
    IND.est_demission,
    IND.est_en_emploi,
    IND.est_periode_courante,
    IND.est_derniere_periode,
    -- Période la plus récente du matricule (une seule par matr) : période
    -- courante d'abord, puis la plus récemment terminée. Référence unique
    -- de int_rrm_evenements, rrm_report_employe et
    -- rrm_report_experience_categorie.
    CASE
        WHEN ROW_NUMBER() OVER (
            PARTITION BY IND.matr
            ORDER BY IND.est_periode_courante DESC, IND.date_fin_retenue DESC, IND.date_debut DESC, IND.cat_empl
        ) = 1
            THEN 1
        ELSE 0
    END AS est_periode_recente_matr,
    DATEDIFF(DAY, IND.date_debut, IND.date_fin_retenue) AS duree_emploi_jours,
    M.mois_complets AS duree_emploi_mois_complets,
    T.tranche AS duree_emploi_tranche,
    T.ordre AS duree_emploi_tranche_ordre,
    IND.aujourdhui AS date_reference
FROM INDICATEURS AS IND
CROSS APPLY (
    -- Mois complets : 15 janv. -> 14 mars = 1 mois, 15 janv. -> 15 mars = 2 mois
    SELECT
        DATEDIFF(MONTH, IND.date_debut, IND.date_fin_retenue)
        - CASE WHEN DAY(IND.date_fin_retenue) < DAY(IND.date_debut) THEN 1 ELSE 0 END
        AS mois_complets
) AS M
-- Tranches de durée d'emploi, en mois complets : [mois_min, mois_max[
INNER JOIN {{ ref("dim_rrm_tranche_duree") }} AS T
    ON M.mois_complets >= T.mois_min
    AND (T.mois_max IS NULL OR M.mois_complets < T.mois_max)
