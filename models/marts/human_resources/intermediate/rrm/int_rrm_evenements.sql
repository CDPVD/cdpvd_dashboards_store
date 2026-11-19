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

/*
    Événements datés du personnel régulier : une ligne par événement.

    Source : fact_rrm_retention (périodes continues matr / catégorie,
    emploi principal régulier).

    Types d'événement (type_evenement) :
      - Embauche           : début de la première période régulière du matricule
      - Mouvement vertical : entrée dans une nouvelle catégorie, après une
                             période dans une autre catégorie (même règle que
                             rrm_report_experience_categorie.mouvement_vertical)
      - Départ             : départ du CSS (statut Démission de
                             fact_rrm_retention : plus aucun emploi actif),
                             daté de la fin du dernier emploi actif
                             (date_depart). Un Inactif (encore un emploi
                             actif) n'est pas un départ.

    Motif de départ (motif_depart_code / motif_depart, Départ seulement) :
    un départ = fin du dernier emploi actif. Emploi actif = ligne pai_hemp
    dont l'ETAT n'est pas C (A, P, S) ou est C10 (mise à pied cyclique).
    Le motif est le premier ETAT C (sauf C10) qui suit la fin du dernier
    emploi actif ; description de pai_tab_etat_empl. Plusieurs codes le
    même jour : un code autre que C01 (autre emploi au CSS) ou C99
    (à extraire), puis le plus petit code. Un matricule qui a encore un
    emploi actif dans pai_hemp n'a pas de motif (NULL).

    Attributs : catégorie / corps / lieu de la période concernée (catégorie
    obtenue pour un mouvement, dernière catégorie pour un départ) et
    ancienneté au CSS à la date de l'événement (depuis le premier DATE_ENG).

    fin_mois : dernier jour du mois de l'événement.

    Étape intermédiaire de rrm_report_periode_faits et
    rrm_report_periode_mensuel.
*/

WITH PERIODES AS (
    SELECT
        R.*,
        MIN(R.date_debut) OVER (PARTITION BY R.matr) AS date_premiere_periode,
        ROW_NUMBER() OVER (
            PARTITION BY R.matr
            ORDER BY R.date_debut, R.date_fin, R.cat_empl
        ) AS rang_ancien,
        ROW_NUMBER() OVER (
            PARTITION BY R.matr, R.cat_empl
            ORDER BY R.date_debut, R.date_fin
        ) AS rang_ancien_cat
    FROM {{ ref("fact_rrm_retention") }} AS R
),

EVENEMENTS AS (
    -- Embauche
    SELECT
        P.matr, P.ce, P.lt,
        'Embauche' AS type_evenement,
        P.date_debut AS date_evenement
    FROM PERIODES AS P
    WHERE P.rang_ancien = 1

    UNION ALL

    -- Mouvement vertical : première période d'une catégorie autre que la première
    SELECT
        P.matr, P.ce, P.lt,
        'Mouvement vertical',
        P.date_debut
    FROM PERIODES AS P
    WHERE P.rang_ancien_cat = 1
      AND P.date_debut > P.date_premiere_periode

    UNION ALL

    -- Départ du CSS
    SELECT
        P.matr, P.ce, P.lt,
        'Départ',
        P.date_depart
    FROM PERIODES AS P
    WHERE P.est_periode_recente_matr = 1
      AND P.statut_emploi = 'Démission'
),

HEMP_DEPARTS AS (
    SELECT H.matr, H.etat, H.date_eff, H.date_fin
    FROM {{ ref("i_pai_hemp") }} AS H
    WHERE H.matr IN (SELECT matr FROM EVENEMENTS WHERE type_evenement = 'Départ')
),

-- Fin du dernier emploi actif (ETAT autre que C, ou C10)
FIN_ACTIF AS (
    SELECT matr, MAX(date_fin) AS fin_dernier_actif
    FROM HEMP_DEPARTS
    WHERE {{ rrm_est_etat_actif("etat") }}
    GROUP BY matr
),

MOTIFS AS (
    SELECT
        H.matr,
        H.etat AS motif_depart_code,
        ROW_NUMBER() OVER (
            PARTITION BY H.matr
            ORDER BY
                H.date_eff,
                CASE WHEN H.etat IN ('C01', 'C99') THEN 1 ELSE 0 END,
                H.etat
        ) AS rang
    FROM HEMP_DEPARTS AS H
    LEFT JOIN FIN_ACTIF AS F
        ON H.matr = F.matr
    WHERE NOT {{ rrm_est_etat_actif("H.etat") }}
      AND H.date_eff > COALESCE(F.fin_dernier_actif, '1900-01-01')
)

SELECT
    E.matr,
    E.type_evenement,
    MOT.motif_depart_code,
    LTRIM(RTRIM(TE.descr)) AS motif_depart,
    E.date_evenement,
    EOMONTH(E.date_evenement) AS fin_mois,
    jg.job_group_category AS cat_emp,
    jg.job_group + ' - ' + jg.job_group_description AS corp_empl,
    wp.workplace_name AS lieu_trav,
    -- Ancienneté au CSS à la date de l'événement
    T.tranche AS anciennete_tranche

FROM EVENEMENTS AS E

INNER JOIN {{ ref("stg_rrm_dossiers") }} AS D
    ON E.matr = D.matr

CROSS APPLY (
    -- Mois complets : 15 janv. -> 14 mars = 1 mois, 15 janv. -> 15 mars = 2 mois
    SELECT
        DATEDIFF(MONTH, D.date_eng, E.date_evenement)
        - CASE WHEN DAY(E.date_evenement) < DAY(D.date_eng) THEN 1 ELSE 0 END
        AS anciennete_mois
) AS A

INNER JOIN {{ ref("dim_rrm_tranche_duree") }} AS T
    ON A.anciennete_mois >= T.mois_min
    AND (T.mois_max IS NULL OR A.anciennete_mois < T.mois_max)

LEFT JOIN MOTIFS AS MOT
    ON E.matr = MOT.matr
    AND E.type_evenement = 'Départ'
    AND MOT.rang = 1

LEFT JOIN {{ ref("i_pai_tab_etat_empl") }} AS TE
    ON MOT.motif_depart_code = TE.etat_empl

LEFT JOIN {{ ref("dim_mapper_job_group") }} AS jg
    ON E.ce = jg.job_group

LEFT JOIN {{ ref("dim_mapper_workplace") }} AS wp
    ON E.lt = wp.workplace
