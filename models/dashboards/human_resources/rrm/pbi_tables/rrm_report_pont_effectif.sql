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

{{ config(alias="rrm_report_pont_effectif") }}

/*
    Pont de l'effectif du tableau de bord RRM (graphique en cascade de la
    page Mouvements du personnel) : passage de l'effectif initial à
    l'effectif final de chaque période de rrm_report_periode.

    On compare, pour chaque période et chaque employé, la photo à date_debut
    (effectif initial) et la photo à date_fin (effectif final), lues dans
    rrm_report_periode_faits (lignes Effectif initial / Effectif final ;
    embauches et départs : lignes Embauche / Départ). Chaque ligne porte une variation (+1 / -1) et
    les attributs de la photo concernée (début pour l'effectif initial et
    les sorties, fin pour les entrées), ce qui fait que le pont balance
    sous n'importe quel filtre Catégorie / Corps / Unité / Tranche :

        SUM(variation) = effectif final

    Étapes (etape_ordre) :
      1 Effectif initial              +1  présent au début
      2 Embauches                     +1  absent au début, présent à la fin,
                                          embauche (periode_faits) dans
                                          la période
      3 Retours et autres entrées     +1  absent au début, présent à la fin,
                                          sans embauche dans la période
                                          (retour d'un inactif, réembauche)
      4 Départs du CSS                -1  présent au début, absent à la fin,
                                          départ dans la période
      5 Autres sorties                -1  présent au début, absent à la fin,
                                          sans départ (devenu inactif, etc.)
      6 Changements de catégorie      -1 dans l'ancienne catégorie,
                                      +1 dans la nouvelle (net 0 sans filtre)
      7 Autres changements            -1 / +1 : même catégorie, mais corps,
        (corps, unité, ancienneté)        unité ou tranche d'ancienneté
                                          différents (net 0 sans filtre)

    Les embauches et départs du pont ne comptent que les personnes absentes
    au début ou à la fin : une personne embauchée puis partie pendant la
    période n'y figure pas (elle figure dans rrm_report_periode_faits).
*/

-- Photos et événements de rrm_report_periode_faits : l'effectif initial et
-- l'effectif final du pont sont, par construction, ceux de periode_faits.
WITH FAITS AS (
    SELECT periode, matr, type_ligne, cat_emp, corp_empl, lieu_trav, anciennete_tranche
    FROM {{ ref("rrm_report_periode_faits") }}
),

DEBUT AS (
    SELECT periode, matr, cat_emp, corp_empl, lieu_trav, anciennete_tranche
    FROM FAITS
    WHERE type_ligne = 'Effectif initial'
),

FIN AS (
    SELECT periode, matr, cat_emp, corp_empl, lieu_trav, anciennete_tranche
    FROM FAITS
    WHERE type_ligne = 'Effectif final'
),

EVENEMENTS AS (
    SELECT
        periode,
        matr,
        MAX(CASE WHEN type_ligne = 'Embauche' THEN 1 ELSE 0 END) AS a_embauche,
        MAX(CASE WHEN type_ligne = 'Départ' THEN 1 ELSE 0 END) AS a_depart
    FROM FAITS
    WHERE type_ligne IN ('Embauche', 'Départ')
    GROUP BY periode, matr
),

PAIRES AS (
    SELECT
        COALESCE(D.periode, F.periode) AS periode,
        COALESCE(D.matr, F.matr) AS matr,
        D.matr AS matr_debut,
        F.matr AS matr_fin,
        D.cat_emp AS cat_debut, D.corp_empl AS corps_debut,
        D.lieu_trav AS lieu_debut, D.anciennete_tranche AS tranche_debut,
        F.cat_emp AS cat_fin, F.corp_empl AS corps_fin,
        F.lieu_trav AS lieu_fin, F.anciennete_tranche AS tranche_fin
    FROM DEBUT AS D
    FULL OUTER JOIN FIN AS F
        ON D.periode = F.periode
        AND D.matr = F.matr
),

CLASSEES AS (
    SELECT
        PA.*,
        COALESCE(EV.a_embauche, 0) AS a_embauche,
        COALESCE(EV.a_depart, 0) AS a_depart,
        CASE WHEN ISNULL(PA.cat_debut, '') <> ISNULL(PA.cat_fin, '') THEN 1 ELSE 0 END AS chg_categorie,
        CASE
            WHEN ISNULL(PA.corps_debut, '') <> ISNULL(PA.corps_fin, '')
                OR ISNULL(PA.lieu_debut, '') <> ISNULL(PA.lieu_fin, '')
                OR ISNULL(PA.tranche_debut, '') <> ISNULL(PA.tranche_fin, '')
                THEN 1
            ELSE 0
        END AS chg_autre
    FROM PAIRES AS PA
    LEFT JOIN EVENEMENTS AS EV
        ON PA.periode = EV.periode
        AND PA.matr = EV.matr
),

LIGNES AS (
    -- Effectif initial
    SELECT periode, matr, 1 AS etape_ordre, 1 AS variation,
        cat_debut AS cat_emp, corps_debut AS corp_empl, lieu_debut AS lieu_trav, tranche_debut AS anciennete_tranche
    FROM CLASSEES WHERE matr_debut IS NOT NULL

    UNION ALL

    -- Embauches / Retours et autres entrées
    SELECT periode, matr, CASE WHEN a_embauche = 1 THEN 2 ELSE 3 END, 1,
        cat_fin, corps_fin, lieu_fin, tranche_fin
    FROM CLASSEES WHERE matr_debut IS NULL

    UNION ALL

    -- Départs du CSS / Autres sorties
    SELECT periode, matr, CASE WHEN a_depart = 1 THEN 4 ELSE 5 END, -1,
        cat_debut, corps_debut, lieu_debut, tranche_debut
    FROM CLASSEES WHERE matr_fin IS NULL

    UNION ALL

    -- Changements : sortie de l'ancienne affectation
    SELECT periode, matr, CASE WHEN chg_categorie = 1 THEN 6 ELSE 7 END, -1,
        cat_debut, corps_debut, lieu_debut, tranche_debut
    FROM CLASSEES
    WHERE matr_debut IS NOT NULL AND matr_fin IS NOT NULL
      AND (chg_categorie = 1 OR chg_autre = 1)

    UNION ALL

    -- Changements : entrée dans la nouvelle affectation
    SELECT periode, matr, CASE WHEN chg_categorie = 1 THEN 6 ELSE 7 END, 1,
        cat_fin, corps_fin, lieu_fin, tranche_fin
    FROM CLASSEES
    WHERE matr_debut IS NOT NULL AND matr_fin IS NOT NULL
      AND (chg_categorie = 1 OR chg_autre = 1)
)

SELECT
    L.periode,
    L.matr,
    E.etape,
    L.etape_ordre,
    L.cat_emp,
    L.corp_empl,
    L.lieu_trav,
    L.anciennete_tranche,
    L.variation
FROM LIGNES AS L
INNER JOIN (VALUES
    (1, 'Effectif initial'),
    (2, 'Embauches'),
    (3, 'Retours et autres entrées'),
    (4, 'Départs du CSS'),
    (5, 'Autres sorties'),
    (6, 'Changements de catégorie'),
    (7, 'Autres changements (corps, unité, ancienneté)')
) AS E (etape_ordre, etape)
    ON L.etape_ordre = E.etape_ordre
