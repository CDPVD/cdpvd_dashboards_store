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
    Liste nominative de tous les emplois des employés du RRM (pedigree).

    Source : pai_hemp (historique des emplois), tous emplois confondus
    (réguliers ou non), pour les matricules présents dans fact_rrm_retention.

    Grain : une ligne par matricule x emploi (ref_empl) x période où les
    caractéristiques de l'emploi ne changent pas. pai_hemp découpe un emploi
    en segments (souvent trimestriels) identiques : les segments consécutifs
    (au plus 1 jour d'écart) aux mêmes corps, lieu, statut d'engagement,
    ETAT, % du poste et heures / semaine sont fusionnés.

    date_fin : NULL si la période est encore ouverte (2079-06-06 dans la paie).

    Étape intermédiaire de rrm_report_periode_emplois.
*/

WITH HEMP AS (
    SELECT DISTINCT
        H.matr,
        H.ref_empl,
        CAST(H.date_eff AS DATE) AS date_eff,
        CAST(H.date_fin AS DATE) AS date_fin,
        H.corp_empl,
        H.lieu_trav,
        H.stat_eng,
        H.etat,
        H.pourc_post,
        H.nb_hre_sem
    FROM {{ ref("i_pai_hemp") }} AS H
    WHERE H.matr IN (SELECT matr FROM {{ ref("fact_rrm_retention") }})
),

-- Segment précédent du même emploi
PRECEDENTS AS (
    SELECT
        H.*,
        LAG(H.date_fin) OVER (PARTITION BY H.matr, H.ref_empl ORDER BY H.date_eff, H.date_fin) AS p_date_fin,
        LAG(CONCAT(
            H.corp_empl, '|', H.lieu_trav, '|', H.stat_eng, '|', H.etat, '|',
            H.pourc_post, '|', H.nb_hre_sem
        )) OVER (PARTITION BY H.matr, H.ref_empl ORDER BY H.date_eff, H.date_fin) AS p_attributs,
        CONCAT(
            H.corp_empl, '|', H.lieu_trav, '|', H.stat_eng, '|', H.etat, '|',
            H.pourc_post, '|', H.nb_hre_sem
        ) AS attributs
    FROM HEMP AS H
),

-- Début d'un nouvel îlot : caractéristique différente ou trou de plus d'un jour
MARQUES AS (
    SELECT
        P.*,
        CASE
            WHEN P.p_date_fin IS NOT NULL
             AND P.date_eff <= DATEADD(DAY, 1, P.p_date_fin)
             AND P.attributs = P.p_attributs
                THEN 0
            ELSE 1
        END AS nouvel_ilot
    FROM PRECEDENTS AS P
),

ILOTS AS (
    SELECT
        M.*,
        SUM(M.nouvel_ilot) OVER (
            PARTITION BY M.matr, M.ref_empl
            ORDER BY M.date_eff, M.date_fin
            ROWS UNBOUNDED PRECEDING
        ) AS no_ilot
    FROM MARQUES AS M
),

PERIODES AS (
    SELECT
        matr,
        ref_empl,
        no_ilot,
        corp_empl,
        lieu_trav,
        stat_eng,
        etat,
        pourc_post,
        nb_hre_sem,
        MIN(date_eff) AS date_debut,
        MAX(date_fin) AS date_fin
    FROM ILOTS
    GROUP BY
        matr,
        ref_empl,
        no_ilot,
        corp_empl,
        lieu_trav,
        stat_eng,
        etat,
        pourc_post,
        nb_hre_sem
)

SELECT
    P.matr,
    CONCAT(emp.last_name, ', ', emp.first_name) AS [Nom complet],
    P.ref_empl,
    P.date_debut,
    CASE WHEN P.date_fin >= '2079-01-01' THEN NULL ELSE P.date_fin END AS date_fin,
    jg.job_group_category AS cat_emp,
    -- Code seul quand la description est absente
    COALESCE(P.corp_empl + ' - ' + jg.job_group_description, P.corp_empl) AS corp_empl,
    COALESCE(wp.workplace_name, P.lieu_trav) AS lieu_trav,
    COALESCE(P.stat_eng + ' - ' + NULLIF(LTRIM(RTRIM(se.descr)), ''), P.stat_eng) AS stat_eng,
    COALESCE(P.etat + ' - ' + NULLIF(LTRIM(RTRIM(te.descr)), ''), P.etat) AS etat,
    P.pourc_post,
    P.nb_hre_sem,
    -- Fin d'emploi : ETAT C qui n'est pas un ETAT actif (macro rrm_est_etat_actif)
    CASE WHEN NOT {{ rrm_est_etat_actif("P.etat") }} THEN 1 ELSE 0 END AS est_fin,
    -- Tri : emplois les plus récents en premier
    ROW_NUMBER() OVER (
        PARTITION BY P.matr
        ORDER BY P.date_debut DESC, P.ref_empl, P.no_ilot DESC
    ) AS ordre

FROM PERIODES AS P

INNER JOIN {{ ref("dim_employees") }} AS emp
    ON P.matr = emp.matr

LEFT JOIN {{ ref("dim_mapper_job_group") }} AS jg
    ON P.corp_empl = jg.job_group

LEFT JOIN {{ ref("dim_mapper_workplace") }} AS wp
    ON P.lieu_trav = wp.workplace

LEFT JOIN {{ ref("stat_eng") }} AS se
    ON P.stat_eng = se.stat_eng

LEFT JOIN {{ ref("i_pai_tab_etat_empl") }} AS te
    ON P.etat = te.etat_empl
