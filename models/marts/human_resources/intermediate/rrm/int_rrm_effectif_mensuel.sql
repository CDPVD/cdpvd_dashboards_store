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
    Effectif régulier en fin de mois : une ligne par mois x employé en emploi.

    Mois couverts : le mois courant et les nb_mois mois précédents.
    date_photo : dernier jour du mois, ou date_reference (aujourd'hui) pour
    le mois en cours.

    Un employé est compté s'il a une période d'emploi principal régulier
    (fact_rrm_retention) qui couvre date_photo. S'il en a plusieurs (deux
    catégories qui se chevauchent), la période commencée le plus récemment
    est retenue : une seule ligne par employé et par mois.

    Attributs : catégorie / corps / lieu de cette période et ancienneté au CSS
    à date_photo (depuis le premier DATE_ENG).

    Étape intermédiaire de rrm_report_periode, rrm_report_periode_faits,
    rrm_report_periode_mensuel et rrm_report_dim_mois.
*/

{% set nb_mois = 120 %}

WITH MOIS AS (
    SELECT
        EOMONTH(DATEADD(MONTH, -N.decalage, PAR.aujourdhui)) AS fin_mois,
        CASE
            WHEN EOMONTH(DATEADD(MONTH, -N.decalage, PAR.aujourdhui)) > PAR.aujourdhui
                THEN PAR.aujourdhui
            ELSE EOMONTH(DATEADD(MONTH, -N.decalage, PAR.aujourdhui))
        END AS date_photo
    FROM {{ ref("stg_rrm_parametres") }} AS PAR
    CROSS JOIN (VALUES
        {%- for i in range(nb_mois + 1) %}
        ({{ i }}){{ "," if not loop.last }}
        {%- endfor %}
    ) AS N (decalage)
),

EN_EMPLOI AS (
    SELECT
        M.fin_mois,
        M.date_photo,
        R.matr,
        R.ce,
        R.lt,
        ROW_NUMBER() OVER (
            PARTITION BY M.fin_mois, R.matr
            ORDER BY R.date_debut DESC, R.cat_empl
        ) AS rang
    FROM MOIS AS M
    INNER JOIN {{ ref("fact_rrm_retention") }} AS R
        ON R.date_debut <= M.date_photo
        AND R.date_fin >= M.date_photo
)

SELECT
    E.fin_mois,
    E.matr,
    jg.job_group_category AS cat_emp,
    jg.job_group + ' - ' + jg.job_group_description AS corp_empl,
    wp.workplace_name AS lieu_trav,
    T.tranche AS anciennete_tranche

FROM EN_EMPLOI AS E

INNER JOIN {{ ref("stg_rrm_dossiers") }} AS D
    ON E.matr = D.matr

CROSS APPLY (
    -- Mois complets : 15 janv. -> 14 mars = 1 mois, 15 janv. -> 15 mars = 2 mois
    SELECT
        DATEDIFF(MONTH, D.date_eng, E.date_photo)
        - CASE WHEN DAY(E.date_photo) < DAY(D.date_eng) THEN 1 ELSE 0 END
        AS anciennete_mois
) AS A

INNER JOIN {{ ref("dim_rrm_tranche_duree") }} AS T
    ON A.anciennete_mois >= T.mois_min
    AND (T.mois_max IS NULL OR A.anciennete_mois < T.mois_max)

LEFT JOIN {{ ref("dim_mapper_job_group") }} AS jg
    ON E.ce = jg.job_group

LEFT JOIN {{ ref("dim_mapper_workplace") }} AS wp
    ON E.lt = wp.workplace

WHERE E.rang = 1
