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

WITH donnees_precalculees AS (
    SELECT
        al.annee,
        al.matricule,
        al.ref_empl,
        al.categories,
        al.lieu_trav,
        al.startdate,
        al.enddate,
        al.corp_empl,
        al.reg_abs,
        al.gr_paie,
        emp.sex_friendly_name AS genre,
        cal.jour_sem,
        jr_tr.jour_trav,
        emp.birth_date,
        al.pourc_sal,
        ((pourc_sal * duree) / 10000.0) AS adjusted_duration
    FROM {{ ref("fact_absence_consecutive") }} AS al
    INNER JOIN {{ ref("fact_nbr_jours_travailles") }} AS jr_tr 
        ON al.annee = jr_tr.an_budg AND al.gr_paie = jr_tr.gr_paie
    INNER JOIN {{ ref("i_pai_tab_cal_jour") }} AS cal 
        ON cal.date_jour BETWEEN al.startdate AND al.enddate				
    INNER JOIN {{ ref("dim_employees") }} AS emp 
        ON al.matricule = emp.matr
    WHERE cal.jour_sem NOT IN (6, 0)
),
agregat AS (
    SELECT
        annee,
        matricule,
        ref_empl,
        categories,
        lieu_trav,
        reg_abs,
        corp_empl,
        jour_trav,
        genre,
        CASE
            WHEN DATEDIFF(year, birth_date, CAST(LEFT(annee, 4) + '-07-01' AS DATE)) < 25 THEN '24 ans et moins'
            WHEN DATEDIFF(year, birth_date, CAST(LEFT(annee, 4) + '-07-01' AS DATE)) BETWEEN 25 AND 34 THEN '25 à 34 ans'
            WHEN DATEDIFF(year, birth_date, CAST(LEFT(annee, 4) + '-07-01' AS DATE)) BETWEEN 35 AND 44 THEN '35 à 44 ans'
            WHEN DATEDIFF(year, birth_date, CAST(LEFT(annee, 4) + '-07-01' AS DATE)) BETWEEN 45 AND 54 THEN '45 à 54 ans'
            WHEN DATEDIFF(year, birth_date, CAST(LEFT(annee, 4) + '-07-01' AS DATE)) BETWEEN 55 AND 64 THEN '55 à 64 ans'
            ELSE '65 ans et plus'
        END AS tranche_age,
        SUM(CASE 
        WHEN corp_empl LIKE '3%' THEN 
            CASE WHEN jour_sem = 1 THEN adjusted_duration END / 6.5
        WHEN corp_empl LIKE '5%' THEN 
            CASE WHEN jour_sem = 1 THEN adjusted_duration END / 7.75
        ELSE 
            CASE WHEN jour_sem = 1 THEN adjusted_duration END / 7
        END) AS total_lundi,
        SUM(CASE WHEN jour_sem = 2 THEN adjusted_duration END) / 7 AS total_mardi,
        SUM(CASE WHEN jour_sem = 3 THEN adjusted_duration END) / 7 AS total_mercredi,
        SUM(CASE WHEN jour_sem = 4 THEN adjusted_duration END) / 7 AS total_jeudi,
        SUM(CASE WHEN jour_sem = 5 THEN adjusted_duration END) / 7 AS total_vendredi,
        COALESCE(SUM(adjusted_duration) / NULLIF(7, 0), 0) AS nbr_jour
    FROM donnees_precalculees
    GROUP BY
        annee, matricule, genre, ref_empl, categories, lieu_trav, reg_abs, corp_empl, jour_trav, birth_date
)
SELECT 
    agg.annee,
    agg.matricule,
    agg.genre,
    agg.corp_empl,
    agg.lieu_trav,
    agg.categories, 
    agg.tranche_age,
    agg.total_lundi AS lundi, 
    agg.total_mardi AS mardi, 
    agg.total_mercredi AS mercredi, 
    agg.total_jeudi AS jeudi, 
    agg.total_vendredi AS vendredi, 
    agg.nbr_jour,     
    COALESCE(SUM(agg.nbr_jour) / NULLIF(Sum(agg.jour_trav), 0), 0) / 100 AS taux,
    agg.jour_trav,
    fnp.nbr
FROM agregat AS agg
inner join {{ ref("fact_nombre_personne")}} as fnp 
    on 
    LEFT(agg.annee,4) = fnp.annee 
    and agg.lieu_trav = fnp.lieu_trav 
    and agg.corp_empl = fnp.corp_empl 
    and agg.genre = fnp.genre
where categories is not null
GROUP BY 
    agg.annee, 
    agg.matricule,
    agg.genre,
    agg.corp_empl,        
    agg.ref_empl,
    agg.lieu_trav, 
    agg.categories, 
    agg.tranche_age,
    agg.jour_trav,
    total_lundi, total_mardi, total_mercredi, total_jeudi, total_vendredi, nbr_jour, fnp.nbr;