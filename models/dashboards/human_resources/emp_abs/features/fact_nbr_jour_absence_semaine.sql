/**************************************************************************************************/
-- Combine CTEs: jourSemaine, taux, and age
/**************************************************************************************************/
WITH 
jourSemaine AS (
    SELECT
        ac.annee,
        ac.matricule,
        ac.ref_empl,
        ac.categories,
        ac.lieu_trav,
        ac.reg_abs,
        ac.gr_paie,
        ac.corp_empl,
        ac.nombre_jours,
        CAST(ROUND(SUM(CASE WHEN cal.jour_sem !=6 AND cal.jour_sem !=0 THEN 1.0 / 7 ELSE 0 END), 0) AS INT) AS nbr_abs,    
        CAST(ROUND(SUM(CASE WHEN cal.jour_sem = 1 THEN 1.0 / 7 ELSE 0 END), 0) AS INT) AS lundi,    
        CAST(ROUND(SUM(CASE WHEN cal.jour_sem = 2 THEN 1.0 / 7 ELSE 0 END), 0) AS INT) AS mardi,    
        CAST(ROUND(SUM(CASE WHEN cal.jour_sem = 3 THEN 1.0 / 7 ELSE 0 END), 0) AS INT) AS mercredi,
        CAST(ROUND(SUM(CASE WHEN cal.jour_sem = 4 THEN 1.0 / 7 ELSE 0 END), 0) AS INT) AS jeudi,    
        CAST(ROUND(SUM(CASE WHEN cal.jour_sem = 5 THEN 1.0 / 7 ELSE 0 END), 0) AS INT) AS vendredi
    FROM {{ref("fact_absence_consecutive")}} AS ac
    INNER JOIN {{ ref("i_pai_tab_cal_jour") }} AS cal 
        ON cal.date_jour BETWEEN ac.startdate AND ac.enddate
    WHERE cal.jour_sem NOT IN (6, 0)
    GROUP BY
        ac.annee,
        ac.matricule,
        ac.ref_empl,
        ac.categories,
        ac.lieu_trav,
        ac.reg_abs,
        ac.gr_paie,
        ac.corp_empl,
        ac.nombre_jours
),
duree as (
        SELECT
        fac.annee,
        fac.matricule,
        fac.ref_empl,
        fac.categories,
        fac.lieu_trav,
        fac.reg_abs,
        fac.gr_paie,
        fac.corp_empl,
        ROUND(COALESCE(SUM(CAST((pourc_sal * duree) AS FLOAT) / 10000.0) / 7, 0), 4) AS duree_abs, /* MODIFIER*/
        ROUND(COALESCE(SUM(CAST((pourc_sal * duree) AS FLOAT) / 10000.0) / 7, 0) / NULLIF(CAST(jr_tr.jour_trav AS FLOAT), 0),4) AS taux
    FROM {{ ref("fact_absence_consecutive") }} AS fac
        INNER JOIN {{ ref("fact_nbr_jours_travailles") }} AS jr_tr 
        ON fac.annee = jr_tr.an_budg AND fac.gr_paie = jr_tr.gr_paie
    INNER JOIN {{ ref("i_pai_tab_cal_jour") }} AS cal 
        ON cal.date_jour BETWEEN fac.startdate AND fac.enddate
    INNER JOIN {{ ref("dim_employees") }} AS emp 
        ON fac.matricule = emp.matr
    WHERE cal.jour_sem NOT IN (6, 0)

    GROUP BY         
        fac.annee,
        fac.matricule,
        fac.ref_empl,
        fac.categories,
        fac.lieu_trav,
        fac.reg_abs,
        fac.gr_paie,
        fac.corp_empl,
        jr_tr.jour_trav
)

/**************************************************************************************************/
-- Final Query
/**************************************************************************************************/
SELECT 
    js.annee,
    js.matricule,
    js.categories,
    js.lieu_trav,
    js.corp_empl,
    js.nbr_abs,
    d.taux,
    d.duree_abs,
    js.lundi,
    js.mardi,
    js.mercredi,
    js.jeudi,
    js.vendredi,
    js.nombre_jours
FROM jourSemaine AS js
INNER JOIN duree AS d
    ON 
js.annee = d.annee and
js.matricule = d.matricule and
js.ref_empl = d.ref_empl and
js.categories = d.categories and
js.lieu_trav = d.lieu_trav and
js.reg_abs = d.reg_abs and
js.gr_paie = d.gr_paie and
js.corp_empl = d.corp_empl

group by js.annee,
js.matricule,
js.categories,
js.lieu_trav,
js.corp_empl,
js.lundi,
js.mardi,
js.mercredi,
js.jeudi,
js.vendredi,
js.nbr_abs,
d.taux,
d.duree_abs,
    js.nombre_jours