SELECT 
    school_year as annee, 
    lieu_trav, 
    corp_empl, 
    genre, 
    tranche_age, 
    COUNT(matr) AS nbr
FROM (
    SELECT 
        fay.school_year, 
        fay.lieu_trav, 
        fay.corp_empl, 
        emp.sex_friendly_name AS genre, 
        fay.matr,
        CASE
            WHEN DATEDIFF(year, emp.birth_date, CAST(LEFT(fay.school_year, 4) + '-07-01' AS DATE)) < 25 THEN '24 ans et moins'
            WHEN DATEDIFF(year, emp.birth_date, CAST(LEFT(fay.school_year, 4) + '-07-01' AS DATE)) BETWEEN 25 AND 34 THEN '25 à 34 ans'
            WHEN DATEDIFF(year, emp.birth_date, CAST(LEFT(fay.school_year, 4) + '-07-01' AS DATE)) BETWEEN 35 AND 44 THEN '35 à 44 ans'
            WHEN DATEDIFF(year, emp.birth_date, CAST(LEFT(fay.school_year, 4) + '-07-01' AS DATE)) BETWEEN 45 AND 54 THEN '45 à 54 ans'
            WHEN DATEDIFF(year, emp.birth_date, CAST(LEFT(fay.school_year, 4) + '-07-01' AS DATE)) BETWEEN 55 AND 64 THEN '55 à 64 ans'
            ELSE '65 ans et plus'
        END AS tranche_age
    FROM {{ ref("fact_activity_yearly") }} AS fay
    INNER JOIN {{ ref("dim_employees") }} AS emp 
        ON fay.matr = emp.matr
    WHERE fay.school_year >= {{ core_dashboards_store.get_current_year() - 5 }} 
      AND fay.is_main_job = 1
) AS subquery
GROUP BY school_year, lieu_trav, corp_empl, genre, tranche_age
