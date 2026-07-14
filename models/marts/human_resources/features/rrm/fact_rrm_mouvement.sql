WITH COMPARAISON AS 
(
    SELECT 
		2 AS TYPE_RRM,
        MATR, 
		CORP_EMPL,
		LIEU_TRAV,
        DATE_ENTR AS DATE_RRM,
        LEFT(CAST(CORP_EMPL AS VARCHAR(20)), 1) AS CE_COURANT,
        LAG(LEFT(CAST(CORP_EMPL AS VARCHAR(20)), 1))
            OVER (PARTITION BY MATR ORDER BY DATE_ENTR)
            AS CE_ANTERIEUR,
        LAG(ETAT)
            OVER (PARTITION BY MATR ORDER BY DATE_ENTR)
            AS EtatPrecedent,
        {{ informations_date("DATE_ENTR") }}
	FROM {{ ref("i_pai_dos_empl") }}	
)

SELECT 
	TYPE_RRM,
	MATR,
	CORP_EMPL,
	LIEU_TRAV,
	DATE_RRM,
	PERIODE,
	ANNEE,
	MOIS,
	SEMAINE
FROM COMPARAISON
WHERE 
	CE_COURANT < (CE_ANTERIEUR)
	and EtatPrecedent = 'C01'
	AND CE_COURANT NOT IN (0,3,4,5)
