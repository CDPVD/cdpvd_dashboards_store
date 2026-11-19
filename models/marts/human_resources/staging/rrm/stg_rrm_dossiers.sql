{{ config(materialized='table') }}

/*
    Dossiers d'employés : une seule ligne par matricule.

    date_fin_serv n'est retenue que si TOUS les dossiers du matricule sont
    fermés : un dossier encore ouvert (ex. réembauche) = toujours en service.
*/

SELECT
    MATR AS matr,
    MIN(date_eng) AS date_eng,
    CASE
        WHEN COUNT(*) = COUNT(DATE_FIN_SERV)
            THEN CAST(MAX(DATE_FIN_SERV) AS DATE)
    END AS date_fin_serv
FROM {{ ref("i_pai_dos") }}
GROUP BY MATR
