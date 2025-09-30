with
    users as (
        select
            cle_organisationnelle,
            compte_authentification,
            nom,
            prenom,
            ecoles,
            corps_emploi,
            ecole_principale,
            description_corps_emploi_principal
        from {{ ref("i_identite") }}
        where
            corps_emploi  like '1%'
    ), eco_princ as (
        select
            cle_organisationnelle,
            compte_authentification,
            nom,
            prenom,
            CASE
                WHEN ecole_principale IS NULL THEN ecoles
                WHEN ecoles IS NULL OR LTRIM(RTRIM(ecoles)) = '' THEN ecole_principale
                WHEN NOT EXISTS (
                    SELECT 1
                    FROM STRING_SPLIT(ecoles, ',') AS s
                    WHERE LTRIM(RTRIM(s.value)) = LTRIM(RTRIM(ecole_principale))
                )
                THEN CONCAT(ecoles, ',', ecole_principale)
                ELSE ecoles
            END AS ecoles,
            corps_emploi,
            ecole_principale,
            description_corps_emploi_principal
        from users
    )
select
    cle_organisationnelle,
    compte_authentification,
    nom,
    prenom,
    ecoles,
    corps_emploi,
    ecole_principale,
    description_corps_emploi_principal,
    value as ecole
from eco_princ cross apply string_split(ecoles, ',')
