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
from users cross apply string_split(ecoles, ',')
