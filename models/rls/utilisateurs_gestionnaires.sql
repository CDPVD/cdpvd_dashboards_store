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
