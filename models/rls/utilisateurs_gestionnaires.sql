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
        where corps_emploi like '1%'
    ),
    eco_princ as (
        select
            cle_organisationnelle,
            compte_authentification,
            nom,
            prenom,
            case
                when ecole_principale is null
                then ecoles
                when ecoles is null or ltrim(rtrim(ecoles)) = ''
                then ecole_principale
                when
                    not exists (
                        select 1
                        from string_split(ecoles, ',') as s
                        where ltrim(rtrim(s.value)) = ltrim(rtrim(ecole_principale))
                    )
                then concat(ecoles, ',', ecole_principale)
                else ecoles
            end as ecoles,
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
