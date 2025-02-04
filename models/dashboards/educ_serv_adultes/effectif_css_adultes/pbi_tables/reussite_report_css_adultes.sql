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
  cte
  as
  (
    SELECT code_perm
      , population
      , fiche
      , annee
      , freq
      , mat
      , grp
      , noseqmat
      , indmatetei
      , grh
      , disc
      , ordchrono
      , nbhresprev
      , nbminrea
      , date_fin
      , statutprofil
      , resens
      , date_deb
      , annee_sanct
      , mois_sanct
      , jour_sanct
      , indtransm
      , service
      , case
        when TRY_CAST(res AS INT) >= 60 or res = 'SU' then 'SU'
        else 'EC'
       END as sanc
      , res
      , nbhresstage
      , descrmat
    FROM {{ ref("fact_reussite_adultes") }}
    where res !='' and annee = 2024
  )
select *
from cte