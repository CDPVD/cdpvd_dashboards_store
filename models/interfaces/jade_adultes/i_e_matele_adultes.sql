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
select
  fiche
  ,annee
  ,freq
  ,mat
  ,grp
  ,datedeb as date_deb
  ,anneesanct as annee_sanct
  ,moissanct as mois_sanct
  ,joursanct as jour_sanct
  ,indtransm
  ,etat
  ,service
  ,res
  ,noseqmat
  ,dateinscriptionres
  ,nbhresstage
from {{ var("database_jade_adultes") }}.dbo.e_matele