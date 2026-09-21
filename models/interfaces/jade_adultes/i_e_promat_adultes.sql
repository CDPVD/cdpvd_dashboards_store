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
    fiche,
    typeprofil as type_profil,
    ses,
    disc,
    ordchrono as ord_chrono,
    mat,
    occurrence,
    noseqmat as no_seq_mat,
    statutprofil as statut_profil,
    nbminsl as nb_mins_l,
    nbminso as nb_mins_o,
    nbminsr as nb_mins_r,
    nbminrea as nb_min_rea,
    datesanct as date_sanct,
    res,
    typmat as typ_mat
from {{ var("database_jade_adultes") }}.dbo.e_promat
