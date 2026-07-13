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
    annee,
    freq,
    mat,
    grp,
    ecocen as eco_cen,
    noseqmat as no_seq_mat,
    indmatetei as ind_mat_etei,
    grh,
    disc,
    ordchrono as ord_chrono,
    nbhresprev as nb_hres_prev,
    nbminrea as nb_min_rea,
    datefin as date_fin,
    statutprofil as statut_profil,
    resens as res_ens
from {{ var("database_jade_adultes") }}.dbo.e_elematfpfga
