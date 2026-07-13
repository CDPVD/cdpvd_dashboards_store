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
    mat,
    descrmat as descr_mat,
    descrabregee as descr_abregee,
    credit,
    genreform as genre_form,
    regimesanct as regime_sanct,
    typecred as type_cred,
    seuilreuss as seuil_reuss,
    nbhrestheo as nb_hres_theo,
    nbhresrev as nb_hres_rev,
    siglesimca as sigle_simca,
    typecours as type_cours,
    anneecycle as annee_cycle,
    secteurform as secteur_form,
    descrmatlongue as descr_mat_longue
from {{ var("database_jade_adultes") }}.dbo.t_mat
