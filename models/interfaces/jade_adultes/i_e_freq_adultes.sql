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
    datedeb as date_deb,
    datefin as date_fin,
    statut,
    org,
    ecocen as eco_cen,
    bat,
    client,
    freq,
    prog,
    activform as activ_form,
    donpers,
    condadmiss as cond_admiss,
    serviceenseign as service_enseign,
    orghor as org_hor,
    typeactiv as type_activ,
    datefinsifca as date_fin_sifca,
    motifdep as motif_depart,
    raisondepart as raison_depart,
    raisongratscol as raison_grat_scol,
    typeparcours as type_parcours,
    indtransm as ind_transm
from {{ var("database_jade_adultes") }}.dbo.e_freq
