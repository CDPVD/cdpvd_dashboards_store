{#
Dashboards Store - Helping students, one dashboard at a time.
Copyright (C) 2023  Sciance Inc.

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

     SELECT fiche
          , annee
          , datedeb AS date_deb
          , datefin AS date_fin
          , statut
          , org
          , ecocen AS eco_cen
          , bat
          , client
          , freq
          , prog
          , serviceenseign AS service_enseign
          , orghor AS org_hor
          , typeactiv AS type_activ
          , datefinsifca AS date_fin_sifca
          , motifdep AS motif_depart
          , raisondepart AS raison_depart
          , TypeParcours AS type_parcours
       FROM {{ var("database_jade_adultes")}}.dbo.e_freq