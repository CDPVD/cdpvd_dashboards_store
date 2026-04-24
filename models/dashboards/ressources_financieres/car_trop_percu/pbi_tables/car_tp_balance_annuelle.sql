{#
CDPVD Dashboards store
Copyright (C) 2025 CDPVD.

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
{{ config(tags=["rf", "car_trop_percu"], schema="dashboard_car_trop_percu") }}

select
    sld.code_perm,
    ct.nom,
    ct.prenom,
    sld.fiche,
    sld.annee,
    sld.eco,
    ua.new_lieu_trav as unite_admin,
    coalesce(concat(ua.new_lieu_trav, ' - ', ua.descr), '-') as descr_unite_admin,
    sld.car_gpi,
    sld.trp_gpi,
    sld.car_ag,
    sld.tp_ag,
    sld.car_proc,
    sld.trp_proc,
    ct.nom_mere,
    ct.pnom_mere,
    ct.adr_electr_mere,
    ct.nom_pere,
    ct.pnom_pere,
    ct.adr_electr_pere,
    ct.nom_tuteur,
    ct.pnom_tuteur,
    ct.adr_electr_tuteur,
    ct.adresse,
    ct.adresse_contact
from {{ ref("fact_contact") }} ct
left join {{ ref("fact_solde_el") }} sld on sld.code_perm = ct.code_perm
left join {{ ref("stg_car_tp_nomen_unit_adm") }} as ua on ua.code = sld.eco
where sld.code_perm is not null
