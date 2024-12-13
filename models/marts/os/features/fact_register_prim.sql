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
-- Identifier les eleves inscrits au css

select
    el.annee
    , el.eco
	, el.nom_ecole
    , def.defavorise
    , el.fiche
    , el.population
    , el.ordre_ens
    , el.niveau_scolaire
    , el.grp_rep
    , dan.date_deb
    , freq.type_freq
    , case
        when el.population like '%reg%' then multi.is_multi
        else 0
    end as is_multi
    , case
        when el.population like '%adapt%' then 1
        else 0
    end as is_spe
    , dan.difficulte
from {{ ref("fact_yearly_student") }} as el 
left join {{ ref("i_gpm_e_dan") }} as dan  
    on dan.id_eco = el.id_eco and dan.fiche = el.fiche
left join {{ ref("fact_cl_multi" )}} as multi
    on multi.id_eco = el.id_eco and multi.grp_rep = el.grp_rep 
left join {{ ref("i_e_freq") }} as freq
    on freq.fiche = el.fiche and freq.date_deb = dan.date_deb
left join {{ ref("eco_defavorise") }} as def
    on def.eco = el.eco
where
    el.annee >= {{ core_dashboards_store.get_current_year() }} - 5  -- Limite par défaut
    and el.ordre_ens != '4'

