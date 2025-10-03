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
    l.id_post,
    l.lieu_trav,
    l.pourc,
    l.date_dern_maj,
    lt.descr as lieu_descr 
from {{ ref('i_grh_poste_lieu_trav') }} as l
left join {{ ref('i_pai_tab_lieu_trav') }} as lt
    on lt.lieu_trav = l.lieu_trav