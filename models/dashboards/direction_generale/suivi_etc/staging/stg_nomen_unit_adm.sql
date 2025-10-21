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

{% if var('use_adjust_nomen_unit_adm', true) %}
    {% set join_table = ref('adjust_nomen_unit_adm') %}
{% else %}
    {% set join_table = "(select null as current_lieu_trav, null as new_lieu_trav, null as new_descr)" %}
{% endif %}

select distinct
    t1.exer_fin,
    t1.code as current_lieu_trav,
    coalesce(t2.new_lieu_trav, t1.code) as new_lieu_trav,
    lower(coalesce(t2.new_descr, t1.descr)) as descr
from {{ ref("i_fin_nomen_unit_adm") }} as t1
left join {{ join_table }} as t2 
    on t2.current_lieu_trav = t1.code