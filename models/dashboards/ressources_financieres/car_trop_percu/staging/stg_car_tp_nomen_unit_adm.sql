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
{{ config(tags=["rf", "car_trop_percu"], schema="rf_staging") }}

{% set use_adjust = (
    var("dashboards", {})
    .get("ressources_financieres", {})
    .get("car_trop_percu", {})
    .get("use_adjust_nomen_unit_adm", False)
) %}

{% if use_adjust %} {% set join_table = ref("adjust_nomen_unit_adm_car_tp") %}
{% else %}
    {% set join_table = "(select null as current_lieu_trav, null as new_lieu_trav, null as new_descr)" %}
{% endif %}

-- Recuperer la derniere valeur de nomenclature pour chaque code de lieu de travail
with seq as (
    select
        coalesce(t1.code, t2.current_lieu_trav, t1.code) as code,
        coalesce(t2.new_lieu_trav, t1.code) as new_lieu_trav, 
        left(coalesce(t2.new_descr, t1.descr), 1) + substring(coalesce(t2.new_descr, t1.descr), 2, len(coalesce(t2.new_descr, t1.descr))) as descr,
        row_number() over (
            partition by coalesce(t1.code, t2.current_lieu_trav)
            order by t1.exer_fin desc
        ) as seq_id
    from {{ ref("i_fin_nomen_unit_adm") }} as t1
    full outer join {{ join_table }} as t2 
        on t2.current_lieu_trav = t1.code

)

select distinct
    code,
    new_lieu_trav,
    descr
from seq
where seq_id = 1