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
{#
    Gather all the filters into one table to allow for between-pages corss filtering.
#}
{{ config(alias="cdpvd_report_filters") }}

with
    source as (
        select
            annee,
            coalesce(school_friendly_name, 'Tout le CSS') as school_friendly_name,
            event_kind,
            coalesce(groupe, 'Tout') as groupe,
            -- RLS hooks : 
            max(id_eco) as id_eco,
            max(eco) as eco
        from {{ ref("cdpvd_abstsm_stg_daily_metrics") }} dly
        group by annee, cube (school_friendly_name, groupe), event_kind
    )

select
    annee,
    school_friendly_name,
    event_kind,
    groupe,
    {{
        dbt_utils.generate_surrogate_key(
            ["annee", "school_friendly_name", "event_kind", "groupe"]
        )
    }} as filter_key,
    -- RLS hooks :
    id_eco,
    eco
from source
