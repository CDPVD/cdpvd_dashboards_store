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
{#
	Compute the number of absences and delay for each students by day of absence / delay.
    Events are further qualified by the number of periods impacted (full day / partial day).
    Each absence is mapped to the student's etape.
#}

{{
    config(
        post_hook=[
            core_dashboards_store.create_clustered_index(
                "{{ this }}", ["fiche", "id_eco",  "groupe"]
            ),
            core_dashboards_store.create_nonclustered_index("{{ this }}", ["fiche"]),
        ]
    )
}}
{% set max_periodes = var("interfaces")["gpi"]["max_periodes"] + 1 %}


with
    src_with_grid_id as (
        select
            date_abs,
            fiche,
            id_eco,
            grille,
            groupe,
            is_absence,
            n_periods_events,
            event_description,
            category_abs,
            remarque
        from {{ ref("cdpvd_stg_absence_daily_src") }} as src 

    -- Pre compute the expected daily number of periods per grid : later used to split
    -- days between the day of complete absence, and day of partial absence
    ),
    grid as (
        select
            id_eco,
            date_evenement,
            grille,
            format(date_evenement, 'dddd', 'fr-FR') as jour_semaine,
            -- datename(weekday, date_evenement) as jour_semaine,
            {% for i in range(1, max_periodes) %}
                case when max(per_{{ "%02d" % i }}) is null then 0 else 1 end
                {%- if not loop.last %} +{% endif -%}
            {% endfor %} as n_periods_expected
        from {{ ref("i_gpm_t_cal") }}
        where jour_cycle is not null  -- Only keep working days
        group by id_eco, date_evenement, grille

    -- Add the expected number of periods to the observed events
    )
select
    src.date_abs,
    src.fiche,
    src.id_eco,
    src.grille,
    coalesce(groupe, '-') as groupe,
    grid.jour_semaine,
    src.is_absence,
    src.n_periods_events,
    grid.n_periods_expected,
    src.event_description,
    category_abs,
    src.remarque,
    src.n_periods_events
    * 100.0
    / grid.n_periods_expected as prct_observed_periods_over_expected
from src_with_grid_id as src
join
    grid
    on src.id_eco = grid.id_eco
    and src.date_abs = grid.date_evenement
    and src.grille = grid.grille
where grid.n_periods_expected > 0  -- If no period is expected then we can't compute an absence rate.
