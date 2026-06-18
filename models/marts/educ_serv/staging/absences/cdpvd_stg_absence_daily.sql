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
                "{{ this }}", ["fiche", "id_eco", "school_year", "groupe"]
            ),
            core_dashboards_store.create_nonclustered_index("{{ this }}", ["fiche"]),
        ]
    )
}}

with src_with_expected_daily as (
        select
            date_abs,
            fiche,
            id_eco,
            grille,
            groupe,
            jour_semaine,
            is_absence,
            n_periods_events,
            n_periods_expected,
            category_abs,
            event_description,
            remarque,
            prct_observed_periods_over_expected,
            sum(prct_observed_periods_over_expected) over (
                partition by date_abs, fiche, id_eco
            ) as prct_observed_daily_over_expected
        from {{ ref("cdpvd_stg_absence_daily_grid") }}
    ),
    event_kind as (
        select

            id_eco,
            grille,
            groupe,
            jour_semaine,
            is_absence,
            n_periods_events,
            n_periods_expected,
            category_abs,
            event_description,
            remarque,
            fiche,
            date_abs,
            prct_observed_periods_over_expected,
            prct_observed_daily_over_expected,
            case
                when prct_observed_daily_over_expected < 100
                then 'Périodes'
                else 'Journée complète'
            end as event_kind
        from src_with_expected_daily
    ),
    rolledup as (
        select
            case
                when month(date_abs) < 7 then year(date_abs) - 1 else year(date_abs)
            end as school_year,
            date_abs,
            fiche,
            id_eco,
            groupe,
            max(jour_semaine) as jour_semaine,
            max(grille) as grille,  -- dymmy aggregation. Already controlled by the tuple (id_eco, fiche)
            -- coalesce(event_kind, 'tous types') as event_kind,
            event_kind,
            case when event_kind is null then 1 else 0 end as is_aggregate_kind,  -- To flag the 'tous types' category
            -- By additivity of absences / retards : two differents events can't be
            -- registered for the same period
            case
                when event_kind is null then null else min(is_absence)
            end as is_absence,
            category_abs,
            min(remarque) as remarque,
            case
                when event_kind is null then 'tous types' else min(event_description)
            end as event_description,  -- arbitrary : first description in lexicographic order

            sum(
                prct_observed_periods_over_expected
            ) as prct_observed_periods_over_expected,
            sum(prct_observed_daily_over_expected) as prct_observed_daily_over_expected

        from event_kind
        group by
            date_abs,
            jour_semaine,
            fiche,
            id_eco,
            groupe,
            category_abs,
            event_kind  -- Superseed is_absence
    -- Handle the weird case where 0.0001% of students have more observed periods of
    -- absences than
    -- expected periods
    )
select
    school_year,
    date_abs,
    jour_semaine,
    fiche,
    id_eco,
    groupe,
    grille,
    event_kind,
    is_aggregate_kind,
    is_absence,
    category_abs,
    event_description,
    remarque
from rolledup
