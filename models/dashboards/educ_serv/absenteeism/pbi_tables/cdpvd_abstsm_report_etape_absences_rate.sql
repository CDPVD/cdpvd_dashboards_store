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
    Compute the etape absences rate for each students by day of absence.
    Add the CSS level metrics to ease comparison.
#}
{{ config(alias="cdpvd_report_etape_absences_rate") }}

with
    source as (
        select
            annee,
            school_friendly_name,
            groupe,
            etape_friendly,
            event_kind,
            max(n_events) as n_events,
            max(n_students_daily) as n_students_daily,
            max(absence_rate) as absence_rate
        from {{ ref("cdpvd_abstsm_stg_daily_metrics") }} as src
        group by annee, school_friendly_name, groupe, etape_friendly, event_kind
    ),
    agg as (
        select
            annee,
            coalesce(school_friendly_name, 'Tout le CSS') as school_friendly_name,
            etape_friendly,
            event_kind,
            coalesce(groupe, 'Tout') as groupe,
            sum(n_events) as n_events,
            sum(n_students_daily) as n_students_daily,
            -- The per etape absence rate is computed as the weighted average of the
            -- daily absence rate
            sum(absence_rate * n_students_daily)
            / sum(n_students_daily) as avg_absence_rate_etape,
            avg(cast(n_students_daily as float)) as weight_etape
        from source as src
        group by annee, cube (school_friendly_name, groupe), etape_friendly, event_kind

    -- Estimate the per etape average absence_rate at the CSS level as the weighted
    -- average of the school's etape absence rate
    ),
    css as (
        select
            annee,
            etape_friendly,
            event_kind,
            sum(absence_rate * n_students_daily)
            / sum(n_students_daily) as avg_absence_rate_etape_css
        {# sum(avg_absence_rate_etape * weight_etape)
            / sum(weight_etape) as avg_absence_rate_etape_css #}
        from source
        group by annee, etape_friendly, event_kind

    -- Compute the Average (past and future) absence rate for each school
    ),
    school as (
        select
            annee,
            coalesce(school_friendly_name, 'Tout le CSS') as school_friendly_name,
            etape_friendly,
            event_kind,
            sum(absence_rate * n_students_daily)
            / sum(n_students_daily) as avg_absence_rate_school
        from source as src
        group by annee, rollup (school_friendly_name), etape_friendly, event_kind

    -- add the css and school metrics to the table
    ),
    aggregated as (
        select
            src.annee,
            src.school_friendly_name,
            src.groupe,
            src.etape_friendly,
            src.weight_etape,
            src.event_kind,
            src.n_events,
            src.n_students_daily,
            src.avg_absence_rate_etape,
            -- css
            css.avg_absence_rate_etape_css,
            -- school
            school.avg_absence_rate_school
        from agg as src
        left join
            css
            on src.annee = css.annee
            and src.etape_friendly = css.etape_friendly
            and src.event_kind = css.event_kind
        left join
            school
            on src.annee = school.annee
            and src.school_friendly_name = school.school_friendly_name
            and src.etape_friendly = school.etape_friendly
            and src.event_kind = school.event_kind
    )

select
    -- Add a filter key to sync filters accross vues
    {{
        dbt_utils.generate_surrogate_key(
            ["annee", "school_friendly_name", "etape_friendly", "event_kind", "groupe"]
        )
    }} as filter_key,
    etape_friendly,
    n_events,
    n_students_daily,
    avg_absence_rate_etape,
    avg_absence_rate_etape_css,
    avg_absence_rate_school
from aggregated
