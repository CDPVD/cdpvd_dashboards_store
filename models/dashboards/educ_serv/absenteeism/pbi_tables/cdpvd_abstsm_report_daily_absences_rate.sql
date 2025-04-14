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
    Compute the daily absences rate for each students by day of absence.
    Add the CSS level metrics to ease comparison.
#}
{{ config(alias="cdpvd_report_daily_absences_rate") }}


with
    source as (
        select
            annee,
            school_friendly_name,
            date_evenement,
            jour_semaine,
            groupe,
            etape_friendly,
            etape_friendly,
            event_kind,
            max(n_events) as n_events,
            max(n_students_daily) as n_students_daily,
            max(absence_rate) as absence_rate
        from {{ ref("cdpvd_abstsm_stg_daily_metrics") }} as src
        group by
            annee,
            school_friendly_name,
            groupe,
            date_evenement,
            jour_semaine,
            etape_friendly,
            etape_friendly,
            event_kind
    ),
    agg as (
        select
            annee,
            coalesce(school_friendly_name, 'Tout le CSS') as school_friendly_name,
            date_evenement,
            jour_semaine,
            coalesce(groupe, 'Tout') as groupe,
            -- coalesce(code_matiere, 'Tout') as code_matiere,
            coalesce(etape_friendly, 'Tout') as etape_friendly,
            event_kind,
            -- The daily is rate is compute as the weighted average of the etapes rates.
            sum(n_events) as n_events,
            sum(n_students_daily) as n_students_daily,
            sum(absence_rate * n_students_daily) / sum(n_students_daily) as absence_rate
        from source as src
        group by
            annee, cube (school_friendly_name, groupe, etape_friendly),
            date_evenement,
            jour_semaine,
            event_kind

    -- Compute the absence_rate at the CSS level (use the weighted absence_rate to
    -- avoid having to re apply corrections on the raw metrics)
    ),
    css as (
        select
            annee,
            date_evenement,
            jour_semaine,
            etape_friendly,
            etape_friendly,
            event_kind,
            sum(absence_rate * n_students_daily)
            / sum(n_students_daily) as absence_rate_css
        from agg
        group by annee, date_evenement, jour_semaine, etape_friendly, event_kind
        group by annee, date_evenement, jour_semaine, etape_friendly, event_kind

    -- Compute the Average (past and future) absence rate for each school
    ),
    school as (
        select
            annee,
            school_friendly_name,
            etape_friendly,
            etape_friendly,
            event_kind,
            sum(absence_rate * n_students_daily)
            / sum(n_students_daily) as avg_absence_rate_school
        from agg
        group by annee, school_friendly_name, etape_friendly, event_kind
        group by annee, school_friendly_name, etape_friendly, event_kind
    -- Compute the Average (past and future) absence rate for each school
    ),
    jour as (
        select
            annee,
            coalesce(school_friendly_name, 'Tout le CSS') as school_friendly_name,
            coalesce(etape_friendly, 'Tout') as etape_friendly,
            event_kind,
            jour_semaine,
            coalesce(groupe, 'Tout') as groupe,
            sum(absence_rate * n_students_daily)
            / sum(n_students_daily) as avg_absence_rate_jour
        from source
        group by
            annee, cube (school_friendly_name, groupe, etape_friendly),
            jour_semaine,
            event_kind

    -- add the css and school metrics to the table
    ),
    aggregated as (
        select
            src.annee,
            src.school_friendly_name,
            src.groupe,
            src.jour_semaine,
            src.date_evenement,
            src.n_students_daily,
            src.etape_friendly,
            src.etape_friendly,
            src.event_kind,
            src.n_events,
            src.absence_rate,
            -- css
            css.absence_rate_css,
            -- school
            school.avg_absence_rate_school,
            -- jour
            jour.avg_absence_rate_jour
        from agg as src
        left join
            css
            on src.annee = css.annee
            and src.date_evenement = css.date_evenement
            and src.event_kind = css.event_kind
            and src.etape_friendly = css.etape_friendly
            and src.etape_friendly = css.etape_friendly
        left join
            school
            on src.annee = school.annee
            and src.school_friendly_name = school.school_friendly_name
            and src.event_kind = school.event_kind
            and src.etape_friendly = school.etape_friendly
            and src.etape_friendly = school.etape_friendly
        left join
            jour
            on src.annee = jour.annee
            and src.school_friendly_name = jour.school_friendly_name
            and src.groupe = jour.groupe
            and src.jour_semaine = jour.jour_semaine
            and src.event_kind = jour.event_kind
            and src.etape_friendly = jour.etape_friendly
            and src.etape_friendly = jour.etape_friendly

    )

select
    -- Add a filter key to sync filters accross vues
    {{
        dbt_utils.generate_surrogate_key(
            ["annee", "school_friendly_name", "etape_friendly", "event_kind", "groupe"]
        )
    }} as filter_key,
    jour_semaine,
    cast(date_evenement as date) as date_evenement,
    n_events,
    absence_rate,
    n_students_daily,
    absence_rate_css,
    avg_absence_rate_school,
    avg_absence_rate_jour
from aggregated
