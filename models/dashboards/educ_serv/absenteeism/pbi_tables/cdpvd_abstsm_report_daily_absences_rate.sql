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
    Taux d'absence quotidien.

    Calcule le taux d'absence journalier (par école, groupe, étape, type
    et catégorie) et ajoute des métriques de comparaison au niveau CSS et
    établissement.
#}
{{ config(alias="cdpvd_report_daily_absences_rate") }}


-- ============================================================================
-- ÉTAPE 1: Extraction / regroupement des métriques sources
-- ============================================================================
with
    source as (
        select
            annee_scolaire,
            school_friendly_name,
            ordre_enseignement,
            date_evenement,
            jour_semaine,
            groupe,
            event_kind,
            coalesce(category_abs, 'Tout') as category_abs,
            sum(n_events) as n_events,
            max(n_students_daily) as n_students_daily
        from {{ ref("cdpvd_abstsm_stg_daily_metrics") }} as src
        group by
            annee_scolaire,
            school_friendly_name,
            ordre_enseignement,
            groupe,
            date_evenement,
            jour_semaine,
            event_kind,
            rollup(category_abs)
    ),
    -- ========================================================================
    -- ÉTAPE 2: Agrégation et calcul du taux d'absence au niveau demandé
    -- ========================================================================
    agg as (
        select
            annee_scolaire,
            coalesce(school_friendly_name, 'Tout le CSS') as school_friendly_name,
            coalesce(ordre_enseignement, 'Tout') as ordre_enseignement,
            date_evenement,
            jour_semaine,
            groupe,
            event_kind,
            category_abs,
            sum(n_events) as n_events,
            sum(n_students_daily) as n_students_daily,
            sum(cast(n_events as float)) / sum(n_students_daily) as absence_rate
        from source as src
        group by
            annee_scolaire, cube(school_friendly_name, ordre_enseignement), 
            groupe,
            date_evenement,
            jour_semaine,
            category_abs,
            event_kind

    ),
    -- ========================================================================
    -- ÉTAPE 3: Moyennes par établissement par étape et type d'absence
    -- ========================================================================
    school as (
        select
            annee_scolaire,
            coalesce(school_friendly_name, 'Tout le CSS') as school_friendly_name,
            coalesce(ordre_enseignement, 'Tout') as ordre_enseignement,
            groupe,            
            event_kind,
            category_abs,
            sum(cast(n_events as float))
            / sum(n_students_daily) as avg_absence_rate_school
        from source
        group by annee_scolaire, cube(school_friendly_name, ordre_enseignement), groupe, event_kind, category_abs 
    ),
    -- ========================================================================
    -- ÉTAPE 4: Moyennes par jour de la semaine
    -- ========================================================================
    jour as (
        select
            annee_scolaire,
            coalesce(school_friendly_name, 'Tout le CSS') as school_friendly_name,
            coalesce(ordre_enseignement, 'Tout') as ordre_enseignement,
            event_kind,
            category_abs,
            jour_semaine,
            groupe,
            sum(cast(n_events as float))
            / sum(n_students_daily) as avg_absence_rate_jour
        from source
        group by
            annee_scolaire, cube(school_friendly_name, ordre_enseignement), groupe, category_abs, event_kind, jour_semaine
    ),
    -- ========================================================================
    -- ÉTAPE 5: Assemblage final des métriques (CSS, école, jour)
    -- ========================================================================
    aggregated as (
        select
            src.annee_scolaire,
            src.school_friendly_name,
            src.ordre_enseignement,
            src.groupe,
            src.jour_semaine,
            src.date_evenement,
            src.n_students_daily,
            src.event_kind,
            src.n_events,
            src.category_abs,
            src.absence_rate,
            -- css
            css.absence_rate as absence_rate_css,
            -- school
            school.avg_absence_rate_school,
            -- jour
            jour.avg_absence_rate_jour
        from agg as src
        left join
            agg as css
            on src.annee_scolaire = css.annee_scolaire
            and src.date_evenement = css.date_evenement
            and src.event_kind = css.event_kind
            and src.category_abs = css.category_abs
			and src.jour_semaine = css.jour_semaine
            and src.ordre_enseignement = css.ordre_enseignement
			and css.school_friendly_name = 'Tout le CSS'
            and css.groupe = 'Tout'
        left join
            school
            on src.annee_scolaire = school.annee_scolaire
            and src.ordre_enseignement = school.ordre_enseignement
            and src.school_friendly_name = school.school_friendly_name
            and src.groupe = school.groupe
            and src.event_kind = school.event_kind
            and src.category_abs = school.category_abs
        left join
            jour
            on src.annee_scolaire = jour.annee_scolaire
            and src.school_friendly_name = jour.school_friendly_name
            and src.ordre_enseignement = jour.ordre_enseignement            
            and src.groupe = jour.groupe
            and src.jour_semaine = jour.jour_semaine
            and src.event_kind = jour.event_kind
            and src.category_abs = jour.category_abs

    )

-- ============================================================================
-- ÉTAPE 6: Sélection finale avec clé de filtre pour Power BI
-- ============================================================================

select
    {{
        dbt_utils.generate_surrogate_key(
            [
                "annee_scolaire",
                "school_friendly_name",
                "ordre_enseignement",
                "event_kind",
                "category_abs",
                "groupe",
            ]
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
