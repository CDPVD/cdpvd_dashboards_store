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
    Taux d'absence par étape.

    Calcule le taux d'absence moyen pondéré par étape et ajoute des métriques
    de comparaison au niveau CSS et établissement.
#}
{{ config(alias="cdpvd_report_etape_absences_rate") }}

-- ============================================================================
-- ÉTAPE 1: Extraction et calcul des taux par étape
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
            etape_friendly,
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
            etape_friendly,
            event_kind,
            rollup (category_abs)
	),
    agg as (
        select
            annee_scolaire,
            coalesce(school_friendly_name, 'Tout le CSS') as school_friendly_name,
            coalesce(ordre_enseignement, 'Tout') as ordre_enseignement,
            groupe,
            etape_friendly,
            event_kind,
			category_abs,
			sum(n_events) as n_events,
            max(n_students_daily) as n_students_daily,
            sum(cast(n_events as float)) / sum(n_students_daily) as avg_absence_rate_etape
        from source as src
        group by annee_scolaire, cube(school_friendly_name, ordre_enseignement), groupe, category_abs, etape_friendly, event_kind
    ),
    -- ========================================================================
    -- ÉTAPE 2: Moyennes annuelles par établissement
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
        from source as src
        group by annee_scolaire, cube(school_friendly_name,ordre_enseignement), groupe, category_abs, event_kind

    ),    
    -- ========================================================================
    -- ÉTAPE 3: Assemblage final des métriques (CSS, école)
    -- ========================================================================
    aggregated as (
        select
            src.annee_scolaire,
            src.school_friendly_name,
            src.ordre_enseignement,
            src.groupe,
            src.etape_friendly,
            src.event_kind,
            src.category_abs,
            src.avg_absence_rate_etape,
            -- css
            css.avg_absence_rate_etape as avg_absence_rate_etape_css,
            -- school
            school.avg_absence_rate_school            
        from agg as src
        left join
            agg css
            on src.annee_scolaire = css.annee_scolaire
            and src.etape_friendly = css.etape_friendly
            and src.event_kind = css.event_kind
            and src.category_abs = css.category_abs
            and src.ordre_enseignement = css.ordre_enseignement
            and css.school_friendly_name = 'Tout le CSS' 
            and css.groupe = 'Tout'
        left join
            school 
            on src.annee_scolaire = school.annee_scolaire
            and src.school_friendly_name = school.school_friendly_name
            and src.ordre_enseignement = school.ordre_enseignement
            and src.groupe = school.groupe
            and src.event_kind = school.event_kind
			and src.category_abs = school.category_abs            

    )

-- ============================================================================
-- ÉTAPE 4: Sélection finale avec clé de filtre pour Power BI
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
    etape_friendly,
    avg_absence_rate_etape,
    avg_absence_rate_etape_css,
    avg_absence_rate_school
from aggregated
