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
    Métriques quotidiennes d'absentéisme avec taux d'absence calculé.
    
    Combine les nombres d'absences avec les nombres d'étudiants (padded et 
    backfilled) pour calculer le taux d'absence par jour, établissement, groupe 
    et étape. Inclut les jours sans absence (taux = 0).
#}
{{
    config(
        alias="cdpvd_stg_daily_metrics",
        post_hook=[
            core_dashboards_store.create_clustered_index(
                "{{ this }}", ["annee", "school_friendly_name", "date_evenement"]
            ),
            core_dashboards_store.create_nonclustered_index(
                "{{ this }}", ["date_evenement"]
            ),
        ],
    )
}}

{% if execute %}
    {% if "nbre_annee_a_extraire" in var("dashboards")["absenteeism"] %}
        {% set nbre_annee_a_extraire = var("dashboards")["absenteeism"][
            "nbre_annee_a_extraire"
        ] %}
        {{
            log(
                "Le nombre d'années de données à extraire pour le tableau de bord d'absentéisme est : "
                ~ nbre_annee_a_extraire,
                true,
            )
        }}
    {% else %}
        {% set nbre_annee_a_extraire = 5 %}
        {{
            log(
                "Le nombre d'années de données à extraire pour le tableau de bord d'absentéisme est par défaut : "
                ~ nbre_annee_a_extraire,
                true,
            )
        }}
    {% endif %}
{% endif %}

with
    source as (
        select
            school_year,
            date_abs,
            jour_semaine,
            fiche,
            id_eco,
            groupe,
            code_matiere,
            grille,
            event_kind,
            is_aggregate_kind,
            is_absence,
            category_abs,
            event_description,
            remarque,
            case when etape in ('1', '2', '3') then etape else 0 end as etape,  -- Map the etape to the same kind of values as the ones from the daily students
            etape_description,
            seq_etape
        from {{ ref("cdpvd_fact_absences_daily") }}
        where
            school_year
            >= {{ core_dashboards_store.get_current_year() }}
            - {{ nbre_annee_a_extraire }}

    -- ============================================================================
    -- ÉTAPE 1: Agrégation des absences par jour, établissement et étape
    -- ============================================================================
    --
    ),
    abs_aggregated as (
        select
            date_abs as date_evenement,
            id_eco,
            jour_semaine,
            coalesce(groupe, 'Tout') as groupe,
            etape,
            event_kind,
            category_abs,
            count(distinct fiche) as n_events
        from source
        group by
            date_abs,
            id_eco,
            jour_semaine, rollup (groupe),
            etape,
            event_kind,
            category_abs

    -- ============================================================================
    -- ÉTAPE 2: Combinaison des absences avec les données de padding
    -- ============================================================================
    ),
    augmented as (
        select
            padd.id_eco,
            padd.date_evenement,
            padd.jour_semaine,
            padd.groupe,
            abs_.event_kind,
            abs_.category_abs,
            padd.etape,
            padd.n_students_daily,
            coalesce(abs_.n_events, 0) as n_events
        from {{ ref("cdpvd_abstsm_stg_padding") }} as padd
        inner join
            abs_aggregated as abs_
            on padd.id_eco = abs_.id_eco
            and padd.date_evenement = abs_.date_evenement
            and padd.groupe = abs_.groupe
            and padd.etape = abs_.etape
        where padd.is_school_day = 1

    -- ============================================================================
    -- ÉTAPE 4: Calcul du taux d'absence (n_events / n_students_daily)
    -- ============================================================================
    ),
    rate as (
        select
            id_eco,
            date_evenement,
            jour_semaine,
            groupe,
            etape,
            event_kind,
            category_abs,
            n_events,
            n_students_daily,
            case
                when n_students_daily = 0 then 0. else n_events * 1.0 / n_students_daily
            end as absence_rate
        from augmented
        where n_students_daily > 0  -- Avoid division by 0

    -- ============================================================================
    -- ÉTAPE 5: Correction des cas dégénérés et formatage des dimensions
    -- ============================================================================
    ),
    corrected as (
        select
            id_eco,
            date_evenement,
            jour_semaine,
            groupe,
            case
                when etape = 0 then 'inconnue' else cast(etape as varchar)
            end as etape_friendly,
            event_kind,
            category_abs,
            n_events,
            n_students_daily,
            case when absence_rate > 1. then 1. else absence_rate end as absence_rate
        from rate
    )

-- ============================================================================
-- ÉTAPE 6: Sélection finale avec enrichissement des noms d'écoles et RLS
-- ============================================================================
select
    annee,
    annee_scolaire,
    school_friendly_name,
    eco.cat_eco as ordre_enseignement,
    date_evenement,
    jour_semaine,
    groupe,
    concat('étape : ', etape_friendly) as etape_friendly,
    event_kind,
    category_abs,
    n_events,
    n_students_daily,
    absence_rate,
    -- RLS hooks:
    src.id_eco,
    eco.eco
from corrected as src
left join {{ ref("dim_mapper_schools") }} as eco on src.id_eco = eco.id_eco
