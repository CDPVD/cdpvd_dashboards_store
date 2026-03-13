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
    Rapport de distribution des absences quotidiennes par type.
    
#}
{{ config(alias="cdpvd_report_daily_absences_kind_distribution") }}

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
    {% set years_of_data_absences = var("marts")["educ_serv"]["recency"][
        "years_of_data_absences"
    ] %}
    {{
        log(
            "Le nombre d'années de données à extraire pour le comptoir d'absentéisme est : "
            ~ years_of_data_absences,
            true,
        )
    }}
{% endif %}

-- ============================================================================
-- ÉTAPE 1: Extraction des absences journalières brutes
-- ============================================================================
-- Récupération des absences distinctes avec tous les détails et dimensions
with
    source as (
        select distinct
            fiche,
            date_abs as date_evenement,
            jour_semaine,
            id_eco,
            groupe,
            case when etape in ('1', '2', '3') then etape else 0 end as etape,  -- Map the etape to the same kind of values as the ones from the daily students
            event_kind,
            -- Gestion des journées multi‑motifs : si 100% des périodes observées et
            -- plusieurs motifs, regrouper en 'Absence mixte' / 'Motifs multiples'.
            -- 
            case
                when count_overdate_fiche_id_eco > 1
                then 'Absence mixte M-NM'
                else category_abs
            end as category_abs,
            case
                when count_overdate_fiche_id_eco > 1
                then 'Motifs multiples'
                else event_description
            end as event_description
        from {{ ref("cdpvd_fact_absences_daily") }}
        where
            school_year
            >= {{ core_dashboards_store.get_current_year() }}
            - {{ nbre_annee_a_extraire }}
            and is_aggregate_kind = 0  -- Do not consider the aggregated type           
    ),
    abs_aggregated as (
        -- ====================================================================
        -- ÉTAPE 2: Agrégation des absences par dimensions
        -- ====================================================================
        -- Compte les occurrences d'absence pour chaque combinaison de dimensions
        select
            date_evenement,
            jour_semaine,
            id_eco,
            coalesce(groupe, 'Tout') as groupe,
            etape,  -- Map the etape to the same kind of values as the ones from the daily students
            event_kind,
            category_abs,
            event_description,
            count(distinct fiche) as n_events
        from source
        group by
            date_evenement,
            jour_semaine,
            id_eco, rollup (groupe),
            etape,
            event_kind,
            category_abs,
            event_description
    ),
    padding as (
        -- ====================================================================
        -- ÉTAPE 3: Création de la table de padding sans la dimension étape
        -- ====================================================================
        -- Réduit le padding à jours d'école et agrège par dimensions sans l'étape
        select id_eco, groupe, date_evenement, jour_semaine, etape as etape_friendly
        from {{ ref("cdpvd_abstsm_stg_padding") }} as padd
        where padd.is_school_day = 1
        group by id_eco, groupe, date_evenement, jour_semaine, etape

    ),
    augmented as (
        -- ====================================================================
        -- ÉTAPE 4: Jointure avec padding et formatage des dimensions
        -- ====================================================================
        -- Limite aux jours d'école et formate les dimensions (étape, etc.)
        select
            padd.id_eco,
            padd.date_evenement,
            padd.jour_semaine,
            padd.groupe,
            abs_.event_kind,
            abs_.category_abs,
            event_description,
            concat('étape : ', padd.etape_friendly) as etape_friendly,
            n_events
        from padding as padd
        inner join
            abs_aggregated as abs_
            on padd.id_eco = abs_.id_eco
            and padd.date_evenement = abs_.date_evenement
            and padd.groupe = abs_.groupe
            and padd.etape_friendly = abs_.etape
        where abs_.n_events is not null

    ),
    aggregated as (
        -- ====================================================================
        -- ÉTAPE 5: Agrégation multidimensionnelle avec CUBE
        -- ====================================================================
        -- Génère les totaux à tous les niveaux (école, groupe, étape, etc.)
        select
            eco.annee_scolaire,
            coalesce(eco.school_friendly_name, 'Tout le CSS') as school_friendly_name,
            coalesce(eco.cat_eco, 'Tout') as ordre_enseignement,
            aug.groupe,
            aug.date_evenement,
            aug.jour_semaine,
            coalesce(aug.etape_friendly, 'Tout') as etape_friendly,
            coalesce(aug.event_kind, 'Tout') as event_kind,
            coalesce(aug.category_abs, 'Tout') as category_abs,
            aug.event_description,
            sum(n_events) as n_events
        from augmented as aug
        left join {{ ref("dim_mapper_schools") }} as eco on aug.id_eco = eco.id_eco
        group by
            eco.annee_scolaire,
            cube (eco.school_friendly_name, eco.cat_eco, aug.category_abs),
            aug.groupe,
            aug.date_evenement,
            aug.jour_semaine,
            aug.etape_friendly,
            aug.event_kind,
            aug.event_description
    )

-- ============================================================================
-- ÉTAPE 6: Sélection finale avec clé de filtre pour Power BI
-- ============================================================================
-- Génération de clé de surrogat pour l'intégration Power BI
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
    cast(date_evenement as date) as date_evenement,
    jour_semaine,
    event_description,
    n_events
from aggregated as agg
