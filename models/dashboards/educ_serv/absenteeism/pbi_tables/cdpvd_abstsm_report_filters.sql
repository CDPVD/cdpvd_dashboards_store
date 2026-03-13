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
    Table de filtres consolidée.

    Regroupe toutes les dimensions utiles (année, école, étape, type, catégorie,
    groupe) avec une clé de filtre unique pour permettre le cross-filtering
    entre pages dans Power BI.
#}
{{ config(alias="cdpvd_report_filters") }}

-- ============================================================================
-- ÉTAPE 1: Extraction des dimensions sources
-- ============================================================================
with
    source as (
        select
            annee_scolaire,
            annee,
            eco,
            coalesce(ordre_enseignement, 'Tout') as ordre_enseignement,
            event_kind,
			coalesce(category_abs, 'Tout') as category_abs,
            groupe
        from {{ ref("cdpvd_abstsm_stg_daily_metrics") }} dly
        group by annee_scolaire,annee, cube (eco, ordre_enseignement, category_abs),groupe, event_kind
    ),
    -- ========================================================================
    -- ÉTAPE 2: Enrichissement des libellés d'école
    -- ========================================================================
    nomeco as (
        select
            src.annee_scolaire,
            src.ordre_enseignement,
            src.annee,
            src.eco,
            src.groupe,
            src.event_kind,
            src.category_abs,
            coalesce(eco.school_friendly_name, 'Tout le CSS') as school_friendly_name
        from source src
        left join
            {{ ref("dim_mapper_schools") }} as eco
            on src.eco = eco.eco
            and src.annee = eco.annee        
    )
-- ============================================================================
-- ÉTAPE 3: Sélection finale avec génération de la clé de filtre
-- ============================================================================

select
    annee_scolaire,
    annee,
    school_friendly_name,
    ordre_enseignement,
    event_kind,
    category_abs,
    groupe,
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
    -- RLS hooks :
    eco
from nomeco

