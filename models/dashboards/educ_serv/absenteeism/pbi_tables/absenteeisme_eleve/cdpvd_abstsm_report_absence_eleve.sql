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
    Rapport d'absence au niveau élève.
    
    Expose les absences journalières par élève avec détails (motif, étape, groupe).
#}

{{ config(alias="cdpvd_report_absence_eleve") }}

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
-- Rapport individuel des absences
-- ============================================================================
-- Crée une ligne par élève/jour/absence avec :
-- - Clé unique (id_eco, grille, date_abs)
-- - Normalisation des multi-motifs (si >1 motif sur la journée → 'Absence mixte')
-- ============================================================================

select distinct
    {{
        dbt_utils.generate_surrogate_key(
            [
                "src.id_eco",
                "src.grille",
                "src.date_abs",
            ]
        )
    }} as id_unique, 
    src.school_year,
    cast(src.date_abs as date) as date_abs,
    datefromparts(year(src.date_abs), month(src.date_abs), 1) as debut_mois,
    src.fiche,
    concat(ele.nom, ', ', ele.pnom, ' (', src.fiche, ' )')  as nom_prenom_fiche,
    eco.school_friendly_name,
    src.groupe,
    src.grille,
    src.event_kind,
    case when  src.count_overdate_fiche_id_eco >1 then 'Absence mixte M-NM' else src.category_abs end as category_abs,
    case when  src.count_overdate_fiche_id_eco >1 then 'Motifs multiples' else src.event_description end as event_description,
    concat('étape : ', src.etape) as etape,
    src.date_debut,
    src.date_fin,
    src.id_eco,
    eco.eco
from {{ ref("cdpvd_fact_absences_daily") }} as src
join {{ ref("i_gpm_e_ele") }} as ele on src.fiche = ele.fiche
join {{ ref("dim_mapper_schools") }} as eco on src.id_eco = eco.id_eco
where
    school_year
    >= {{ core_dashboards_store.get_current_year() }}
    - {{ nbre_annee_a_extraire }}