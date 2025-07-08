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
    Identify the students who are absent for more than 8 days
#}
{{ config(alias="cdpvd_report_bris_service") }}

{% if execute %}
    {% set dict = {
        "grp_rep": "Le groupe repère",
        "dist": "La distribution",
        "class": "La classification",
    } %}
    {% if "groupe_primaire" in var("dashboards")["absenteeism"] %}
        {% set groupe_primaire = var("dashboards")["absenteeism"]["groupe_primaire"] %}
        {{
            log(
                "Le groupe primaire sélectionné pour le tableau de bord d'absentéisme est : "
                ~ dict[groupe_primaire],
                info=True,
            )
        }}
    {% else %}
        {% set groupe_primaire = "grp_rep" %}
        {{
            log(
                'La variable "groupe_primaire" est par défaut : '
                ~ dict[groupe_primaire]
                ~ ", elle peut être modifié dans le dbt_project pour le tableau de bord d'absentéisme. Les possibilités disponibles sont : grp_rep, dist et class.",
                true,
            )
        }}
    {% endif %}

    {% if "groupe_secondaire" in var("dashboards")["absenteeism"] %}
        {% set groupe_secondaire = var("dashboards")["absenteeism"][
            "groupe_secondaire"
        ] %}
        {{
            log(
                "Le groupe secondaire sélectionné pour le tableau de bord d'absentéisme est : "
                ~ dict[groupe_secondaire],
                info=True,
            )
        }}
    {% else %}
        {% set groupe_secondaire = "dist" %}
        {{
            log(
                'La variable "groupe_secondaire" est par défaut : '
                ~ dict[groupe_secondaire]
                ~ ", elle peut être modifié dans le dbt_project pour le tableau de bord d'absentéisme. Les possibilités disponibles sont : grp_rep, dist et class.",
                true,
            )
        }}
    {% endif %}
{% endif %}

with
    src as (
        select
            fiche,
            id_eco,
            last_event_description,
            last_remarque,
            event_start_date,
            event_end_date,
            events_sequence_length,
            event_kind,
            coalesce(etape_description, 'inconnue') as etape_description
        from {{ ref("cdpvd_fact_absences_sequence") }}
        where
            school_year
            between {{ core_dashboards_store.get_current_year() }}
            - 1 and {{ core_dashboards_store.get_current_year() }}  -- Only consider the current school year

    -- Add some metadata to better identify the sutdent
    ),
    named as (
        select
            eco.school_friendly_name,
            eco.id_eco,
            eco.eco,
            eco.annee_scolaire,
            ele.fiche,
            concat(ele.nom, ' ', ele.pnom) as full_name,
            case
                when ordre_ens = 4
                then dan.{{ groupe_secondaire }}
                else dan.{{ groupe_primaire }}
            end as groupe,
            etape_description,
            event_start_date,
            event_end_date,
            events_sequence_length,
            event_kind,
            last_event_description,
            last_remarque
        from src
        join {{ ref("i_gpm_e_ele") }} as ele on src.fiche = ele.fiche
        join
            {{ ref("i_gpm_e_dan") }} as dan
            on src.fiche = dan.fiche
            and src.id_eco = dan.id_eco
        left join {{ ref("dim_mapper_schools") }} as eco on src.id_eco = eco.id_eco
    )

select
    school_friendly_name,
    annee_scolaire,
    fiche,
    full_name,
    coalesce(groupe, '-') as groupe,
    etape_description,
    cast(event_start_date as date) as event_start_date,
    cast(event_end_date as date) as event_end_date,
    case
        when events_sequence_length < 10
        then 'Moins de 10 jours'
        when events_sequence_length between 10 and 19
        then 'entre 10 et 19 jours'
        when events_sequence_length between 20 and 29
        then 'entre 20 et 29 jours'
        else 'plus de 30 jours'
    end as events_sequence_interval,
    events_sequence_length,
    event_kind,
    last_event_description,
    last_remarque,
    -- RLS hooks 
    id_eco,
    eco
from named
