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
                "{{ this }}", ["fiche", "id_eco",  "groupe"]
            ),
            core_dashboards_store.create_nonclustered_index("{{ this }}", ["fiche"]),
        ]
    )
}}

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


  -- aggregate the absences / retards by date of absence, student, school, type of event and subjects (only valid for the secondary)
with src as (
        select
            src.date_abs,
            src.fiche,
            src.id_eco,
            coalesce(dim.is_absence, 1) as is_absence,  -- Default to 0 if the absence is not qualified (prefer false positive over false negative)
            count(*) as n_periods_events,
            coalesce(min(dim.description_abs), 'inconnue') as event_description,  -- Take the first one, in lexicographic order. It's completely arbitrary ;) A better proxy would be the most common occurence
            coalesce(min(dim.category_abs), 'inconnue') as category_abs,
            min(src.remarque) as remarque
        from {{ ref("i_gpm_e_abs") }} as src
        inner join
            {{ ref("cdpvd_stg_dim_absences_inclusion") }} as dim
            on src.id_eco = dim.id_eco
            and src.motif_abs = dim.motif_abs
        group by
            src.date_abs,
            src.fiche,
            src.id_eco,
            dim.is_absence,
            category_abs

    -- Add the calendar grille the student follows from the DAN
    )
select
    src.date_abs,
    src.fiche,
    src.id_eco,
    case
        when ordre_ens = 4
        then dan.{{ groupe_secondaire }}
        else dan.{{ groupe_primaire }}
    end as groupe,
    dan.grille,
    src.is_absence,
    src.n_periods_events,
    category_abs,
    src.remarque,
    src.event_description
from src
join
    {{ ref("i_gpm_e_dan") }} as dan
    on src.fiche = dan.fiche
    and src.id_eco = dan.id_eco
