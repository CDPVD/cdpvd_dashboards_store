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
                "{{ this }}", ["fiche", "id_eco"] 
            ), 
            core_dashboards_store.create_nonclustered_index("{{ this }}", ["fiche"]), 
        ] 
    ) 
}} 

{% set max_periodes = var("interfaces")["gpi"]["max_periodes"] + 1 %}

{% if execute %}
    {% set dict = {
    'grp_rep': 'Le groupe repère',
    'dist': 'La distribution',
    'class': 'La classification'} %}
    {% if "groupe_primaire" in var("dashboards")["absenteeism"] %}
        {% set groupe_primaire = var("dashboards")["absenteeism"]["groupe_primaire"] %}
        {{ log("Le groupe primaire sélectionné pour le tableau de bord d'absentéisme est : " ~ dict[groupe_primaire], info=True) }}
    {% else %}
        {% set groupe_primaire = 'grp_rep' %}
        {{ log('La variable "groupe_primaire" est par défaut : ' ~ dict[groupe_primaire] ~ ', elle peut être modifié dans le dbt_project pour le tableau de bord d\'absentéisme. Les possibilités disponibles sont : grp_rep, dist et class.',true )
        }}
    {% endif %}

    {% if "groupe_secondaire" in var("dashboards")["absenteeism"] %}
        {% set groupe_secondaire = var("dashboards")["absenteeism"]["groupe_secondaire"] %}
        {{ log("Le groupe secondaire sélectionné pour le tableau de bord d'absentéisme est : " ~ dict[groupe_secondaire], info=True) }}
    {% else %}
        {% set groupe_secondaire = 'dist' %}
        {{ log('La variable "groupe_secondaire" est par défaut : ' ~ dict[groupe_secondaire] ~ ', elle peut être modifié dans le dbt_project pour le tableau de bord d\'absentéisme. Les possibilités disponibles sont : grp_rep, dist et class.',true )
        }}
    {% endif %}
{% endif %}

with
    -- Extract all the qualified absences / retards 
    matiere as (
		select --top 1000
			fct.date_abs,
            fct.fiche,
            fct.id_eco,
			fct.motif_abs,
			coalesce(mat, '-') as code_matiere
		from {{ ref("i_gpm_e_abs") }} as fct
		left join {{ ref("i_gpm_t_mat_grp") }} as mat 
            on fct.id_eco = mat.id_eco 
            and mat.id_mat_grp = fct.id_mat_grp
        --where fct.id_eco = '1102'
), -- aggregate the absences / retards by date of absence, student, school, type of event and subjects (only valid for the secondary)
    src as (
        select
            src.date_abs,
            src.fiche,
            src.id_eco,
            coalesce(src.code_matiere, 'Tout') as code_matiere,
            coalesce(dim.is_absence, 1) as is_absence,  -- Default to 0 if the absence is not qualified (prefer false positive over false negative)
            count(*) as n_periods_events,
            coalesce(min(dim.description_abs), 'inconnue') as event_description  -- Take the first one, in lexicographic order. It's completely arbitrary ;) A better proxy would be the most common occurence
        from matiere as src
        inner join
            {{ ref("cdpvd_stg_dim_absences_retards_inclusion") }} as dim
            on src.id_eco = dim.id_eco
            and src.motif_abs = dim.motif_abs
        group by src.date_abs, src.fiche, src.id_eco, dim.is_absence, cube( code_matiere)

    -- Add the calendar grille the student follows from the DAN
    ),
    src_with_grid_id as (
        select
            src.date_abs,
            src.fiche,
            src.id_eco,
            src.code_matiere,
            case when ordre_ens = 4 then dan.{{ groupe_secondaire }} else dan.{{ groupe_primaire }} end as groupe,
            dan.grille,
            src.is_absence,
            src.n_periods_events,
            src.event_description
        from src
        join
            {{ ref("i_gpm_e_dan") }} as dan
            on src.fiche = dan.fiche
            and src.id_eco = dan.id_eco

    -- Pre compute the expected daily number of periods per grid : later used to split
    -- days between the day of complete absence, and day of partial absence
    ),
    grid as (
        select
            id_eco,
            date_evenement,
            grille,
            DATENAME(WEEKDAY, date_evenement) AS jour_semaine,
            {% for i in range(1, max_periodes) %}
                case when max(per_{{ "%02d" % i }}) is null then 0 else 1 end
                {%- if not loop.last %} +{% endif -%}
            {% endfor %} as n_periods_expected
        from {{ ref("i_gpm_t_cal") }}
        where jour_cycle is not null  -- Only keep working days
        group by id_eco, date_evenement, grille

    -- Add the expected number of periods to the observed events
    ),
    src_with_expected_periodes as (
        select
            src.date_abs,
            src.fiche,
            src.id_eco,
            src.code_matiere,
            src.grille,
			src.groupe,
            grid.jour_semaine,
            src.is_absence,
            src.n_periods_events,
            grid.n_periods_expected,
            src.event_description,
            src.n_periods_events
            * 100.0
            / grid.n_periods_expected as prct_observed_periods_over_expected,
            -- Categorize the events based on : full-day / partial and absence / retard
            -- By construyction , the category is not nullable. The null case is
            -- outputed as test hook.
            case
                when src.n_periods_events >= grid.n_periods_expected  -- Schould logically be a strict = but a few students have more event than expected periods
                then
                    case
                        when src.is_absence = 1
                        then 'absence (journée complète)'
                        else null
                    end
                when src.n_periods_events < grid.n_periods_expected
                then
                    case
                        when src.is_absence = 1
                        then 'absence (période)'
                        else null
                    end
                else null
            end as event_kind
        from src_with_grid_id as src
        join
            grid
            on src.id_eco = grid.id_eco
            and src.date_abs = grid.date_evenement
            and src.grille = grid.grille
        where grid.n_periods_expected > 0  -- If no period is expected then we can't compute an absence rate.

    -- Add a 'tous types' category
    )
select
    date_abs,
    fiche,
    id_eco,
    code_matiere,
    grille,
    groupe,
    jour_semaine,
    is_absence,
    n_periods_events,
    n_periods_expected,
    event_description,
    prct_observed_periods_over_expected,
    event_kind
from src_with_expected_periodes
