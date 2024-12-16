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
                "{{ this }}", ["fiche", "id_eco", "school_year","groupe"] 
            ), 
            core_dashboards_store.create_nonclustered_index("{{ this }}", ["fiche"]), 
        ] 
    ) 
}} 

with
    src as (
        select
            case
                when month(date_abs) <= 7 then year(date_abs) - 1 else year(date_abs)
            end as school_year,
            date_abs,
            fiche,
            id_eco,
			groupe,
            max(jour_semaine) as jour_semaine,
            max(grille) as grille,  -- dymmy aggregation. Already controlled by the tuple (id_eco, fiche)
            coalesce(event_kind, 'tous types') as event_kind,
            case when event_kind is null then 1 else 0 end as is_aggregate_kind,  -- To flag the 'tous types' category
            -- By additivity of absences / retards : two differents events can't be
            -- registered for the same period
            case
                when event_kind is null then null else min(is_absence)
            end as is_absence,
            min(event_description) as event_description, -- arbitrary : first description in lexicographic order
            {# case
                when event_kind is null then 'tous types' else min(event_description)
            end as event_description,   #}
            sum(
                prct_observed_periods_over_expected
            ) as prct_observed_periods_over_expected
        from {{ ref("cdpvd_stg_absences") }}
        where event_kind = 'absence (journée complète)' and code_matiere = 'Tout'
        group by date_abs, fiche, id_eco, groupe, event_kind  -- Superseed is_absence

    -- Handle the weird case where 0.0001% of students have more observed periods of
    -- absences than
    -- expected periods
    ),
    corrected as (
        select
            school_year,
            date_abs,
            fiche,
            id_eco,
            grille,
			groupe,
            jour_semaine,
            event_kind,
            is_aggregate_kind,
            is_absence,
            event_description,
            case
                when prct_observed_periods_over_expected > 100.0
                then 100.0
                else prct_observed_periods_over_expected
            end as prct_observed_periods_over_expected
        from src
    )

-- Add the etape
select
    src.school_year,
    src.date_abs,
    src.fiche,
    src.id_eco,
    src.grille,
	groupe,
    src.jour_semaine,
    src.event_kind,
    src.is_aggregate_kind,
    src.is_absence,
    src.event_description,
    etp.etape,
    etp.etape_description,
    etp.seq_etape
from corrected as src
left join
    {{ ref("stg_fact_fiche_etapes") }} as etp
    on src.fiche = etp.fiche
    and src.id_eco = etp.id_eco
    and src.date_abs between etp.date_debut and etp.date_fin
