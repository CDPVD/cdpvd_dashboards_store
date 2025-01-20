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
    Compute the grille / etape / day / eco padding table.
    
    Backfilled is done by / etape.
    Padding is needed to properly aggregate absences rate : if no absences has been recorded for a given day, the absences rate should be 0.
    Padding is done by / etape, / grille, / id_eco, / date_evenement, / event_kind to accound for various grid per school and allow number of aggregation leves

    To minimize computation, and to avoid having to expose unnecessarary rows to the downstream tables, backfilling the number of students is done in it's table too.
    backfilled and padded version of the daily number of students.    
#}
{{
    config(
        alias="cdpvd_stg_padding",
        post_hook=[
            core_dashboards_store.create_clustered_index(
                "{{ this }}", ["id_eco", "grille", "date_evenement"]
            ),
            core_dashboards_store.create_nonclustered_index(
                "{{ this }}", ["date_evenement"]
            ),
        ],
    )
}}

{% if execute %}
    {% if "nbre_annee_a_extraire" in var("dashboards")["absenteeism"] %}
        {% set nbre_annee_a_extraire = var("dashboards")["absenteeism"]["nbre_annee_a_extraire"] %}
        {{ log("Le nombre d'années de données à extraire pour le tableau de bord d'absentéisme est : " ~ nbre_annee_a_extraire, true) }}
    {% else %}
        {% set nbre_annee_a_extraire = 5 %}
        {{ log("Le nombre d'années de données à extraire pour le tableau de bord d'absentéisme est par défaut : " ~ nbre_annee_a_extraire, true) }}
    {% endif %}
    {% set years_of_data_absences = var("marts")["educ_serv"]["recency"]["years_of_data_absences"] %}
    {{ log("Le nombre d'années de données à extraire pour le comptoir d'absentéisme est : " ~ years_of_data_absences, true) }}
{% endif %}

-- Extract all the dates and grid to padd the data with
with
    padding_cal as (
        select
            id_eco,
            date_evenement,
            grille,
            max(case when jour_cycle is null then 0 else 1 end) as is_school_day
        from {{ ref("i_gpm_t_cal") }} as cal
        where
            date_evenement <= getdate() and id_eco in (select id_eco from {{ ref("cdpvd_fact_absences_daily") }})
            and year(date_evenement)
            >= {{ core_dashboards_store.get_current_year() }} - {{ nbre_annee_a_extraire }}
        group by id_eco, date_evenement, grille

    -- Extract all the absences event kind 
    ),
    kinds as (
        select distinct event_kind from {{ ref("cdpvd_fact_absences_daily") }}
    ),
    matieres as (
        select distinct id_eco, groupe, code_matiere from {{ ref("cdpvd_fact_absences_daily") }}

    -- Extract all the etapes per grid, eco and day
    ),
    etapes as (
        select
            id_eco,
			groupe,
            grille,
            etape,
            min(etape_date_debut) as etape_date_debut,
            max(etape_date_fin) as etape_date_fin
        from {{ ref("cdpvd_abstsm_stg_daily_students") }}
        group by id_eco, groupe, grille, etape

    -- Combine all the dimensions into one padding table (to rule them all and in the
    -- shadows bind them, tout ca, tout ca)
    ),
    padding as (
        select
            cal.id_eco,
            etp.groupe,
            cal.date_evenement,
            cal.grille,
            cal.is_school_day,
            mat.code_matiere,
			kind.event_kind,
            etp.etape,
            etp.etape_date_debut,
            etp.etape_date_fin
        from padding_cal as cal 
        cross join kinds as kind
        inner join etapes as etp on cal.id_eco = etp.id_eco and cal.grille = etp.grille
		inner join matieres as mat on cal.id_eco = mat.id_eco and mat.groupe = etp.groupe
    -- DO NOT restrict to the etape's date range. We want to backfill the data for the
    -- whole year.
    -- Add the daily number of students
    ),
    daily_unpadded as (
        select
            pad.id_eco,
            pad.date_evenement,
            pad.grille,
            pad.is_school_day,
            pad.event_kind,
            pad.code_matiere,
            pad.etape,
            pad.etape_date_debut,
            pad.etape_date_fin,
			pad.groupe,
            dly.n_students_daily
        from padding as pad
        left join
            {{ ref("cdpvd_abstsm_stg_daily_students") }} as dly
            on pad.id_eco = dly.id_eco
            and pad.date_evenement = dly.date_evenement
            and pad.grille = dly.grille
            and pad.etape = dly.etape
			and pad.groupe = dly.groupe

    -- Backfill the number of daily students
    ),
    backfilled as (
        select
            src.id_eco,
            src.date_evenement,
			src.groupe,
            src.grille,
            src.is_school_day,
            src.event_kind,
            src.code_matiere,
            src.etape,
            src.etape_date_debut,
            src.etape_date_fin,
            src.backfill_partition,
            src.n_students_daily as n_students_daily_ctrl,
            max(src.n_students_daily) over (
                partition by id_eco, grille, groupe, etape, backfill_partition
                order by date_evenement
                rows between unbounded preceding and unbounded following
            ) as n_students_daily
        from
            (
                select
                    id_eco,
                    date_evenement,
                    groupe,
                    grille,
                    is_school_day,
                    event_kind,
                    code_matiere,
                    etape,
                    etape_date_debut,
                    etape_date_fin,
                    n_students_daily,
                    sum(case when n_students_daily is not null then 1 else 0 end) over (
                        partition by id_eco, grille, groupe, etape
                        order by date_evenement
                        rows between unbounded preceding and current row
                    ) as backfill_partition
                from daily_unpadded
            ) as src

    -- Since the backfill is done, out of etape events can now be removed 
    )

select
    src.id_eco,
    src.date_evenement,
    DATENAME(WEEKDAY, date_evenement) AS jour_semaine,
    src.groupe,
    src.grille,
    src.is_school_day,
    src.event_kind,
    src.code_matiere,
    src.etape,
    src.etape_date_debut,
    src.etape_date_fin,
    src.n_students_daily
from backfilled as src
where src.date_evenement between src.etape_date_debut and src.etape_date_fin
