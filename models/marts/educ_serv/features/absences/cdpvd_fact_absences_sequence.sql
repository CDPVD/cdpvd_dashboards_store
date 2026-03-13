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
    Compute the sequence of days with at least one periode of absence.
#}
{{
    config(
        post_hook=[
            core_dashboards_store.create_clustered_index(
                "{{ this }}", ["id_eco",  "fiche"]
            ),
            core_dashboards_store.create_nonclustered_index("{{ this }}", ["fiche"]),
        ]
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


-- Extract all the days a student is expected to be there 
with
    expected_cal as (
        select
            case
                when month(date_evenement) < 7
                then year(date_evenement) - 1
                else year(date_evenement)
            end as school_year,
            id_eco,
            grille,
            date_evenement
        from {{ ref("i_gpm_t_cal") }} as cal
        where jour_cycle is not null

    -- Add a sequence id : day_id to later identify the break between two sequences of
    -- absences
    ),
    expected_cal_with_id as (
        select
            id_eco,
            grille,
            date_evenement,
            row_number() over (
                partition by id_eco, grille order by date_evenement
            ) as day_id
        from expected_cal
        where date_evenement <= getdate() 
            and school_year
            >= {{ core_dashboards_store.get_current_year() }}
            - {{ nbre_annee_a_extraire }}
    -- Left join the observed absences on the calendar
    ),
    observed as (
        select distinct
            exp.id_eco,
            exp.date_evenement,
            exp.day_id,
            exp.grille,
            abs.fiche,
			abs.category_abs,            
            abs.event_kind,
            abs.event_description,
            abs.remarque,
            abs.etape,
            abs.etape_description,
            abs.seq_etape
        from expected_cal_with_id as exp
        inner join
            {{ ref("cdpvd_fact_absences_daily") }} as abs
            on exp.id_eco = abs.id_eco
            and exp.date_evenement = abs.date_abs
            and exp.grille = abs.grille

    ),
    /* 3) Niveau JOUR (clé: fiche + date_evenement).
        Une ligne par jour/élève pour détecter les ruptures GLOBALes,
        même si plusieurs event_kind existent le même jour. */
        observed_daylevel as (
            select 
                id_eco,
                date_evenement,
                day_id,
                fiche,
                max(etape) as etape,
                max(etape_description) as etape_description,
                max(seq_etape) as seq_etape,
                max(event_description) as event_description,
                max(remarque) as remarque                
            from observed
    		group by id_eco, date_evenement, day_id, fiche

    ) 
    ,
    -- 4) Ruptures PAR TYPE (comme chez toi, mais on va aussi produire la version globale)
    breaks_by_kind as (
        select
            id_eco,
            date_evenement,
            day_id,
            fiche,
            event_kind,
			category_abs,
            event_description,
            remarque,
            etape,
            etape_description,
            seq_etape,
            case
                when day_id - lag(day_id) over (
                        partition by  id_eco, fiche, event_kind
                        order by day_id
                    ) > 1 then 1 else 0
            end as sequence_break
        from observed
    ),
    breaks_all as (
        select
            id_eco,
            date_evenement,
            day_id,
            fiche,
            event_description,
            remarque,
            etape,
            etape_description,
            seq_etape,            
            case
                when day_id - lag(day_id) over (
                        partition by id_eco, fiche
                        order by day_id
                    ) > 1 then 1 else 0
            end as sequence_break_all
        from observed_daylevel
    ),

    -- 5) Identifiants de séquence
    sequences_by_kind as (
        select
            id_eco,
            date_evenement,
            day_id,
            fiche,
            event_kind,
            category_abs,
            event_description,
            remarque,
            etape,
            etape_description,
            seq_etape,
            sum(sequence_break) over (
                partition by id_eco, fiche, event_kind ,category_abs
                order by day_id
                rows between unbounded preceding and current row
            ) as absence_sequence_id_kind
        from breaks_by_kind
    ),

    sequences_all as (
        select
            id_eco,
            date_evenement,
            event_description,
            remarque,
            day_id,
            fiche,
            etape,
            etape_description,
            seq_etape,
            sum(sequence_break_all) over (
                partition by id_eco, fiche
                order by day_id
                rows between unbounded preceding and current row
            ) as absence_sequence_id_all
        from breaks_all
    ),

    -- 6) Contextualisation (dernière description/remarque) – PAR TYPE
    context_by_kind as (
        select
            s.id_eco,
            s.date_evenement,
            s.day_id,
            s.fiche,
            s.event_kind,
            s.category_abs,
            s.etape,
            s.etape_description,
            s.seq_etape,
            last_value(s.event_description) over (
                partition by s.fiche, s.id_eco, s.event_kind, s.category_abs, s.absence_sequence_id_kind
                order by s.day_id
                rows between unbounded preceding and unbounded following
            ) as last_event_description,
            last_value(s.remarque) over (
                partition by s.fiche, s.id_eco, s.event_kind, s.category_abs, s.absence_sequence_id_kind
                order by s.day_id
                rows between unbounded preceding and unbounded following
            ) as last_remarque,
            s.absence_sequence_id_kind as absence_sequence_id,
            'by_kind' as sequence_scope
        from sequences_by_kind as s
    ),

    -- 7) Contextualisation – GLOBALE (au-delà des event_kind)
    --    On rattache la dernière description/remarque du jour (s'il y en a plusieurs, on prend celle du "dernier" day_id de la séquence).

        -- on récupère, pour chaque jour/élève, une description/remarque "du jour"
        day_context as (
            select
                sa.id_eco,
                sa.date_evenement,
                sa.day_id,
                sa.fiche,
                -- Heuristique: dernière description/remarque du jour (sur les lignes observées)
                last_value(sa.event_description) over (
                    partition by sa.id_eco, sa.fiche, sa.absence_sequence_id_all
                    order by sa.day_id  -- ordre quelconque, on étend à tout le jour
                    rows between unbounded preceding and unbounded following
                ) as day_event_description,
                last_value(sa.remarque) over (
                    partition by  sa.id_eco, sa.fiche, sa.absence_sequence_id_all
                    order by sa.day_id
                    rows between unbounded preceding and unbounded following
                ) as day_remarque,
                max(sa.etape) over (partition by sa.id_eco, sa.fiche, sa.date_evenement) as etape, -- agrégations "dummy"
                max(sa.etape_description) over (partition by sa.id_eco, sa.fiche, sa.date_evenement) as etape_description,
                max(sa.seq_etape) over (partition by sa.id_eco, sa.fiche, sa.date_evenement) as seq_etape
            from sequences_all sa
        )

    ,context_all as (

        select
            sa.id_eco,
            sa.date_evenement,
            sa.day_id,
            sa.fiche,
            null as event_kind,  -- on ne garde pas le type pour le scope global
            null as category_abs,  -- on ne garde pas la catégorie pour le scope global
            dc.etape,
            dc.etape_description,
            dc.seq_etape,
            dc.day_event_description as last_event_description,
            dc.day_remarque as last_remarque,
            sa.absence_sequence_id_all as absence_sequence_id,
            'all' as sequence_scope
        from sequences_all sa
        join day_context dc
            on sa.id_eco      = dc.id_eco
            and sa.fiche       = dc.fiche
            and sa.date_evenement = dc.date_evenement

    ),

    -- 8) Union des deux portées
    context_union as (
        select * from context_by_kind
        union all
        select * from context_all

    ),

    -- 9) Agrégation au niveau séquence
    aggregated as (
        select
            fiche,
            id_eco,
            sequence_scope,
            absence_sequence_id,
            coalesce(event_kind, 'Tout') as event_kind,
			coalesce(category_abs, 'Tout') as category_abs, 
            min(last_event_description) as last_event_description,  -- dummy agg
            min(last_remarque)        as last_remarque,            -- dummy agg
            min(date_evenement)       as event_start_date,
            max(date_evenement)       as event_end_date,
            max(day_id) - min(day_id) + 1 as events_sequence_length,
            min(etape)              as etape,
            min(etape_description)  as etape_description,
            min(seq_etape)          as seq_etape
        from context_union
        group by fiche, id_eco, sequence_scope, absence_sequence_id, coalesce(event_kind, 'Tout'), coalesce(category_abs, 'Tout')  
    )

    -- 10) Filtre années scolaires et projection (comme chez toi)

select
    fiche,
    id_eco,
    last_event_description,
    last_remarque,
    event_start_date,
    event_end_date,
    events_sequence_length,
    event_kind,
    category_abs,
    sequence_scope,
    coalesce(etape_description, 'inconnue') as etape_description
from aggregated 