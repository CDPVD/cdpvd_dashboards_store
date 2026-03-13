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
    Table de remplissage (padding) pour l'absentéisme.
    
    Crée une table complète avec toutes les combinaisons de (établissement, 
    étape, jour, groupe) pour garantir que les jours sans absence 
    soient comptés comme 0% et non omis. Remplit aussi le nombre d'étudiants
    de façon rétroactive pour chaque partition.
#}
{{
    config(
        alias="cdpvd_stg_padding",
        post_hook=[
            core_dashboards_store.create_clustered_index(
                "{{ this }}", ["id_eco", "date_evenement"]
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
-- ÉTAPE 1: Extraction du calendrier pour le remplissage des données
-- ============================================================================
with
    padding_cal as (
        select
            cal.id_eco,
            date_evenement,
            max(case when jour_cycle is null then 0 else 1 end) as is_school_day
        from {{ ref("i_gpm_t_cal") }} as cal
        join {{ ref("i_gpm_t_eco") }} as eco on cal.id_eco = eco.id_eco
        where
            date_evenement <= getdate()
            and cal.id_eco
            in (select id_eco from {{ ref("cdpvd_fact_absences_daily") }})
            and annee
            >= {{ core_dashboards_store.get_current_year() }}
            - {{ nbre_annee_a_extraire }}
        group by cal.id_eco, date_evenement

    -- ============================================================================
    -- ÉTAPE 2: Extraction des étapes par établissement et jour
    -- ============================================================================
    ),
    etapes as (
        select
            id_eco,
            groupe,
            etape,
            min(etape_date_debut) as etape_date_debut,
            max(etape_date_fin) as etape_date_fin
        from {{ ref("cdpvd_abstsm_stg_daily_students") }}
        group by id_eco, groupe, etape

    -- ============================================================================
    -- ÉTAPE 3: Combinaison de toutes les dimensions dans une table de remplissage
    -- ============================================================================
    ),
    padding as (
        select
            cal.id_eco,
            etp.groupe,
            cal.date_evenement,
            cal.is_school_day,
            etp.etape,
            etp.etape_date_debut,
            etp.etape_date_fin
        from padding_cal as cal
        inner join etapes as etp on cal.id_eco = etp.id_eco
    ),
    daily_unpadded as (
        -- ====================================================================
        -- ÉTAPE 4: Ajout du nombre quotidien d'étudiants à la table de remplissage
        -- ====================================================================
        select
            pad.id_eco,
            pad.date_evenement,
            pad.is_school_day,
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
            and pad.etape = dly.etape
            and pad.groupe = dly.groupe
    ),
    first_known as (
        select
            id_eco,
            groupe,
            min(
                case when n_students_daily is not null then date_evenement end
            ) as first_known_date
        from daily_unpadded
        group by id_eco, groupe
    ),
    first_value as (
        select
            d.id_eco,
            d.groupe,
            fk.first_known_date,
            max(d.n_students_daily) as first_known_value
        from daily_unpadded d
        join
            first_known fk
            on fk.id_eco = d.id_eco
            and fk.groupe = d.groupe
            and d.date_evenement = fk.first_known_date
        group by d.id_eco, d.groupe, fk.first_known_date
    ),
    daily_prefilled as (
        select
            d.*,
            case
                when
                    d.n_students_daily is null
                    and fv.first_known_date is not null
                    and d.date_evenement
                    between d.etape_date_debut and fv.first_known_date
                then fv.first_known_value
                else d.n_students_daily
            end as n_students_daily_prefilled
        from daily_unpadded d
        left join first_value fv on fv.id_eco = d.id_eco and fv.groupe = d.groupe

    ),
    backfilled as (
        -- ====================================================================
        -- ÉTAPE 5: Remplissage (backfill) du nombre quotidien d'étudiants
        -- ====================================================================
        -- Propagation de la dernière valeur connue du nombre d'étudiants
        -- pour chaque partition (établissement, classe, étape)
        select
            src.id_eco,
            src.date_evenement,
            src.groupe,
            src.is_school_day,
            src.etape,
            src.etape_date_debut,
            src.etape_date_fin,
            src.backfill_partition,
            max(src.n_students_daily_prefilled) over (
                partition by id_eco, groupe, backfill_partition
                order by date_evenement
                rows between unbounded preceding and unbounded following
            ) as n_students_daily
        from
            (
                select
                    id_eco,
                    date_evenement,
                    groupe,
                    is_school_day,
                    etape,
                    etape_date_debut,
                    etape_date_fin,
                    n_students_daily_prefilled,
                    sum(case when n_students_daily is not null then 1 else 0 end) over (
                        partition by id_eco, groupe
                        order by date_evenement
                        rows between unbounded preceding and current row
                    ) as backfill_partition
                from daily_prefilled
            ) as src

    )

-- ============================================================================
-- ÉTAPE 6: Sélection finale avec filtrage par date d'étape
-- ============================================================================
-- Récupération des données complètes en excluant les lignes en dehors
-- de la plage de dates des étapes et ajout du jour de la semaine
select
    src.id_eco,
    src.date_evenement,
    datename(weekday, date_evenement) as jour_semaine,
    src.groupe,
    src.is_school_day,
    src.etape,
    src.etape_date_debut,
    src.etape_date_fin,
    src.n_students_daily
from backfilled as src
where src.date_evenement between src.etape_date_debut and src.etape_date_fin
