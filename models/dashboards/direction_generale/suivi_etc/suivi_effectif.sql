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
with
    reel as (
        select
            concat(left(hrs.an_budg, 4), '-', right(hrs.an_budg, 4)) an_budg,
            coalesce(cast(hrs.no_per as varchar), '-') as no_per,
            coalesce(hrs.date_cheq, '-') as date_cheq,
            coalesce(job.job_group_category, '-') as cat_emploi,
            coalesce(concat(hrs.corp_emploi, ' - ', ptce.descr), '-') as corp_emploi,
            coalesce(hrs.lieu_trav, '-') as lieu_trav,
            coalesce(concat(hrs.stat_eng, ' - ', eng.descr_stat_eng), '-') as stat_eng,
            case
                when hrs.typeremun = '1'
                then 'Traitement régulier'
                when hrs.typeremun = '2'
                then 'Absences'
                when hrs.typeremun = '3'
                then 'Temps supplémentaire'
                else '-'
            end as type_remun,
            hrs.nb_hre_remun_fin
        from {{ ref("fact_h_remun") }} as hrs
        join {{ ref("dim_mapper_job_group") }} as job on job.job_group = hrs.corp_emploi
        join
            {{ ref("i_pai_tab_corp_empl") }} as ptce on ptce.corp_empl = hrs.corp_emploi
        join {{ ref("i_pai_tab_stat_eng") }} as eng on eng.stat_eng = hrs.stat_eng

    -- sommer le tout    
    ),
    tot as (
        select
            an_budg,
            no_per,
            date_cheq,
            cat_emploi,
            corp_emploi,
            lieu_trav,
            stat_eng,
            type_remun,
            sum(nb_hre_remun_fin) as nombre_heures_remun
        from reel
        group by
            an_budg,
            no_per,
            date_cheq,
            cat_emploi,
            corp_emploi,
            lieu_trav,
            stat_eng,
            type_remun

    -- agreger selon les differentes combinaisons
    ),
    cube_agg as (
        select
            an_budg,
            coalesce(no_per, 'Tout') as no_per,
            coalesce(corp_emploi, 'Tout') as corp_emploi,
            coalesce(lieu_trav, 'Tout') as lieu_trav,
            coalesce(stat_eng, 'Tout') as stat_eng,
            coalesce(type_remun, 'Tout') as type_remun,
            sum(nombre_heures_remun) as nombre_heures_remun
        from tot
        group by an_budg, cube (no_per, corp_emploi, lieu_trav, stat_eng, type_remun)

    -- ajout d'un cumul progressif
    ),
    cum as (
        select
            an_budg,
            no_per,
            corp_emploi,
            lieu_trav,
            stat_eng,
            type_remun,
            nombre_heures_remun,
            case
                when no_per <> 'Tout'
                then
                    sum(nombre_heures_remun) over (
                        partition by
                            an_budg, corp_emploi, lieu_trav, stat_eng, type_remun
                        order by
                            case
                                when no_per in ('Tout', '-')
                                then 9999
                                else cast(no_per as int)
                            end
                        rows between unbounded preceding and current row
                    )
            end as cumul_progressif
        from cube_agg

    -- rajout des colonnes date_cheque, cat_emploi
    ),
    map as (
        select distinct an_budg, no_per, date_cheq, corp_emploi, cat_emploi
        from tot
        union all
        -- ligne "Totale" pour chaque période / année
        select
            an_budg,
            no_per,
            max(date_cheq) as date_cheq,
            'Tout' as corp_emploi,
            'Tout' as cat_emploi
        from tot
        group by an_budg, no_per
    )

select
    cum.an_budg,
    cum.no_per,
    map.date_cheq,
    map.cat_emploi,
    cum.corp_emploi,
    cum.lieu_trav as cod_lieu_trav,
    case
        when lieu.descr is not null
        then concat(cum.lieu_trav, ' - ', lieu.descr)
        else cum.lieu_trav
    end as lieu_trav,
    cum.stat_eng,
    cum.type_remun,
    cum.nombre_heures_remun,
    cum.cumul_progressif
from cum
join
    map
    on map.an_budg = cum.an_budg
    and map.no_per = cum.no_per
    and map.corp_emploi = cum.corp_emploi
left join {{ ref("i_pai_tab_lieu_trav") }} as lieu on lieu.lieu_trav = cum.lieu_trav
