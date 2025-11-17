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
            coalesce(job.job_group_category, '-') as cat_emploi,
            coalesce(concat(hrs.corp_emploi, ' - ', ptce.descr), '-') as corp_emploi,
            coalesce(concat(ua.new_lieu_trav, ' - ', ua.descr), '-') as lieu_trav,
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
            hrs.nb_hre_remun_dist
        from {{ ref("fact_h_remun") }} as hrs
        left join
            {{ ref("stg_nomen_unit_adm") }} as ua
            on ua.exer_fin = hrs.an_budg
            and ua.current_lieu_trav = hrs.lieu_trav_cpt_budg
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
            cat_emploi,
            corp_emploi,
            lieu_trav,
            stat_eng,
            type_remun,
            round(sum(nb_hre_remun_dist), 2) as nombre_heures_remun
        from reel
        group by
            an_budg, no_per, cat_emploi, corp_emploi, lieu_trav, stat_eng, type_remun

    -- agreger selon les differentes combinaisons
    ),
    cube_agg as (
        select
            an_budg,
            no_per,
            coalesce(cat_emploi, 'Tout') as cat_emploi,
            coalesce(corp_emploi, 'Tout') as corp_emploi,
            coalesce(lieu_trav, 'Tout') as lieu_trav,
            coalesce(stat_eng, 'Tout') as stat_eng,
            coalesce(type_remun, 'Tout') as type_remun,
            sum(nombre_heures_remun) as nombre_heures_remun
        from tot
        group by
            an_budg,
            no_per, cube (cat_emploi, corp_emploi, lieu_trav, stat_eng, type_remun)

    -- ajout d'un cumul progressif
    ),
    cum as (
        select
            an_budg,
            no_per,
            cat_emploi,
            corp_emploi,
            lieu_trav,
            stat_eng,
            type_remun,
            nombre_heures_remun,
            sum(nombre_heures_remun) over (
                partition by
                    an_budg, cat_emploi, corp_emploi, lieu_trav, stat_eng, type_remun
                order by cast(no_per as int)
                rows between unbounded preceding and current row
            ) as cumul_progressif
        from cube_agg

    )

select
    an_budg,
    no_per,
    cat_emploi,
    corp_emploi,
    lieu_trav,
    stat_eng,
    type_remun,
    nombre_heures_remun,
    cumul_progressif
from cum
