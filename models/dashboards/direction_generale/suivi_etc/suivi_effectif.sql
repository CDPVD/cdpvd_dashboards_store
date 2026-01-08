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
            no_cmpt,
            cast(hrs.no_per as int) as no_per,
            coalesce(job.job_group_category, '-') as cat_emploi,
            coalesce(concat(hrs.corp_emploi, ' - ', ptce.descr), '-') as corp_emploi,
            coalesce(
                concat(ua.new_lieu_trav, ' - ', ua.descr), '-'
            ) as descr_unite_admin,
            ua.new_lieu_trav as unite_admin,
            hrs.lieu_trav as code_lieu_trav,
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
            and ua.current_lieu_trav = hrs.unite_admin
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
            no_cmpt,
            cat_emploi,
            corp_emploi,
            unite_admin,
            code_lieu_trav,
            descr_unite_admin,
            stat_eng,
            type_remun,
            sum(nb_hre_remun_dist) as nombre_heures_remun,
            sum((nb_hre_remun_dist / 1826.3)) as equivalent_temps_plein
        from reel
        group by
            an_budg,
            no_per,
            no_cmpt,
            cat_emploi,
            corp_emploi,
            unite_admin,
            code_lieu_trav,
            descr_unite_admin,
            stat_eng,
            type_remun
    )
select
    an_budg,
    no_per,
    no_cmpt,
    cat_emploi,
    corp_emploi,
    code_lieu_trav,
    unite_admin,
    descr_unite_admin,
    stat_eng,
    type_remun,
    nombre_heures_remun,
    equivalent_temps_plein
from tot
