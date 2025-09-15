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

with reel as (
    select
        hrs.an_budg
        , hrs.no_per
        , hrs.date_cheq
        , job.job_group_category as cat_emploi
        , concat(hrs.corp_emploi, ' - ', ptce.descr) as corp_emploi
        , concat(hrs.lieu_trav, ' - ', lieu.descr) as lieu_trav
        , concat(hrs.stat_eng, ' - ', eng.descr_stat_eng) as stat_eng
        , case 
            when hrs.typeremun = '1' then 'Traitement régulier'
            when hrs.typeremun = '2' then 'Absences'	 
            when hrs.typeremun = '3' then 'Temps supplémentaire'
        end as typeremun_descr
        , hrs.nb_hre_remun_fin
    from  {{ ref('fact_h_remun') }} as hrs
    join {{ ref('dim_mapper_job_group') }} as job
        on job.job_group = hrs.corp_emploi
    join {{ ref('i_pai_tab_corp_empl') }} as ptce
        on ptce.corp_empl = hrs.corp_emploi
    join {{ ref('i_pai_tab_stat_eng') }} as eng
        on eng.stat_eng = hrs.stat_eng
    join {{ ref('i_pai_tab_lieu_trav') }} as lieu
        on lieu.lieu_trav = hrs.lieu_trav    

-- sommer le tout    
), tot as (
    select
        an_budg
        , no_per
        , date_cheq
        , cat_emploi
        , corp_emploi
        , lieu_trav
        , stat_eng
        , typeremun_descr
        , sum(nb_hre_remun_fin) as nombre_heures_remun
    from reel
    group by an_budg, no_per, date_cheq, cat_emploi, corp_emploi, lieu_trav, stat_eng, typeremun_descr
)

select
    an_budg
    , no_per
    , min(date_cheq) as date_cheq
    , cat_emploi
    , corp_emploi
    , lieu_trav
    , stat_eng
    , typeremun_descr
    , sum(nombre_heures_remun) as nombre_heures_remun
from tot
group by
    an_budg
    , cube (no_per, cat_emploi, corp_emploi, lieu_trav, stat_eng, typeremun_descr)
