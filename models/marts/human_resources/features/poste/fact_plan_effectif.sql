
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

with tab as (
    select
        concat(p.corp_empl,'-',p.no_seq_post) as poste
        , p.no_seq_post
        , p.id_post
        , p.corp_empl 
        , ce.descr as emploi_descr
        , ce.nb_hres_an                                                 -- pertinent??
        , ce.nb_hres_an * p.pourc / 100. as nbhrsan_pond                -- pertinent??
        , p.caract_post 
        , cp.descr as descr_caract
        , p.plan_eff 
        , dplan.cf_descr as descr_plan_eff
        , p.an_budg
        , p.type_poste as type_poste
        , dtypep.cf_descr as descr_type_poste
        , p.pourc / 100. as etc
        , p.date_eff 
        , p.date_fin
        , p.date_creat
        , row_number() over (partition by p.no_seq_post, p.corp_empl, p.an_budg order by p.date_eff desc ) as seq_id
        , case when cast('{{ run_started_at.strftime("%Y-%m-%d") }}' as date) between p.date_eff and p.date_fin then 1 else 0 end as is_open
    from {{ ref('i_grh_poste') }} as p
    left join {{ ref('i_grh_tab_param_caract_post') }} as cp
        on cp.caract_post = p.caract_post
    left join {{ ref('i_pai_tab_corp_empl') }} as ce
        on ce.corp_empl = p.corp_empl
    left join {{ ref('i_wl_descr_paie') }} as dplan
        on dplan.nom_table = 'plan_eff' and dplan.code = p.plan_eff
    left join {{ ref('i_wl_descr_paie') }} as dtypep
        on dtypep.nom_table = 'type_poste' and dtypep.code = p.type_poste
)

select 
    poste
    , left(corp_empl, 1) as cod_secteur
    , case
        when left(corp_empl, 1) = '1' then 'Personnel hors-cadres et cadres, personnel de gérance'
        when left(corp_empl, 1) = '2' then 'Personnel professionnel'
        when left(corp_empl, 1) = '3' then 'Personnel enseignant (FGJ - FGA - FP)'
        when left(corp_empl, 1) = '4' then 'Personnel administratif et personnel technique'
        when left(corp_empl, 1) = '5' then 'Personnel ouvrier'
        else NULL
    end as secteur
    , no_seq_post
    , id_post
    , corp_empl 
    , emploi_descr
    , nb_hres_an
    , nbhrsan_pond
    , caract_post 
    , descr_caract
    , plan_eff 
    ,  descr_plan_eff
    , left(an_budg, 4) as annee
    , an_budg
    , type_poste
    , descr_type_poste
    , etc
    , date_eff 
    , date_fin
    , date_creat
    , seq_id
    , is_open
from tab