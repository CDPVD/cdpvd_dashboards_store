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
		p.id_affect
		, concat(p.corp_empl, '-', p.no_seq_post) as poste
		, p.corp_empl
		, p.no_seq_post
		, p.date_eff
		, p.date_fin
		, p.matr
		, p.ref_empl
		, p.type_aff
		, p.lieu_trav
		, p.pourc
		, p.motif_aff
		, m.descr
		, p.date_creat
		, p.date_dern_maj
	from {{ ref('i_grh_poste_affect') }} as p
	left join {{ ref('i_grh_tab_motif_aff') }} m
		on p.motif_aff = m.motif_aff

-- Ajout d'un champs next_date_eff pour éviter les chevauchements et definir borne de fin à chaque ligne
), tab2 as (
	select 
		id_affect
		, poste
		, corp_empl
		, no_seq_post
		, date_eff
		, date_fin
		, lag(date_eff) over (partition by poste, lieu_trav, matr order by date_eff desc, date_dern_maj desc) as next_date_eff
		, matr
		, ref_empl
		, type_aff
		, lieu_trav
		, pourc
		, motif_aff
		, descr
		, date_creat
		, date_dern_maj
	from tab

-- Définir une borne de fin corrigée qui est la plus petite entre date_fin et next_date_eff - 1
), bornes as (
    select 
        *
        , case
            when next_date_eff is not null and date_fin is not null then 
                case 
					when dateadd(day, -1, next_date_eff) < date_fin then dateadd(day, -1, next_date_eff) 
					else date_fin 
				end
           	when next_date_eff is not null then dateadd(day, -1, next_date_eff)
            else coalesce(date_fin, cast('2079-06-06' as date))
        end as date_fin_corr
    from tab2
)

select 
	id_affect
	, poste
	, corp_empl
	, no_seq_post
	, date_eff
	, date_fin
	--, date_fin_corr
	, matr
	, ref_empl
	, type_aff
	, lieu_trav
	, pourc
	, motif_aff
	, descr
	, date_creat
	, date_dern_maj
	, case when cast('{{ run_started_at.strftime("%Y-%m-%d") }}' as date) between date_eff and date_fin_corr then 1 else 0 end as current_post
from bornes