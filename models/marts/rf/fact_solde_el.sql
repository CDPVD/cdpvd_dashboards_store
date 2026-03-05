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
	-- soldes GPI
	soldes_gpi as (
		select 
			el.code_perm 
			, f.empr as fiche
			, eco.annee
			, eco.eco
			, sum(case when f.motif_fact in ('F','V') then f.solde else 0 end) as car_gpi
			, sum(case when f.motif_fact = 'A' then f.solde else 0 end) * -1 as trp_gpi
        from {{ ref("i_gpm_n_fact") }} as f 
		left join {{ ref("i_gpm_t_eco") }} as eco 
			on eco.id_eco = f.id_eco
		left join {{ ref("i_gpm_e_ele") }} as el 
			on el.fiche = f.empr
		where 
			eco.annee between {{ core_dashboards_store.get_current_year() }}-15 and {{ core_dashboards_store.get_current_year() }}
			and f.type_empr = 'E'
		group by el.code_perm, f.empr, eco.annee, eco.eco
	
	-- car AG
	), car_ag as (
		select 
			el.code_perm
			, car.fiche
			, car.annee
			, bat_sdg.eco
			, isnull(sum(car.solde), 0.0) as car_ag
        from {{ ref("i_sdg_e_fact") }} as car
		left join {{ ref("i_sdg_e_ele") }} as el
			on el.fiche = car.fiche
		left join {{ ref("mapping_bat_sdg_eco") }} as bat_sdg 
			on bat_sdg.id_sdg = car.id_sdg
		where 
			car.annee between {{ core_dashboards_store.get_current_year() }}-15 and {{ core_dashboards_store.get_current_year() }}
		group by el.code_perm, car.fiche, car.annee, bat_sdg.eco
	
	-- tp AG
	), tp_ag as (
		select 
			el.code_perm
			, tp.fiche
			, tp.annee
			, bat_sdg.eco
			, isnull(sum(tp.mnt), 0.0) as tp_ag
        from {{ ref("i_sdg_e_trop_percus") }} as tp
		left join {{ ref("i_sdg_e_ele") }} as el 
			on el.fiche = tp.fiche
		left join {{ ref("mapping_bat_sdg_eco") }} as bat_sdg
			on bat_sdg.id_sdg = tp.id_sdg
		where 
			tp.annee between {{ core_dashboards_store.get_current_year() }}-15 and {{ core_dashboards_store.get_current_year() }}
		group by el.code_perm, tp.fiche, tp.annee, bat_sdg.eco

	-- car tp PROCURE + recuperer les ecoles associées aux eleves inscrits en FP/FGA
	), car_tp_proc as (
		select
			car_tp_proc.code_perm,
			car_tp_proc.fiche, 
			car_tp_proc.annee,
			coalesce(freq.eco_cen , car_tp_proc.eco) as eco, -- privilégier l'eco de la freq si dispo
			car_tp_proc.car_proc,
			car_tp_proc.trp_proc
		from {{ ref("car_tp_procure") }} as car_tp_proc
		left join {{ ref("stg_populations_adultes") }} as pop
			on pop.code_perm = car_tp_proc.code_perm and pop.fiche = car_tp_proc.fiche and pop.annee = car_tp_proc.annee 
		left join {{ ref("i_e_freq_adultes") }} as freq
			on freq.fiche = pop.fiche and freq.annee = pop.annee and freq.freq = pop.freq
		where 
			car_tp_proc.annee between {{ core_dashboards_store.get_current_year() }}-15 and {{ core_dashboards_store.get_current_year() }}
	
	-- perimetre final FGJ + FGA
    ), perim as (
        select 
            code_perm,
            fiche,
            annee,
            eco
        from soldes_gpi
        union
        select 
            code_perm,
            fiche,
            annee,
            eco
        from car_ag
        union
        select 
            code_perm,
            fiche,
            annee,
            eco
        from tp_ag
        union
        select 
            code_perm,
            cast(fiche as varchar(7)) as fiche,
            annee,
            eco
		from car_tp_proc
	)     

-- REQUETE FINALE
select 
    perim.code_perm,
	string_agg(perim.fiche, ', ') AS fiche, -- pour considerer les eleves avec 1 code_perm, 2 fiches la meme annee
    perim.annee,
    perim.eco
	-- GPI
	, sum(isnull(gpi.car_gpi, 0.0)) as car_gpi
	, sum(isnull(gpi.trp_gpi, 0.0)) as trp_gpi
	-- AG
	, sum(isnull(car_ag.car_ag, 0.0)) as car_ag
	, sum(isnull(tp_ag.tp_ag, 0.0)) as tp_ag
	-- PROCURE
	, sum(isnull(car_tp_proc.car_proc, 0.0)) as car_proc
	, sum(isnull(car_tp_proc.trp_proc, 0.0)) as trp_proc
from perim
left join soldes_gpi as gpi 
	on gpi.code_perm = perim.code_perm and gpi.annee = perim.annee and gpi.eco = perim.eco
left join car_ag 
	on car_ag.code_perm = perim.code_perm and car_ag.annee = perim.annee and car_ag.eco = perim.eco
left join tp_ag 
	on tp_ag.code_perm = perim.code_perm and tp_ag.annee = perim.annee and tp_ag.eco = perim.eco
left join car_tp_proc 
	on car_tp_proc.code_perm = perim.code_perm and car_tp_proc.annee = perim.annee and car_tp_proc.eco = perim.eco
group by perim.code_perm, perim.annee, perim.eco