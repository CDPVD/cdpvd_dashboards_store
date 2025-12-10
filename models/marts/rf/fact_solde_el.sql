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
    -- Recuperer l'ensemble des eleves qui ont des inscriptions ces 15 dernieres annees en FGJ
    fgj as (
        select distinct 
			el.code_perm
			, dan.fiche
			, eco.annee
			, eco.eco
			, dan.id_eco
        from {{ ref("i_gpm_e_dan") }} as dan
		left join {{ ref("i_gpm_t_eco") }} as eco on eco.id_eco = dan.id_eco
		left join {{ ref("i_gpm_e_ele") }} as el on el.fiche = dan.fiche
		where eco.annee between {{ core_dashboards_store.get_current_year() }}-15 and {{ core_dashboards_store.get_current_year() }}

	-- soldes GPI
	), soldes_gpi as (
		select 
			fgj.code_perm 
			, fgj.fiche
			, fgj.annee
			, fgj.eco
			, sum(case when f.motif_fact in ('F','V') then f.solde else 0 end) as car_gpi
			, sum(case when f.motif_fact = 'A' then f.solde else 0 end) as trp_gpi
        from fgj
		left join {{ ref("i_gpm_n_fact") }} f on f.empr = fgj.fiche and f.id_eco = fgj.id_eco
		where 
			f.type_empr = 'E'
		group by fgj.code_perm, fgj.fiche, fgj.annee, fgj.eco
	
	-- car AG
	), car_ag as (
		select 
			fgj.code_perm 
			, fgj.fiche
			, fgj.annee
			, fgj.eco
			, isnull(sum(el.solde), 0.0) as car_ag
        from fgj
		left join {{ ref("i_sdg_e_fact") }} el on el.fiche = right('0000000' + cast(fgj.fiche as varchar(7)), 7) and el.annee = fgj.annee
		group by fgj.code_perm, fgj.fiche, fgj.annee, fgj.eco
	
	-- tp AG
	), tp_ag as (
		select 
			fgj.code_perm 
			, fgj.fiche
			, fgj.annee
			, fgj.eco
			, isnull(sum(tp.mnt), 0.0) + isnull(sum(rfnd.mnt), 0.0) as tp_ag
        from fgj
		left join {{ ref("i_sdg_e_trop_percus") }} tp on tp.fiche = right('0000000' + cast(fgj.fiche as varchar(7)), 7) and tp.annee = fgj.annee and tp.mnt > 0
		-- tp remboursés
		left join (select distinct fiche, id_sdg, sum(mnt) as mnt from {{ ref("i_sdg_e_trop_percus") }} where mnt < 0 group by fiche, id_sdg) as rfnd on rfnd.fiche = tp.fiche and rfnd.id_sdg = tp.id_sdg
		group by fgj.code_perm, fgj.fiche, fgj.annee, fgj.eco
	
	-- car tp PROCURE
	), car_tp_proc as (
		select
			code_perm,
			fiche, 
			annee,
			eco,
			car_proc,
			trp_proc
		from {{ ref("car_tp_procure") }}
	
	-- perimetre
    ), perim as (
        select distinct 
			coalesce(fgj.code_perm, car_tp_proc.code_perm) as code_perm,
			coalesce(cast(fgj.fiche as varchar(7)), cast(car_tp_proc.fiche as varchar(7))) as fiche,
            coalesce(fgj.annee, car_tp_proc.annee) as annee,
            coalesce(fgj.eco, car_tp_proc.eco) as eco
    from fgj
    full join car_tp_proc on fgj.code_perm = car_tp_proc.code_perm and fgj.annee = car_tp_proc.annee
	)

-- REQUETE FINALE
select 
    perim.code_perm,
	string_agg(perim.fiche, ', ') AS fiche, -- pour considerer les eleves avec 1 CP, 2 fiches la meme annee
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
left join soldes_gpi as gpi on gpi.code_perm = perim.code_perm and gpi.annee = perim.annee and gpi.eco = perim.eco
left join car_ag on car_ag.code_perm = perim.code_perm and car_ag.annee = perim.annee and car_ag.eco = perim.eco
left join tp_ag on tp_ag.code_perm = perim.code_perm and tp_ag.annee = perim.annee and tp_ag.eco = perim.eco
left join car_tp_proc on car_tp_proc.code_perm = perim.code_perm and car_tp_proc.annee = perim.annee and car_tp_proc.eco = perim.eco
group by perim.code_perm, perim.annee, perim.eco