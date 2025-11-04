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
    -- Recuperer l'ensemble des eleves qui ont des inscriptions ces 10 dernieres annees en FGJ
    fgj as (
        select distinct code_perm, fiche, annee, eco, id_eco
        from {{ ref("spine") }}
		where annee between {{ core_dashboards_store.get_current_year() }}-10 and {{ core_dashboards_store.get_current_year() }}

	-- Recuperer l'ensemble des eleves qui ont des inscriptions ces 10 dernieres annees en FP/FGA
    -- Possible qu'il y ai le cas 1 code_perm -> 2 fiches la meme annee (si 2 centres dans un meme CSS / 2 BD adultes...)
    ), fp as (
        select pop.code_perm, pop.fiche, pop.annee, freq.eco_cen as eco
        from {{ ref("stg_populations_adultes") }} as pop
		join {{ ref("i_e_freq_adultes") }} as freq
			on freq.fiche = pop.fiche and freq.annee = pop.annee and freq.freq = pop.freq
		where pop.annee BETWEEN {{ core_dashboards_store.get_current_year() }}-10 AND {{ core_dashboards_store.get_current_year() }}

	-- perimetre
    ), perim as (
        select distinct 
			coalesce(fgj.code_perm, fp.code_perm) as code_perm,
			coalesce(fgj.fiche, left(fp.fiche, isnull(nullif(charindex('_', fp.fiche)-1, -1), len(fp.fiche)))) as fiche, -- gestion fiche avec / sans '_'
            coalesce(fgj.annee, fp.annee) as annee,
            coalesce(fgj.eco, fp.eco) as eco
    from fgj
    full join fp on fgj.code_perm = fp.code_perm and fgj.annee = fp.annee
	-- soldes GPI
	), soldes_gpi as (
		select 
			fgj.code_perm 
			, fgj.fiche
			, fgj.annee
			, fgj.eco
			, sum(case when f.motif_fact = 'F' then f.solde else 0 end) as car_gpi
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
		left join {{ ref("i_sdg_t_service") }} serv on fgj.eco = serv.eco
		left join {{ ref("i_sdg_e_fact") }} el on el.fiche = right('0000000' + cast(fgj.fiche as varchar(7)), 7) and el.id_sdg = serv.id_sdg
		group by fgj.code_perm, fgj.fiche, fgj.annee, fgj.eco
	
	-- tp AG
	), tp_ag as (
		select 
			fgj.code_perm 
			, fgj.fiche
			, fgj.annee
			, fgj.eco
			, isnull(sum(tp.mnt), 0.0) as tp_ag
        from fgj
		left join {{ ref("i_sdg_t_service") }} serv on fgj.eco = serv.eco
		left join {{ ref("i_sdg_e_trop_percus") }} tp on tp.fiche = right('0000000' + cast(fgj.fiche as varchar(7)), 7) and tp.id_sdg = serv.id_sdg
		group by fgj.code_perm, fgj.fiche, fgj.annee, fgj.eco

	-- car PROCURE
	), car_proc as (
		select 
			fp.code_perm 
			, fp.fiche
			, fp.annee
			, fp.eco
			, isnull(sum(car.solde), 0.0) as car_proc
        from fp
		left join {{ ref("i_pro_art_emprunt") }} as car on right('0000000' + cast(car.code_emprunt as varchar(7)), 7) = left(fp.fiche, isnull(nullif(charindex('_', fp.fiche)-1, -1), len(fp.fiche))) and car.eco_cen = fp.eco
		where 
			car.statut != 15
		group by fp.code_perm, fp.fiche, fp.annee, fp.eco

	-- tp PROCURE
	), tp_proc as (
		select 
			fp.code_perm 
			, fp.fiche
			, fp.annee
			, fp.eco
			, isnull(sum(tp.mont_non_repart), 0.0) as trp_proc
        from fp
		left join {{ ref("i_pro_paiemnt") }} as tp on right('0000000' + cast(tp.code_emprunt as varchar(7)), 7) = left(fp.fiche, isnull(nullif(charindex('_', fp.fiche)-1, -1), len(fp.fiche))) and tp.eco_cen = fp.eco
		where 
			tp.type_emprunt = '1' 
			and	tp.date_annul is null 
			and	tp.type_paiemnt = '4'
		group by fp.code_perm, fp.fiche, fp.annee, fp.eco
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
	, sum(isnull(car_proc.car_proc, 0.0)) as car_proc
	, sum(isnull(tp_proc.trp_proc, 0.0)) as trp_proc
from perim
left join soldes_gpi as gpi on gpi.code_perm = perim.code_perm and gpi.annee = perim.annee and gpi.eco = perim.eco
left join car_ag on car_ag.code_perm = perim.code_perm and car_ag.annee = perim.annee and car_ag.eco = perim.eco
left join tp_ag on tp_ag.code_perm = perim.code_perm and tp_ag.annee = perim.annee and tp_ag.eco = perim.eco
left join car_proc on car_proc.code_perm = perim.code_perm and car_proc.annee = perim.annee and car_proc.eco = perim.eco
left join tp_proc on tp_proc.code_perm = perim.code_perm and tp_proc.annee = perim.annee and tp_proc.eco = perim.eco
group by perim.code_perm, perim.annee, perim.eco