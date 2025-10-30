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
        select code_perm, fiche, annee, eco_cen as eco
        from {{ ref("fact_freq_adultes") }}
		where annee BETWEEN {{ core_dashboards_store.get_current_year() }}-10 AND {{ core_dashboards_store.get_current_year() }}

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
	
	-- soldes AG
	), soldes_ag as (
		select 
			fgj.code_perm 
			, fgj.fiche
			, fgj.annee
			, fgj.eco
			, isnull(sum(el.solde), 0.0) as car_ag
			, isnull(sum(tp.mnt), 0.0) as tp_ag
        from fgj
		left join {{ ref("i_sdg_t_service") }} serv on fgj.eco = serv.eco
		left join {{ ref("i_sdg_e_fact") }} el on el.fiche = fgj.fiche and el.id_sdg = serv.id_sdg AND el.annee = fgj.annee
		left join {{ ref("i_sdg_e_trop_percus") }} tp on tp.fiche = fgj.fiche and tp.id_sdg = serv.id_sdg AND tp.annee = fgj.annee
		group by fgj.code_perm, fgj.fiche, fgj.annee, fgj.eco

	-- soldes PROCURE
    -- le cast force les fiches a etre des varchar car mes fiche FP/FGA le sont et sinon ca pete (cas specifique vdc)
	), soldes_proc as (
		select 
			fp.code_perm 
			, left(fp.fiche, isnull(nullif(charindex('_', fp.fiche)-1, -1), len(fp.fiche)) ) as fiche
			, fp.annee
			, fp.eco
			, isnull(sum(car.solde), 0.0) as car_proc
			, isnull(sum(tp.mont_non_repart), 0.0) as trp_proc
        from fp
		left join {{ ref("i_pro_art_emprunt") }} as car on car.code_emprunt = fp.fiche and car.eco_cen = fp.eco and car.annee = fp.annee
		left join {{ ref("i_pro_paiemnt") }} as tp on tp.code_emprunt = fp.fiche and tp.eco_cen = fp.eco and year(tp.date_paiemnt) = fp.annee
		where 
			car.statut != 15
			and tp.type_emprunt = '1' 
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
	, sum(isnull(ag.car_ag, 0.0)) as car_ag
	, sum(isnull(ag.tp_ag, 0.0)) as tp_ag
	-- PROCURE
	, sum(isnull(prc.car_proc, 0.0)) as car_proc
	, sum(isnull(prc.trp_proc, 0.0)) as trp_proc
from perim
left join soldes_gpi as gpi on gpi.code_perm = perim.code_perm and gpi.annee = perim.annee and gpi.eco = perim.eco
left join soldes_ag as ag on ag.code_perm = perim.code_perm and ag.annee = perim.annee and ag.eco = perim.eco
left join soldes_proc as prc on prc.code_perm = perim.code_perm and prc.annee = perim.annee and prc.eco = perim.eco
group by perim.code_perm, perim.annee, perim.eco