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

    ), 
	
	-- Recuperer l'ensemble des eleves qui ont des inscriptions ces 10 dernieres annees en FP/FGA
    -- Possible qu'il y ai le cas 1 code_perm -> 2 fiches la meme annee (si 2 centres dans un meme CSS / 2 BD adultes...)
	fp as (
        select code_perm, fiche, annee, eco_cen as eco
        from {{ ref("fact_freq_adultes") }}
		where annee BETWEEN {{ core_dashboards_store.get_current_year() }}-10 AND {{ core_dashboards_store.get_current_year() }}

    ), 
	
	-- perimetre
	perim as (
        select distinct 
			coalesce(fgj.code_perm, fp.code_perm) as code_perm,
			coalesce(fgj.fiche, left(fp.fiche, ISNULL(NULLIF(charindex('_', fp.fiche)-1, -1), LEN(fp.fiche)))) as fiche,
            coalesce(fgj.annee, fp.annee) as annee,
            coalesce(fgj.eco, fp.eco) as eco
    from fgj
    full join fp on fgj.code_perm = fp.code_perm and fgj.annee = fp.annee
	),
	
	
	-- GPI - Comptes à recevoir
	car_gpi as (
		select 
			fgj.code_perm 
			, fgj.fiche
			, fgj.annee
			, fgj.eco
			, isnull(sum(f.solde), 0.0) as car_gpi
        from fgj
		left join {{ ref("i_gpm_n_fact") }} f on f.empr = fgj.fiche and f.id_eco = fgj.id_eco and f.annee = fgj.annee
        -- BESOIN????
		--left join [192.168.207.153].[GPIPRIM].dbo.gpm_t_projet p on f.projet = p.projet and f.id_eco = p.id_eco
		where 
			f.type_empr = 'E'
			and f.motif_fact = 'F'
			-- valider les descr et org (l. 91 à 93)
		group by fgj.code_perm, fgj.fiche, fgj.annee, fgj.eco
	), 
	
	-- GPI - Trop perçus
	trp_gpi as (
		select 
			fgj.code_perm 
			, fgj.fiche
			, fgj.annee
			, fgj.eco
			, isnull(sum(f.solde), 0.0) as trp_gpi
        from fgj
		left join {{ ref("i_gpm_n_fact") }} f on f.empr = fgj.fiche and f.id_eco = fgj.id_eco and f.annee = fgj.annee
        -- BESOIN????
		--left join [192.168.207.153].[GPIPRIM].dbo.gpm_t_projet p on f.projet = p.projet and f.id_eco = p.id_eco
		where 
			f.type_empr = 'E'
			and f.motif_fact = 'A'
			-- valider les descr et org (l. 91 à 93)
		group by fgj.code_perm, fgj.fiche, fgj.annee, fgj.eco
	), 
	
	-- AG - Comptes à recevoir
	car_ag as (
		select 
			fgj.code_perm 
			, fgj.fiche
			, fgj.annee
			, fgj.eco
			, isnull(sum(f.solde), 0.0) as car_ag
        from fgj
		left join {{ ref("i_sdg_eco") }} s on fgj.eco = s.eco
		left join {{ ref("i_sdg_e_fact") }} f on f.fiche = fgj.fiche and f.id_sdg = s.id_sdg AND f.annee = fgj.annee
		-- Voir si on a besoin de la table SDG_T_SERVICE ?? (Ecole différente de l'ECO)
		-- valider les ecos (l. 112)
		group by fgj.code_perm, fgj.fiche, fgj.annee, fgj.eco
	), 
	
	-- AG - Trop perçus
	trp_ag as (
		select 
			fgj.code_perm 
			, fgj.fiche
			, fgj.annee
			, fgj.eco
			, isnull(sum(tp.MNT), 0.0) as trp_ag
        from fgj
		left join {{ ref("i_sdg_eco") }} s on fgj.eco = s.eco
		left join {{ ref("i_sdg_e_trop_percus") }} tp on tp.fiche = fgj.fiche and tp.id_sdg = s.id_sdg AND tp.annee = fgj.annee
		-- Voir si on a besoin de la table SDG_T_SERVICE ?? (Ecole différente de l'ECO)
		-- valider les ecos (l. 112)
		group by fgj.code_perm, fgj.fiche, fgj.annee, fgj.eco

	-- soldes PROCURE
    -- le cast force les fiches a etre des varchar car mes fiche FP/FGA le sont et sinon ca pete (cas specifique vdc)
	), 
	
	-- PROCURE - Comptes à recevoir
	car_proc as (
		select 
			fp.code_perm 
			--, left(fp.fiche, charindex('_', fp.fiche)-1) as fiche
			, left(fp.fiche, ISNULL(NULLIF(charindex('_', fp.fiche)-1, -1), LEN(fp.fiche)) ) as fiche
			, fp.annee
			, fp.eco
			, isnull(sum(f.solde), 0.0) as car_proc
        from fp
		left join {{ ref("i_pro_art_emprunt") }} as f on f.code_emprunt = try_cast(fp.fiche as nvarchar) and f.eco_cen = fp.eco and f.annee = fp.annee
		--BESOIN????
        --left join [192.168.207.153].[PROCCRIF].dbo.PRO_PAIEMNT as c on c.code_emprunt = cast(fp.fiche as nvarchar(7)) and c.ecocen = fp.eco
		where 
			f.statut != 15
			-- valider les descr et org (l. 131)
		group by fp.code_perm, fp.fiche, fp.annee, fp.eco
	), 
	
	-- PROCURE - Trop perçus
	trp_proc as (
		select 
			fp.code_perm 
			--, left(fp.fiche, charindex('_', fp.fiche)-1) as fiche
			, left(fp.fiche, ISNULL(NULLIF(charindex('_', fp.fiche)-1, -1), LEN(fp.fiche)) ) as fiche
			, fp.annee
			, fp.eco
			, isnull(sum(p.mont_non_repart), 0.0) as trp_proc
        from fp
		left join {{ ref("i_pro_paiemnt") }} as p on p.code_emprunt = try_cast(fp.fiche as nvarchar) and p.eco_cen = fp.eco and p.annee = fp.annee
		--BESOIN????
        --left join [192.168.207.153].[PROCCRIF].dbo.PRO_PAIEMNT as c on c.code_emprunt = cast(fp.fiche as nvarchar(7)) and c.ecocen = fp.eco
		where 
			p.type_emprunt = '1' 
			and	p.date_annul is null 
			and	p.type_paiemnt = '4'
			-- valider les descr et org (l. 131)
		group by fp.code_perm, fp.fiche, fp.annee, fp.eco
	), 
	
	-- Contacts Père, mère, tuteur
	contacts as (
		select 
			code_perm, 
			fiche,
			nom_pere, 
			pnom_pere,
			adr_electr_pere, 
			nom_mere, 
			pnom_mere,
			adr_electr_mere,
			nom_tuteur, 
			pnom_tuteur,
			adr_electr_tuteur,
			adr_pere,
			adr_mere,
			adr_tuteur
		from {{ ref("eleves_contacts_adresses") }}
	)

-- REQUETE FINALE
select 
    perim.code_perm,
	string_agg(perim.fiche, ', ') AS fiche, -- pour considerer les eleves avec 1 CP, 2 fiches la meme annee
    perim.annee,
    perim.eco,
	-- Contacts
	c.nom_pere, 
	c.pnom_pere,
	c.adr_electr_pere, 
	c.nom_mere, 
	c.pnom_mere,
	c.adr_electr_mere,
	c.nom_tuteur, 
	c.pnom_tuteur,
	c.adr_electr_tuteur,
	c.adr_pere,
	c.adr_mere,
	c.adr_tuteur,
	-- GPI
	 sum(isnull(car_gpi.car_gpi, 0.0)) as car_gpi
	, sum(isnull(trp_gpi.trp_gpi, 0.0)) as trp_gpi
	-- AG
	, sum(isnull(car_ag.car_ag, 0.0)) as car_ag
	, sum(isnull(trp_ag.trp_ag, 0.0)) as trp_ag
	-- PROCURE
	, sum(isnull(car_proc.car_proc, 0.0)) as car_proc
	, sum(isnull(trp_proc.trp_proc, 0.0)) as trp_proc

from perim
left join car_gpi on car_gpi.code_perm = perim.code_perm and car_gpi.annee = perim.annee and car_gpi.eco = perim.eco
left join trp_gpi on trp_gpi.code_perm = perim.code_perm and trp_gpi.annee = perim.annee and trp_gpi.eco = perim.eco
left join car_ag on car_ag.code_perm = perim.code_perm and car_ag.annee = perim.annee and car_ag.eco = perim.eco
left join trp_ag on trp_ag.code_perm = perim.code_perm and trp_ag.annee = perim.annee and trp_ag.eco = perim.eco
left join car_proc on car_proc.code_perm = perim.code_perm and car_proc.annee = perim.annee and car_proc.eco = perim.eco
left join trp_proc on trp_proc.code_perm = perim.code_perm and trp_proc.annee = perim.annee and trp_proc.eco = perim.eco
left join contacts c on perim.fiche = c.fiche
group by perim.code_perm, perim.annee, perim.eco,
c.nom_pere,
c.pnom_pere,
c.adr_electr_pere,
c.adr_pere,
c.nom_mere,
c.pnom_mere,
c.adr_electr_mere,
c.adr_mere,
c.nom_tuteur,
c.pnom_tuteur,
c.adr_electr_tuteur,
c.adr_tuteur