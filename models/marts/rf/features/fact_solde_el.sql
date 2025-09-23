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
        select distinct code_perm, cast(fiche as nvarchar) as fiche, annee, eco, id_eco
        from {{ ref("spine_all") }}
		where annee between {{ core_dashboards_store.get_current_year() }}-10 and {{ core_dashboards_store.get_current_year() }}

    ), 
	
	-- Recuperer l'ensemble des eleves qui ont des inscriptions ces 10 dernieres annees en FP/FGA
    -- Possible qu'il y ai le cas 1 code_perm -> 2 fiches la meme annee (si 2 centres dans un meme CSS / 2 BD adultes...)
	fp as (
        select code_perm, cast(fiche as nvarchar) as fiche, annee, eco_cen as eco
        from {{ ref("fact_freq_adultes") }}
		where annee BETWEEN {{ core_dashboards_store.get_current_year() }}-10 AND {{ core_dashboards_store.get_current_year() }}

    ), 
	
	-- perimetre
	perim as (
        select distinct 
			coalesce(fgj.code_perm, fp.code_perm) as code_perm,
			coalesce(fgj.fiche, left(fp.fiche, ISNULL(NULLIF(charindex('_', fp.fiche)-1, -1), LEN(fp.fiche)))) as fiche,
            coalesce(fgj.annee, fp.annee) as annee,
            coalesce(fgj.eco, fp.eco) as eco,
			iif(fgj.code_perm is not null, 'J', iif(fp.code_perm is not null, 'A', NULL)) as type_perim -- J - Jeunes, A - Adultes
    from fgj
    full join fp on fgj.code_perm = fp.code_perm and fgj.annee = fp.annee
	),

	contacts_fgj as (
		select 
			code_perm, 
			cast(fiche as nvarchar) as fiche,
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
		from {{ ref("i_fgj_contacts_adresses") }}
	), 

	contacts_fgafp as (
		select 
			codePerm, 
			cast(fiche as nvarchar) as fiche,
			nomPere, 
			pnomPere,
			ADR_ELECTR_PERE, 
			NomMere, 
			PnomMere,
			ADR_ELECTR_MERE,
			NomTuteur, 
			PnomTuteur,
			ADR_ELECTR_TUTEUR,
			ADR_ELECTR_ELE,
			adr_pere,
			adr_mere,
			adr_tuteur,
			adr_eleve
		from {{ ref("i_fgafp_contacts_adresses") }}
	), 
	
	perim_contacts as (
		select p.*,
		coalesce(cj.nom_pere, ca.nomPere) as nom_pere, 
		coalesce(cj.pnom_pere, ca.pnomPere) as pnom_pere, 
		coalesce(cj.adr_electr_pere, ca.adr_electr_pere) as adr_electr_pere, 
		coalesce(cj.nom_mere, ca.NomMere) as nom_mere, 
		coalesce(cj.pnom_mere, ca.PnomMere) as pnom_mere, 
		coalesce(cj.adr_electr_mere, ca.adr_electr_mere) as adr_electr_mere, 
		coalesce(cj.nom_tuteur, ca.NomTuteur) as nom_tuteur, 
		coalesce(cj.pnom_tuteur, ca.PnomTuteur) as pnom_tuteur, 
		coalesce(cj.adr_electr_tuteur, ca.adr_electr_tuteur) as adr_electr_tuteur, 
		coalesce(cj.adr_pere, ca.adr_pere) as adr_pere, 
		coalesce(cj.adr_mere, ca.adr_mere) as adr_mere, 
		coalesce(cj.adr_tuteur, ca.adr_tuteur) as adr_tuteur, 
		iif(p.type_perim = 'A', ca.ADR_ELECTR_ELE, '') as adr_electr_eleve,
		iif(p.type_perim = 'A', ca.adr_eleve, '') as adr_eleve
		from perim p
		left join contacts_fgj cj on p.fiche = cj.fiche AND p.type_perim = 'J'
		left join contacts_fgafp ca on p.fiche = ca.fiche AND p.type_perim = 'A'
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
	
	actifs as (
		select 		cast(dan.fiche as nvarchar) as fiche, eco.eco, concat('(', eco.eco, ') - ', eco.nom_eco) as nom_ecole
		from 		{{ ref("i_gpm_e_dan") }} dan
		inner join 	{{ ref("i_gpm_t_eco") }} eco on dan.id_eco = eco.id_eco
		where		eco.annee = {{ core_dashboards_store.get_current_year() }} and dan.statut_don_an = 'A'
		union all
		select		cast(f.fiche as nvarchar), f.eco_cen as eco, concat('(', ec.eco_cen, ') - ', ec.descr) as nom_ecole
		from		{{ ref("i_e_freq_adultes") }} f
		left join	{{ ref("i_t_ecocen_adultes") }} ec on f.eco_cen = ec.eco_cen
		where		((MONTH(CURRENT_TIMESTAMP) < 8 and annee = YEAR(CURRENT_TIMESTAMP)-1)
					OR (MONTH(CURRENT_TIMESTAMP) > 8 and annee = YEAR(CURRENT_TIMESTAMP)))
				and (ec.CfpOff <> '' or ec.CenOff <> '')
				and f.Date_Fin <> ''
	)

-- REQUETE FINALE
select 
    p.code_perm,
	p.type_perim,
	IIF(cast(a.fiche as nvarchar) IS NOT NULL, 'A', 'I') as statut,
	ele.nom,
	ele.pnom,
	string_agg(p.fiche, ', ') AS fiche, -- pour considerer les eleves avec 1 CP, 2 fiches la meme annee
	p.annee,
	p.eco,
	a.nom_ecole, 
	p.nom_pere, 
	p.pnom_pere,
	p.adr_electr_pere, 
	p.nom_mere, 
	p.pnom_mere,
	p.adr_electr_mere,
	p.nom_tuteur, 
	p.pnom_tuteur,
	p.adr_electr_tuteur,
	p.adr_pere,
	p.adr_mere,
	p.adr_tuteur,
	p.adr_electr_eleve, 
	p.adr_eleve,
 
	-- GPI
	 sum(isnull(car_gpi.car_gpi, 0.0)) as car_gpi
	, sum(isnull(trp_gpi.trp_gpi, 0.0)) as trp_gpi
	-- AG
	, sum(isnull(car_ag.car_ag, 0.0)) as car_ag
	, sum(isnull(trp_ag.trp_ag, 0.0)) as trp_ag
	-- PROCURE
	, sum(isnull(car_proc.car_proc, 0.0)) as car_proc
	, sum(isnull(trp_proc.trp_proc, 0.0)) as trp_proc

from perim_contacts p
inner join {{ ref("i_gpm_e_ele") }} ele on ele.fiche = p.fiche
left join actifs a on a.fiche = p.fiche and a.eco = p.eco
left join car_gpi on car_gpi.code_perm = p.code_perm and car_gpi.annee = p.annee and car_gpi.eco = p.eco
left join trp_gpi on trp_gpi.code_perm = p.code_perm and trp_gpi.annee = p.annee and trp_gpi.eco = p.eco
left join car_ag on car_ag.code_perm = p.code_perm and car_ag.annee = p.annee and car_ag.eco = p.eco
left join trp_ag on trp_ag.code_perm = p.code_perm and trp_ag.annee = p.annee and trp_ag.eco = p.eco
left join car_proc on car_proc.code_perm = p.code_perm and car_proc.annee = p.annee and car_proc.eco = p.eco
left join trp_proc on trp_proc.code_perm = p.code_perm and trp_proc.annee = p.annee and trp_proc.eco = p.eco
group by p.code_perm, p.type_perim, a.fiche, ele.nom, ele.pnom, p.annee, p.eco, a.nom_ecole,
p.nom_pere, 
p.pnom_pere,
p.adr_electr_pere, 
p.nom_mere, 
p.pnom_mere,
p.adr_electr_mere,
p.nom_tuteur, 
p.pnom_tuteur,
p.adr_electr_tuteur,
p.adr_pere,
p.adr_mere,
p.adr_tuteur,
p.adr_electr_eleve, 
p.adr_eleve