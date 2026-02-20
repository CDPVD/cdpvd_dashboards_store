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
	-- Recuperer l'ensemble des adresses FGJ-FP-FGA
	adr as (
		-- FGJ
        select
			ele.code_perm,
			right('0000000' + cast(adr.fiche as varchar(7)), 7) as fiche,
			ele.nom, 
    		ele.prenom, 
    		ele.nom_mere, 
			ele.pnom_mere,
			ele.adr_electr_mere,
			ele.nom_pere, 
			ele.pnom_pere,
			ele.adr_electr_pere, 
			ele.nom_tuteur, 
			ele.pnom_tuteur,
			ele.adr_electr_tuteur,
			adr.type_adr,
			adr.date_effect,
			adr.date_fin,
			ltrim(rtrim(
				isnull(nullif(adr.app, '') + '-', '') +
				isnull(nullif(adr.no_civ, '') + ' ', '') +
				isnull(nullif(adr.orient_rue, '') + ' ', '') +
				isnull(nullif(adr.genre_rue, '') + ' ', '') +
				isnull(nullif(adr.rue, '') + ', ', '') +
				isnull(nullif(adr.ville, '') + ', ', '') +
				isnull(nullif(adr.code_post, ''), '')
			)) as adresse
		from {{ ref("i_e_adr") }} adr
		left join {{ ref("i_e_ele") }} ele
			on ele.fiche = adr.fiche
		where 
			cast(getdate() as date) >= adr.date_effect 	-- l'adresse doit etre effective aujourd'hui
			and ele.code_perm is not null

		union all

		-- FP et FGA
		select
			ele.code_perm,
			case
				when charindex('_', adr.fiche) > 0 then right('0000000' + left(adr.fiche, charindex('_', adr.fiche) - 1), 7)
				else right('0000000' + cast(adr.fiche as varchar(7)), 7)
			end as fiche,
			ele.nom, 
    		ele.prenom,
    		ele.nom_mere, 
			ele.pnom_mere,
			ele.adr_electr_mere,
			ele.nom_pere, 
			ele.pnom_pere,
			ele.adr_electr_pere, 
			ele.nom_tuteur, 
			ele.pnom_tuteur,
			ele.adr_electr_tuteur,
			adr.typeadr as type_adr,
			adr.date_effect,
			adr.date_fin,
			ltrim(rtrim(
				isnull(adr.app + '-', '') +
				isnull(adr.no_civ + ' ', '') +
				isnull(adr.orient_rue + ' ', '') +
				isnull(adr.genrerue + ' ', '') +
				isnull(adr.rue + ', ', '') +
				isnull(adr.ville + ', ', '') +
				isnull(adr.code_post, '')
			)) as adresse
		from {{ ref("i_e_adr_adultes") }} adr
		left join {{ ref("i_e_ele_adultes") }} ele
			on ele.fiche = adr.fiche
	where 
		cast(getdate() as date) >= adr.date_effect 	-- l'adresse doit etre effective aujourd'hui
		and ele.code_perm is not null
	
	-- Ajout d'un seq_id pour conserver les dernieres lignes par type d'adresse
	), seq as (
        select 
			*,
			row_number() over (partition by code_perm, type_adr order by date_effect desc) as seq_id
        from adr

	-- Conserver les dernieres adresses par type
	), latest as (
		select *
		from seq
		where seq_id = 1

	-- Redefinir les types d'adresses en role
	), roles as (
		select 
			fiche, 
			code_perm,
			nom, 
    		prenom, 
			'mere' as role, 
			date_effect, 
			date_fin,
			adresse, 
    		nom_mere, 
			pnom_mere,
			adr_electr_mere,
			nom_pere, 
			pnom_pere,
			adr_electr_pere, 
			nom_tuteur, 
			pnom_tuteur,
			adr_electr_tuteur
		from latest
		where type_adr in (1,3)

		union all

		select 
			fiche, 
			code_perm, 
			nom, 
    		prenom, 
			'pere' as role, 
			date_effect, 
			date_fin, 
			adresse, 
    		nom_mere, 
			pnom_mere,
			adr_electr_mere,
			nom_pere, 
			pnom_pere,
			adr_electr_pere, 
			nom_tuteur, 
			pnom_tuteur,
			adr_electr_tuteur
		from latest
		where type_adr in (1,2)

		union all

		select 
			fiche, 
			code_perm, 
			nom, 
    		prenom, 
			'tuteur' as role, 
			date_effect, 
			date_fin, 
			adresse, 
    		nom_mere, 
			pnom_mere,
			adr_electr_mere,
			nom_pere, 
			pnom_pere,
			adr_electr_pere, 
			nom_tuteur, 
			pnom_tuteur,
			adr_electr_tuteur
		from latest
		where type_adr = 4
	
		union all

		select 
			fiche, 
			code_perm, 
			nom, 
    		prenom, 
			'eleve' as role, 
			date_effect, 
			date_fin, 
			adresse, 
    		nom_mere, 
			pnom_mere,
			adr_electr_mere,
			nom_pere, 
			pnom_pere,
			adr_electr_pere, 
			nom_tuteur, 
			pnom_tuteur,
			adr_electr_tuteur
		from latest
		where type_adr = 5

	-- ajout d'un nouveau seq_id par role
	), seq2 as (
    select 
		*,
        row_number() over (partition by code_perm, role order by date_effect desc) as seq_id
    from roles
)

select
    code_perm,
	nom, 
    prenom, 
	fiche,
	max(case when role = 'eleve' and seq_id = 1 then adresse end) as adresse_eleve,
    nom_mere, 
	pnom_mere,
	adr_electr_mere,
    max(case when role = 'mere' and seq_id = 1 then adresse end) as adresse_mere,
	nom_pere, 
	pnom_pere,
	adr_electr_pere, 
    max(case when role = 'pere' and seq_id = 1 then adresse end) as adresse_pere,
	nom_tuteur, 
	pnom_tuteur,
	adr_electr_tuteur,
    max(case when role = 'tuteur' and seq_id = 1 then adresse end) as adresse_tuteur
from seq2
group by code_perm, nom, prenom, fiche, nom_mere, pnom_mere, adr_electr_mere, nom_pere, pnom_pere, adr_electr_pere, nom_tuteur, pnom_tuteur, adr_electr_tuteur
