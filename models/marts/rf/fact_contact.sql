{#
CDPVD Dashboards store
Copyright (C) 2025 CDPVD.

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
		-- Avt-Garde
        select
			ele.code_perm,
			ele.fiche,
			ele.nom, 
    		ele.prenom, 
			ele.nom_mere, 
			ele.pnom_mere,
			null as adr_electr_mere,
			ele.nom_pere, 
			ele.pnom_pere,
			null as adr_electr_pere, 
			ele.nom_tuteur, 
			ele.pnom_tuteur,
			null as adr_electr_tuteur,
			adr.type_contact as type_adr,
			ele.date_maj,
			ltrim(rtrim(
				isnull(nullif(adr.app, '') + '-', '') +
				isnull(nullif(adr.no_civ, '') + ' ', '') +
				isnull(nullif(adr.orient_rue, '') + ' ', '') +
				isnull(nullif(adr.genre_rue, '') + ' ', '') +
				isnull(nullif(adr.rue, '') + ', ', '') +
				isnull(nullif(adr.ville, '') + ', ', '') +
				isnull(nullif(adr.code_post, ''), '')
			)) as adresse
		from {{ ref("contact_sdg") }} ele
		left join {{ ref("i_sdg_e_contact") }} adr
			on adr.fiche = ele.fiche
		where 
			ele.code_perm is not null
			and adr.adr_eleve = '1' 					-- adresse principal uniquement
		
		union all

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
			ele.date_maj,
			ltrim(rtrim(
				isnull(nullif(adr.app, '') + '-', '') +
				isnull(nullif(adr.no_civ, '') + ' ', '') +
				isnull(nullif(adr.orient_rue, '') + ' ', '') +
				isnull(nullif(adr.genre_rue, '') + ' ', '') +
				isnull(nullif(adr.rue, '') + ', ', '') +
				isnull(nullif(adr.ville, '') + ', ', '') +
				isnull(nullif(adr.code_post, ''), '')
			)) as adresse
		from {{ ref("i_e_ele") }} ele
		left join {{ ref("i_e_adr") }} adr
			on adr.fiche = ele.fiche
		where 
			ele.code_perm is not null
			and adr.ind_envoi_meq = '1' 				-- adresse principal uniquement
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
			ele.date_maj,
			ltrim(rtrim(
				isnull(adr.app + '-', '') +
				isnull(adr.no_civ + ' ', '') +
				isnull(adr.orient_rue + ' ', '') +
				isnull(adr.genrerue + ' ', '') +
				isnull(adr.rue + ', ', '') +
				isnull(adr.ville + ', ', '') +
				isnull(adr.code_post, '')
			)) as adresse
		from {{ ref("i_e_ele_adultes") }} ele
		left join {{ ref("i_e_adr_adultes") }} adr
			on adr.fiche = ele.fiche
		where 
			ele.code_perm is not null
			and adr.envoimeq = '1' 					 -- adresse principal uniquement
	
	-- Pour chaque cp, conserver la derniere maj et la fiche associée
	), latest as (
		select *
		from (
			select 
				*,
				row_number() over (partition by code_perm order by date_maj desc) as seq_id
			from adr
		) as seq
		where seq_id = 1

	)

select
    code_perm,
    nom,		 
    prenom,
    fiche,
	nom_mere, 
	pnom_mere,
	adr_electr_mere,
	nom_pere, 
	pnom_pere,
	adr_electr_pere, 
	nom_tuteur, 
	pnom_tuteur,
	adr_electr_tuteur,
	adresse,
	case 
		when type_adr = '1' then 'Père et mère'
		when type_adr = '2' then 'Père'
		when type_adr = '3' then 'Mère'
		when type_adr = '4' then 'Tuteur'
		when type_adr = '5' then 'Elève'
		else 'Autre'
	end as adresse_contact
from latest