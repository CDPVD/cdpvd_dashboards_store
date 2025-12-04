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
    -- Ajout du champs adresse et un seq_id pour conserver les dernieres lignes par type d'adresse
    seq as (
        select 
			fiche,
			type_adr,
			date_effect,
			iif(app is not null, app + '-', ' ') +
			iif(no_civ is not null, no_civ + ' ', ' ') +
			iif(orient_rue is not null, orient_rue + ' ', ' ') +
			iif(genre_rue is not null, genre_rue + ' ', ' ') +
			iif(rue is not null, rue + ', ', ' ') +
			iif(ville is not null, ville + ', ', ' ') +
			iif(code_post is not null, code_post, ' ') as adresse,
			row_number() over (partition by fiche, type_adr order by date_effect desc) as seq_id
        from {{ ref("i_gpm_e_adr") }}

	-- Conserver les dernieres adresses par type
	), latest as (
		select *
		from seq
		where seq_id = 1

	-- redefinir les types d'adresses en role
	), roles as (
		select fiche, 'mere' as role, date_effect, adresse
		from latest
		where type_adr in (1,3)

		union all

		select fiche, 'pere' as role, date_effect, adresse
		from latest
		where type_adr in (1,2)

		union all

		select fiche, 'tuteur' as role, date_effect, adresse
		from latest
		where type_adr = 4
	
	-- ajout d'un nouveau seq_id par role
	), seq2 as (
    select 
		*,
        row_number() over (partition by fiche, role order by date_effect desc) as seq_id
    from roles
)

select
    el.code_perm,
	el.nom, 
    el.pnom, 
	seq2.fiche,
    el.nom_mere, 
	el.pnom_mere,
	el.adr_electr_mere,
    max(case when seq2.role = 'mere' and seq2.seq_id = 1 then seq2.adresse end) as adresse_mere,
	el.nom_pere, 
	el.pnom_pere,
	el.adr_electr_pere, 
    max(case when seq2.role = 'pere' and seq2.seq_id = 1 then seq2.adresse end) as adresse_pere,
	el.nom_tuteur, 
	el.pnom_tuteur,
	el.adr_electr_tuteur,
    max(case when seq2.role = 'tuteur' and seq2.seq_id = 1 then seq2.adresse end) as adresse_tuteur
from seq2
left join {{ ref("i_gpm_e_ele") }} as el on el.fiche = seq2.fiche
group by el.code_perm, 	el.nom, el.pnom, seq2.fiche, el.nom_mere, el.pnom_mere, el.adr_electr_mere, el.nom_pere, el.pnom_pere, el.adr_electr_pere, el.nom_tuteur, el.pnom_tuteur, el.adr_electr_tuteur