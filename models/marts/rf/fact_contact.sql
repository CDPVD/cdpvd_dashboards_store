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
    -- construit le champs adresse et un seq_id pour conserver les dernieres lignes par type d'adresse
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
        from {{ ref("i_gpm_e_adr") }} as adr

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
    fiche,
    max(case when role = 'mere' and seq_id = 1 then adresse end) as adresse_maman,
    max(case when role = 'pere' and seq_id = 1 then adresse end) as adresse_papa,
    max(case when role = 'tuteur' and seq_id = 1 then adresse end) as adresse_tuteur
from seq2
group by fiche;