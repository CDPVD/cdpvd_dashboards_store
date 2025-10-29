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
    -- construit l'adresse
    adr as (
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
			iif(code_post is not null, code_post, ' ') as adresse
        from {{ ref("i_gpm_e_adr") }}

	-- table avec les différentes adresses
	)

	select * 
	from adr