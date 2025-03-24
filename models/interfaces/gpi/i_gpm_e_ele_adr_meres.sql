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
select
    fiche, 
    IIF(adr.APP IS NOT NULL, adr.APP + '-', ' ') +
    IIF(adr.NO_CIV IS NOT NULL, adr.NO_CIV + ' ', ' ') +
    IIF(adr.ORIENT_RUE IS NOT NULL, adr.ORIENT_RUE + ' ', ' ') +
    IIF(adr.GENRE_RUE IS NOT NULL, adr.GENRE_RUE + ' ', ' ') +
    IIF(adr.RUE IS NOT NULL, adr.RUE + ', ', ' ') +
    IIF(adr.VILLE IS NOT NULL, adr.VILLE + ', ', ' ') +
    IIF(adr.CODE_POST IS NOT NULL, adr.CODE_POST, ' ') as adresse

from {{ var("database_gpi") }}.dbo.GPM_E_ADR adr 
where adr.TYPE_ADR in ('1', '3') and adr.DATE_EFFECT = (select max(DATE_EFFECT) FROM {{ var("database_gpi") }}.dbo.GPM_E_ADR where FICHE = adr.FICHE and TYPE_ADR in ('1', '3'))