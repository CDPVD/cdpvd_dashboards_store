{#
Dashboards Store - Helping students, one dashboard at a time.
Copyright (C) 2023  Sciance Inc.

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
    codeperm as code_perm,
    nom,
    pnom as prenom,
    nompere as nom_pere, 
    pnompere as pnom_pere,
    nommere as nom_mere, 
    pnommere as pnom_mere,
    nomtuteur as nom_tuteur, 
    pnomtuteur as pnom_tuteur,
    adr_electr_ele, 
    adr_electr_pere, 
    adr_electr_mere,
    adr_electr_tuteur
from {{ var("database_jade") }}.dbo.e_ele
with (nolock)
