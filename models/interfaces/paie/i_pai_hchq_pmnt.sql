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
    matr,
    no_seq,
    code_pmnt,
    corp_empl,
    lieu_trav,
    mode,
    nb_unit,
    ref_empl,
    no_cheq,
    date_cheq,
    date_deb,
    date_fin,
    code_prov,
    mnt,
    no_cmpt_cour_trait_diff
from {{ var("database_paie") }}.dbo.pai_hchq_pmnt
