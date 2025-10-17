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

-- Récupérer la date de debut d'extraction
{% set date_pivot = var("marts")["human_resources"]["date_pivot"] %}

with src as (
    select
        matr
        , date_cheq
        , no_cheq
        , no_seq
        , no_cmpt
        , left(no_cmpt, 3) as lieu_trav_cpt_budg
        , mnt_dist
    from {{ ref("i_pai_hchq_pmnt_dist_cmpt") }}
    where date_cheq >= {d '{{ date_pivot }}'}
)

select 
    matr
    , date_cheq
    , no_cheq
    , no_seq
    , no_cmpt
    , lieu_trav_cpt_budg
    , sum(mnt_dist) as mnt_dist
from src
group by matr, date_cheq, no_cheq, no_seq, no_cmpt, lieu_trav_cpt_budg

