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
{# 
    Extract all the resignation date for all of the employees.
#}
-- Extract all the valid resignations etat as well as the the corps_empl, lieu_trav and
--
{{
    config(
        materialized="table",
        post_hook=[
            core_dashboards_store.create_clustered_index(
                "{{ this }}",
                ["matr", "corp_empl", "lieu_trav", "school_year", "ref_empl"],
                unique=True,
            ),
        ],
    )
}}

with

    extraction as (
        select
            act.matr,
            act.corp_empl,
            act.lieu_trav,
            act.school_year,
            act.ref_empl,
            count(distinct(act.ref_empl)) as nbremploi,
            count(distinct(res.ref_empl)) as nbrdemission,
            empl.genre as genre
        from [dbo_human_resources_staging]. [cdpvd_stg_activity_history] act
        left join  -- left join pour aller chercher tous les employés, et non seulement ceux qui ont démissionner.
            [dbo_human_resources_features]. [fact_resignation] res
            on res.matr = act.matr
            and res.corp_empl = act.corp_empl
            and res.lieu_trav = act.lieu_trav
            and res.ref_empl = act.ref_empl
            and res.school_year = act.school_year
        inner join [dbo_human_resources]. [dim_employees] empl on empl.matr = act.matr
        group by
            act.matr,
            act.corp_empl,
            act.lieu_trav,
            act.ref_empl,
            act.school_year,
            empl.genre
    )

select matr, corp_empl, lieu_trav, school_year, ref_empl, nbremploi, nbrdemission, genre
from extraction
