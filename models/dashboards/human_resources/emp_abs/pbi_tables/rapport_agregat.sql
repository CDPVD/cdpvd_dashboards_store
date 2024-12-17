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
{{ config(alias="emp_abs_report_final") }}

select distinct
    LEFT(annee,4) as annee,
    matricule,
    -- ref_empl,
    -- gr_paie,
    genre,
    corp_empl,
    categories,
    abs.lieu_trav,
    nbr_jour,
    lundi,
    mardi,
    mercredi,
    jeudi,
    vendredi,
    taux,
    sec.secteur_Descr as secteur,
    tranche_age,
    jour_trav,
    wk.workplace_name as lieu_nom,
    nbr,
    jg.job_group_category as cat_emp,
    {{
        dbt_utils.generate_surrogate_key(
            ["annee", "abs.corp_empl", "lieu_trav", "tranche_age", "genre"]
        )
    }} as filter_key     
from {{ ref("fact_agregat") }} as abs

inner join {{ ref("dim_mapper_workplace") }} as wk on abs.lieu_trav = wk.workplace

inner join {{ ref("secteur") }} as sec on abs.lieu_trav = sec.ua_id
inner join {{ ref("dim_mapper_job_group") }} as jg on abs.corp_empl = jg.job_group
