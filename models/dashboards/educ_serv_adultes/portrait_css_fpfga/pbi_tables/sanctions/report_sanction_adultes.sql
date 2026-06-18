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
{{
    config(
        post_hook=[
            core_dashboards_store.stamp_model("dashboard_portrait_css_fpfga")
        ]
    )
}}
{%- set source_relation = adapter.get_relation(
    database=target.database,
    schema=target.schema + "_dashboard_portrait_css_fpfga_seeds",
    identifier="depassement_heure_mat",
) -%}
{% set table_exists = source_relation is not none %}
{% set depassement_par_defaut = var("dashboards", {}).get("educ_serv_adultes", {}).get("portrait_css_fpfga", {}).get("depassement_par_defaut", "150") %}

{% if table_exists %}
    {% set table_seed = "{{ ref('depassement_heure_mat') }}" %}
    {% if execute %}
        {{
            log(
                "La seed 'depassement_heure_mat' existe. Les matieres de la seed auront des valeurs personnalisées sinon la valeur par défaut de " ~ depassement_par_defaut ~ " sera utilisée.",
                true,
            )
        }}
    {% endif %}
{% else %}
    {% if execute %}
        {{ log("La seed 'depassement_heure_mat' n'existe pas. La valeur par défaut de " ~ depassement_par_defaut ~ " sera utilisée pour toutes les matières. Elle est configurable dans les variables du projet.", true) }}
    {% endif %}
{% endif %}

with
    cte as (
        select *, round(cast(nbminrea as float) / 60, 2) as nbhresrea
        from {{ ref("fac_reussi_sanction_heure_adultes") }} facr
    ),
    depasse as (
        select
            cte.*,
            {%- if table_exists %}
                case
                    when
                        dep.depassement is null
                        and right(ltrim(rtrim(mat)), 1) like '[0-9]'
                    then right(ltrim(rtrim(mat)), 1) * 25
                    when
                        dep.depassement is null
                        and right(ltrim(rtrim(mat)), 1) like '[^0-9]'
                    then {{ depassement_par_defaut }}
                    else dep.depassement
                end as depassement
            {% else -%}
                case
                    when right(ltrim(rtrim(mat)), 1) like '[0-9]'
                    then right(ltrim(rtrim(mat)), 1) * 25
                    else {{ depassement_par_defaut }}
                end as depassement
            {% endif -%}
        from cte
        {%- if table_exists %}
            left join {{ source_relation }} dep on cte.mat = dep.matieres
        {% endif -%}
    ),
    compt_depass as (
        select
            *,
            case
                when nbhresrea > depassement then nbhresrea - depassement else 0
            end as depassement_heure,
            case when nbhresrea > depassement then 1 else 0 end as nbre_ele_depasse
        from depasse
    )
select *, case when nbre_ele_depasse = 1 then 'Oui' else 'Non' end as 'En dépassement'
from compt_depass
