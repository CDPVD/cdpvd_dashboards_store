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
            core_dashboards_store.create_nonclustered_index(
                "{{ this }}", ["corp_empl", "stat_eng", "type_qualif"]
            ),
            core_dashboards_store.create_clustered_index(
                "{{ this }}", ["matr", "workplace", "etat_empl"]
            ),
        ]
    )
}}
/***************************************************************************************************
    Étape 1
***************************************************************************************************/
with
    e1 as (
        select
            util.matr,  -- Matricule
            emp.etat as etat_empl,  -- Code de létat demploi
            emp.lieu_trav as workplace,  -- Code du lieu de travail
            emp.stat_eng,  -- Code du statut dengagement
            emp.corp_empl,  -- Corp demploi
            qa.type_qualif as type_qualif,  -- Qualification / Certification
            qa.date_expir, -- Date d'expiration de la qualification
            row_number() over (
                partition by util.matr, qa.type_qualif order by util.matr, qa.type_qualif, qa.date_expir desc
            ) as row_number
        from {{ ref("dim_employees") }} as util
        inner join {{ ref("i_pai_dos_empl") }} as emp on util.matr = emp.matr
        inner join {{ ref("etat_empl") }} as etat on emp.etat = etat.etat_empl
        inner join {{ ref("fact_activity_current") }} as ca on util.matr = ca.matr  -- Employé actif ds paie

        -- LEFT JOIN Aller chercher les qualifications valides (date_expir NULL ou
        -- future)
        left join
            {{ ref("i_pai_qualif") }} as qa
            on util.matr = qa.matr
            and (qa.date_expir is null or qa.date_expir >= getdate())

        where
            etat.etat_actif = 1  -- Si l'employé est actif
            and emp.ind_empl_princ = 1  -- Prendre en considération uniquement son emploi principal
            and emp.corp_empl like '3%'  -- Enseignant(e)

        group by
            util.matr,
            emp.etat,
            emp.lieu_trav,
            emp.stat_eng,
            emp.corp_empl,
            qa.type_qualif,
            qa.date_expir
    )

/***************************************************************************************************
    Étape 2
***************************************************************************************************/
select matr, etat_empl, workplace, stat_eng, corp_empl, type_qualif, date_expir
from e1
where row_number = 1
