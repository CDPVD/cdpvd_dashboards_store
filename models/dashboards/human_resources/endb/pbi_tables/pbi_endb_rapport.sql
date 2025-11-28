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



{{ config
    (
        post_hook=[
            core_dashboards_store.create_nonclustered_index(
                "{{ this }}", 
                ["nom_legal","tranche_age","genre"]
            ),
            core_dashboards_store.create_clustered_index(
                "{{ this }}", 
                ["matricule", "lieu_principal","ordre_Ens", "statut_enseignant", "qualification"]
            ),
            core_dashboards_store.stamp_model("dashboard_endb")           
        ]
    )
}}

select
    ens.matr as matricule,
    emp.legal_name as 'nom_legal',
    emp.sex_friendly_name as genre,
    case
        when datediff(year, emp.birth_date, getdate()) < 25 then '24 ans et moins'
        when datediff(year, emp.birth_date, getdate()) >= 25 and datediff(year, emp.birth_date, 
        getdate()) < 35 then '25 à 34 ans'
        when datediff(year, emp.birth_date, getdate()) >= 35 and datediff(year, emp.birth_date, 
        getdate()) < 45 then '35 à 44 ans'
        when datediff(year, emp.birth_date, getdate()) >= 45 and datediff(year, emp.birth_date, 
        getdate()) < 55 then '45 à 54 ans'
        when datediff(year, emp.birth_date, getdate()) >= 55 and datediff(year, emp.birth_date, 
        getdate()) < 65 then '55 à 64 ans'
        when datediff(year, emp.birth_date, getdate()) >= 65 then '65 ans et plus'
    end as 'tranche_age',
    lieu.workplace_name as lieu_principal,
    job_class.code_job_name as corp_empl,
    case when qualif.code is null then 'Aucune' else qualif.descr end as qualification,
    state.descr as etat_empl,
    case when emp.sex_friendly_name = 'femme' then 1 end as Femme,
    case when emp.sex_friendly_name = 'homme' then 1 end as Homme,
    case when qualif.is_qualified = 1 then 1 ELSE 0 end as edb,
    CASE WHEN qualif.is_qualified IS NULL OR qualif.is_qualified != 1 THEN 1 ELSE 0 END AS endb,
    statut_ens.statut as statut_enseignant,
    sect.ordre_Ens,
    sect.secteur_Descr as secteur
from {{ ref("fact_endb_liste") }} as ens

inner join {{ ref("dim_employees") }} as emp on ens.matr = emp.matr
inner join {{ ref("dim_mapper_workplace") }} lieu on ens.workplace = lieu.workplace
inner join {{ ref("etat_empl") }} state on ens.etat_empl = state.etat_empl

inner join {{ ref("dim_mapper_job_class") }} as job_class on job_class.code_job = ens.corp_empl
inner join {{ ref("statut_enseignant") }} as statut_ens on ens.stat_eng = statut_ens.code

-- Left join pour aller chercher le secteur et l'ordre d'enseignement left au cas ou l'UA ne se troueve pas dans la table secteur
left join {{ ref("secteur") }} AS sect on ens.workplace = sect.lieu_trav

-- LEFT JOIN requis pour assurer une bonne représentation de la population
left join {{ ref("ens_qualification") }} qualif on ens.type_qualif = qualif.code    