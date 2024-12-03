select
    util.matr,                      -- Matricule
    emp.etat as etat_empl,          -- Code de létat demploi
    emp.lieu_trav as workplace,     -- Code du lieu de travail
    emp.stat_eng,                   -- Code du statut dengagement
    emp.corp_empl,                  -- Corp demploi
    qa.type_qualif                  -- Qualification / Certification

from {{ ref("dim_employees") }} as util
inner join {{ ref("i_pai_dos_empl") }} as emp 
    on util.matr = emp.matr
inner join {{ ref("etat_empl") }}  as etat 
    on emp.etat = etat.etat_empl

-- LEFT JOIN requis pour assurer une bonne représentation de la population
left join {{ ref("i_pai_qualif") }} as qa 
    on util.matr = qa.matr
inner join {{ ref("fact_activity_current") }} as ca 
    on util.matr = ca.matr

where
    etat.etat_actif = 1             -- Si l'employé est actif
    and emp.ind_empl_princ = 1      -- Prendre en considération uniquement sont emploi principal
    and emp.corp_empl like '3%'     -- Enseignant(e)