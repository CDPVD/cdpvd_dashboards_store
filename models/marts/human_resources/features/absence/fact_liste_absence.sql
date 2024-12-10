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
{{ config(alias="fact_liste_absence") }}

select
    case -- Année format des calendriers ** Meilleure approche?
        -- Si mois courant entre janvier et juin inclusivement
        when month(absence. [date]) >= 1 and month(absence. [date]) < 7 
        then concat(year(absence. [date]) - 1, '', year(absence. [date]))
        else concat(year(absence. [date]), '', year(absence. [date]) + 1)
    end as [annee], -- Année format des calendriers
    absence. [matr] as [matricule], -- Matricule
    absence. [date], -- Date d'absence
    absence.mot_abs as motif_abs, -- Motif d'absence
    [dure] * emp.pourc_sal as [duree], -- Durée en fonction de la tâche de travail
    absence.lieu_trav as lieu_trav, -- Lieu de travail
    emp.pourc_sal, -- Pourcentage du salaire
    emp.gr_paie, -- Groupe de paie
    absence.ref_empl, -- Référence d'emploi
    absence.reg_abs, -- Règle d'absence
    absence.corp_empl -- Corp d'emploi
from {{ ref("i_pai_habs") }} as absence

inner join
    {{ ref("i_paie_hemp") }} as emp -- Historique des emplois
    on absence.matr = emp.matr
    and absence.ref_empl = emp.ref_empl
    and absence.corp_empl = emp.corp_empl
    and absence.date between emp.date_eff and emp.date_fin
    and absence.sect = emp.sect

where
    year(absence.date) >= {{ get_current_year() - 4 }} -- Retour 5 ans en arrière
    and dure != 0 -- Dure non égale à 0
    and emp.pourc_sal != 0 -- Pour_sal non égale à 0
    and absence.mot_abs != '05' -- Création d'une seed **
    and absence.mot_abs != '09'
    and absence.mot_abs != '13'
    and absence.mot_abs != '14'
    and absence.mot_abs != '16'