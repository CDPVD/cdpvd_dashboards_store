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
{{ config(alias="fact_absence_consecutive") }}

with
    absence as (
        select distinct -- ** Vérifier si important
        liste.*, -- Tous les champs de fact_liste_absence
        jour_sem, -- Jour de la semaine (0,1,2,3,4,5,6)
        bal_jour_ouv, -- Jour de l'année scolaire
        ta.categories
        from {{ ref("fact_liste_absence") }} as liste
        inner join
            {{ ref("i_pai_tab_cal_jour") }} as cal
            on liste.annee = cal.an_budg
            and liste.gr_paie = cal.gr_paie
            and liste.date = cal.date_jour
        inner join
            {{ ref("type_absence") }} as ta  -- À modifier
            on liste.motif_abs = ta.motif_id                    
    ),

    -- Calculer la différence entre chacune des absences
    diffAbs as (
        select
            annee,
            duree,
            gr_paie,
            matricule,
            absence.corp_empl,
            categories,
            lieu_trav,
            bal_jour_ouv,
            reg_abs,
            date,
            absence.ref_empl,
            pourc_sal,
            datediff(
                day,
                lag(date) over (
                    partition by annee, matricule, categories, lieu_trav order by date, bal_jour_ouv
                ),
                date
            ) as diff_days,
            bal_jour_ouv - lag(bal_jour_ouv) over (
                partition by annee, matricule, categories, lieu_trav  order by date, bal_jour_ouv
            ) as diff_bal_jour,
            datepart(weekday, date) as weekday
        from absence
    ),

    -- Permet d'identifier les absences consécutives en ayant le même motif
    regroupement as (
        select
            annee,
            matricule,
            corp_empl,
            gr_paie,
            categories,
            lieu_trav,
            bal_jour_ouv,
            date,
            diff_days,
            diff_bal_jour,
            weekday,
            duree,
            ref_empl,
            reg_abs,
            pourc_sal,
            sum(
                case -- ** D'autres conditions possibles?
                    when diff_days is null
                    then 0  -- Première instance
                    when diff_days = 3 and weekday = 2
                    then 0  -- Fin de semaine
                    when diff_days > 1
                    then 1  -- Nouveau groupe, en fonction de l'écart
                    else 0
                end
            ) over (
                partition by annee, matricule, categories, lieu_trav
                order by date
                rows unbounded preceding
            ) as group_id
        from diffAbs
    ),

    -- Permet d'avoir les absences consécutive en une seule
    absence_consecutive as (
        select
            annee,
            matricule,
            corp_empl,
            gr_paie,
            lieu_trav,
            group_id,
            ref_empl,
            min(date) as startdate,
            max(date) as enddate,
            count(*) as nombre_jours,
            duree,
            reg_abs,
            pourc_sal,
            categories
        from regroupement
        group by
            annee,
            matricule,
            ref_empl,
            categories,
            lieu_trav,
            group_id,
            gr_paie,
            duree,
            pourc_sal,
            reg_abs,
            corp_empl
    )

select 
            annee,
            matricule,
            corp_empl,
            gr_paie,
            lieu_trav,
            group_id,
            ref_empl,
           startdate,
            enddate,
           nombre_jours,
            duree,
            reg_abs,
            pourc_sal,
            categories



from absence_consecutive
