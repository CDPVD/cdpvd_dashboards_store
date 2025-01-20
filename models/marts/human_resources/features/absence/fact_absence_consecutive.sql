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
            dure,
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
            dure,
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
            dure,
            ref_empl,
            min(date) as startdate,
            max(date) as enddate,
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
            pourc_sal,
            reg_abs,
            corp_empl,
            dure
    ),

abd as ( select 
            ac.annee,
            ac.matricule,
            ac.corp_empl,
            ac.gr_paie,
            ac.lieu_trav,
            ac.group_id,
            ac.ref_empl,
            ac.startdate,
             ac.enddate,
            ac.reg_abs,
            ac.pourc_sal,
            ac.categories,
            dure,
        CAST(ROUND(SUM(CASE WHEN cal.jour_sem = 1 THEN 1.0 / 7 ELSE 0 END), 0) AS INT) AS lundi,    
        CAST(ROUND(SUM(CASE WHEN cal.jour_sem = 2 THEN 1.0 / 7 ELSE 0 END), 0) AS INT) AS mardi,    
        CAST(ROUND(SUM(CASE WHEN cal.jour_sem = 3 THEN 1.0 / 7 ELSE 0 END), 0) AS INT) AS mercredi,
        CAST(ROUND(SUM(CASE WHEN cal.jour_sem = 4 THEN 1.0 / 7 ELSE 0 END), 0) AS INT) AS jeudi,    
        CAST(ROUND(SUM(CASE WHEN cal.jour_sem = 5 THEN 1.0 / 7 ELSE 0 END), 0) AS INT) AS vendredi,
        CAST(ROUND(SUM(CASE WHEN cal.jour_sem != 0 AND cal.jour_sem != 6 THEN 1.0 / 7 ELSE 0 END), 0) AS INT) AS nbr_jours
 from absence_consecutive as ac
    INNER JOIN {{ ref("i_pai_tab_cal_jour") }} AS cal 
        ON cal.date_jour BETWEEN ac.startdate AND ac.enddate 
    group by
            ac.annee,
            ac.matricule,
            ac.corp_empl,
            ac.gr_paie,
            ac.lieu_trav,
            ac.group_id,
            ac.ref_empl,
            ac.startdate,
             ac.enddate,
            ac.reg_abs,
            ac.pourc_sal,
            ac.categories,
            dure
 ),



 test as (
    SELECT
        annee,
        matricule,
        corp_empl,
        gr_paie,
        lieu_trav,
        group_id,
        ref_empl,
        startdate,
        enddate,
        categories,
        SUM(dure) AS total_dure,
        nbr_jours,
        lundi,
        mardi,
        mercredi,
        jeudi,
        vendredi,
        reg_abs,
        pourc_sal,
        (nbr_jours * ht.heures) / 7 as absence_normalisee,
        CAST(((pourc_sal * SUM(dure)) * nbr_jours) AS FLOAT) / 100.0 AS duree_abs,
        CAST(((pourc_sal * SUM(dure)) * ((nbr_jours * ht.heures) / 7)) AS FLOAT) / 100.0 AS duree_abs_n,
        left(abd.corp_empl,4) as asdfsdf
FROM abd
inner join {{ ref('heures_travaillees') }} as ht
ON ht.cat_emploi = left(abd.corp_empl,1)
GROUP BY 
    annee,
    matricule,
    corp_empl,
    gr_paie,
    lieu_trav,
    categories,
    group_id,
    ref_empl,
    startdate,
    enddate,
    nbr_jours,
    lundi,
    mardi,
    mercredi,
    jeudi,
    vendredi,
    reg_abs,
    pourc_sal,
    ht.heures
)

select distinct
            fac.annee,
            matricule,
            corp_empl,
            fac.gr_paie,
            lieu_trav,
            group_id,
            ref_empl,
           startdate,
            enddate,
            total_dure as absence_duree,
            nbr_jours as absence_jours,
            absence_normalisee,
            lundi, mardi, mercredi, jeudi, vendredi,
            reg_abs,
            pourc_sal,
            categories,
            jr_tr.jour_trav,
            duree_abs AS absence_jour_duree,
            duree_abs_n AS absence_jour_duree_normalisee,
        ROUND(COALESCE(CAST(((pourc_sal * total_dure) * nbr_jours) AS FLOAT) / 100.0, 0) / NULLIF(CAST(jr_tr.jour_trav AS FLOAT), 0),24) AS taux,
        ROUND(COALESCE(CAST(((pourc_sal * total_dure) * absence_normalisee) AS FLOAT) / 100.0, 0) / NULLIF(CAST(jr_tr.jour_trav AS FLOAT), 0),24) AS taux_normalise
from test as fac
        INNER JOIN {{ ref("fact_nbr_jours_travailles") }} AS jr_tr 
        ON fac.annee = jr_tr.an_budg AND fac.gr_paie = jr_tr.gr_paie
        --where rn = 1
        group by
            fac.annee,
            fac.matricule,
            fac.corp_empl,
            fac.gr_paie,
            fac.lieu_trav,
            fac.group_id,
            fac.ref_empl,
            fac.startdate,
            fac.enddate,
            fac.total_dure,
            fac.nbr_jours,
            lundi, mardi, mercredi, jeudi, vendredi,
            fac.reg_abs,
            fac.pourc_sal,
            fac.categories,
            jr_tr.jour_trav,
            duree_abs,
            duree_abs_n,
            absence_normalisee
