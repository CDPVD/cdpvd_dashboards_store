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

with perimetre as (
    select
        perimetre.code_perm,
        perimetre.Fiche,
        MIN(perimetre.id_eco) as id_eco_fpt_1,
        MIN(perimetre.Annee) as Annee_Fpt_1 -- Prend l'année minimum si 2 année de suite en secondaire
    from {{ ref("fact_yearly_student") }} as perimetre
    inner join {{ ref("i_e_freq") }} as freq
        on perimetre.fiche = freq.fiche and perimetre.annee = freq.annee
    where freq.type_freq = 'FIN' -- On veut la fréquentation avant le 30 septembre.
    and perimetre.type_parcours = '07' -- FPT
    and perimetre.is_doubleur = 0 -- On veut l'année la moins récente de l'élève
	and perimetre.Annee between {{ core_dashboards_store.get_current_year() }} - 5 and {{ core_dashboards_store.get_current_year() }} 
	GROUP BY perimetre.fiche, perimetre.code_perm
),

_parcours as (
	Select
        perimetre.code_perm,
		perimetre.Fiche,
        perimetre.id_eco_fpt_1,
		Annee_Fpt_1,
        -- Mesure qui tag les élèves dont le parcours a déjà commencé avant 2021
        CASE
            WHEN EXISTS (
            SELECT 1
            FROM {{ ref("fact_yearly_student") }} y_stud
            WHERE y_stud.fiche = perimetre.fiche
                AND y_stud.annee < perimetre.Annee_Fpt_1
                AND y_stud.type_parcours = '07' --FPT
            ) THEN 1
            ELSE 0
        END AS had_fpt_before_annee_fpt_1,
		CASE
			WHEN MAX(fact.annee) = {{ core_dashboards_store.get_current_year() }} + 1 -- Année prévisionnelle existante
			THEN {{ core_dashboards_store.get_current_year() }}
		ELSE MAX(fact.annee)
		END AS Annee_Fin_Parcours -- On veut l'année la plus récente de l'élève. (Son parcours)
	FROM perimetre
	inner join 
        {{ ref("fact_yearly_student") }} as fact 
        ON perimetre.fiche = fact.fiche
	GROUP BY perimetre.code_perm, perimetre.fiche, perimetre.id_eco_fpt_1, Annee_Fpt_1
),

-- Création de la cohorte par rapport à la première année du FPT
_cohorte as (
	Select
        code_perm,
		Fiche,
        id_eco_fpt_1,
		Annee_Fpt_1,
		Annee_Fin_Parcours,
		CASE
			WHEN Annee_Fpt_1 = Annee_Fpt_1 THEN CONCAT(Annee_Fpt_1, '-' ,Annee_Fpt_1 + 1)
			ELSE Convert(varchar, Annee_Fpt_1)
		END AS Cohorte
	from _parcours
    where had_fpt_before_annee_fpt_1 = 0 --Exclus les élèves dont le parcours a déjà commencé avant 2021
),

--Nombre de fréquentation depuis la 1er cohorte selon l'année
Frequentation as (
	Select
        code_perm,
		Fiche,
		Cohorte,
        id_eco_fpt_1,
		Annee_Fpt_1,
		Annee_Fin_Parcours,
		SUM(Annee_Fin_Parcours - Annee_Fpt_1 + 1) as Freq --Inclus l'année de départ (donc, +1)
	from _cohorte
	group by
        code_perm,
		Fiche,
        id_eco_fpt_1,
		Cohorte,
		Annee_Fpt_1,
		Annee_Fin_Parcours
)

select 
    code_perm,
    Fiche,
    Cohorte,
    school_friendly_name,
    Annee_Fpt_1,
    Annee_Fin_Parcours,
    Freq
from Frequentation
inner join 
    {{ ref("dim_mapper_schools") }} as sch on id_eco_fpt_1 = sch.id_eco
where Annee_Fpt_1 < {{ core_dashboards_store.get_current_year() }} - 2 -- Cohorte des 3 dernières années dont les données sont disponible. Exlcus l'année scolaire en cours.
