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
        MIN(perimetre.Annee) as Annee_Sec_1 -- Prend l'année minimum si 2 année de suite en secondaire 1 sans ind_doubleur à 1
    from {{ ref("fact_yearly_student") }} as perimetre
    inner join {{ ref("i_e_freq") }} as freq
        on perimetre.fiche = freq.fiche and perimetre.annee = freq.annee
    where freq.type_freq = 'FIN' -- On veut la fréquentation avant le 30 septembre.
    and perimetre.ordre_ens = 4 -- Secondaire
    and perimetre.niveau_scolaire = 'Sec 1' -- Secondaire 1
    and perimetre.is_doubleur = 0 -- On veut l'année la moins récente de l'élève en secondaire 1
	and perimetre.Annee between {{ core_dashboards_store.get_current_year() }} - 7 and {{ core_dashboards_store.get_current_year() }}
	GROUP BY perimetre.fiche, perimetre.code_perm
),

_parcours as (
	Select
        perimetre.code_perm,
		perimetre.Fiche,
		Annee_Sec_1,
		CASE
			WHEN MAX(fact.annee) = {{ core_dashboards_store.get_current_year() }} + 1 -- Année prévisionnelle existante
			THEN {{ core_dashboards_store.get_current_year() }}
		ELSE MAX(fact.annee)
		END AS Annee_Courant -- On veut l'année la plus récente de l'élève. (Son parcours)
	FROM perimetre
	INNER JOIN {{ ref("fact_yearly_student") }} as fact ON perimetre.fiche = fact.fiche
	GROUP BY perimetre.code_perm, perimetre.fiche, Annee_Sec_1
),

-- Création de la cohorte par rapport à l'année du 1er secondaire.
_cohorte as (
	Select
        code_perm,
		Fiche,
		Annee_Sec_1,
		Annee_Courant,
		CASE
			WHEN Annee_Sec_1 = Annee_Sec_1 THEN CONCAT(Annee_Sec_1, '-' ,Annee_Sec_1 + 1)
			ELSE Convert(varchar, Annee_Sec_1)
		END AS Cohorte
	from _parcours
),

--Nombre de fréquentation depuis la 1er cohorte selon l'année
Frequentation as (
	Select
        code_perm,
		Fiche,
		Cohorte,
		Annee_Sec_1,
		Annee_Courant,
		SUM(Annee_Courant - Annee_Sec_1 + 1) as Freq --Inclus l'année de départ (donc, +1)
	from _cohorte
	group by
        code_perm,
		Fiche,
		Cohorte,
		Annee_Sec_1,
		Annee_Courant
)

select * from Frequentation