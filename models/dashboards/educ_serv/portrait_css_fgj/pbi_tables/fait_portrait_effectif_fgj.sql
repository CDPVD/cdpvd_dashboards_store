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
            core_dashboards_store.stamp_model("dashboard_portrait_css_fgj")
        ]
    )
}}

with
src as (
select src.code_perm
      ,src.id_eco
      ,annee
      ,src.fiche
      ,dim.genre
      ,nom
      ,prenom
      ,nom_prenom_fiche
      ,date_naissance
      ,code_lieu_naiss_ele
      ,desc_code_lieu_naiss_ele
      ,lieu_naiss
      ,langue_mater
      ,langue_maison
      ,desc_langue_mater
      ,code_post
      ,ville
      ,tel
      ,case
        when population = 'prescolaire'
        then 'Prescolaire'
        when population = 'primaire_reg'
        then 'Primaire régulier'
        when population = 'primaire_adapt'
        then 'Primaire adapté'
        when population = 'secondaire_reg'
        then 'Secondaire régulier'
        when population = 'secondaire_adapt'
        then 'Secondaire adapté'
        else population
    end as population
      ,eco
      ,coalesce(passp.code_perm, null) as is_passepartout
      ,grp_rep
      ,dist
      ,nom_ecole
      ,ordre_ens
      ,classe
      ,class
      ,cycle_ref
      ,annee_cycle_ref
      ,plan_interv_ehdaa
      ,case
        when niveau_scolaire = 'mat 4'
        then 'Maternelle 4'
        when niveau_scolaire = 'mat 5'
        then 'Maternelle 5'
        when niveau_scolaire = 'Prim 1'
        then 'Primaire 1'
        when niveau_scolaire = 'Prim 2'
        then 'Primaire 2'
        when niveau_scolaire = 'Prim 3'
        then 'Primaire 3'
        when niveau_scolaire = 'Prim 4'
        then 'Primaire 4'
        when niveau_scolaire = 'Prim 5'
        then 'Primaire 5'
        when niveau_scolaire = 'Prim 6'
        then 'Primaire 6'
        else niveau_scolaire
    end as niveau_scolaire
      ,difficulte
      ,is_doubleur
      ,age_30_sept
      ,type_parcours
      ,type_programme_particulier
      ,categorie_programme_particulier
      ,is_ppp
      ,type_mesure
      ,is_francisation
      ,description_type_mesure
      ,grille from {{ ref("fact_yearly_student") }} src
left join {{ ref("dim_eleve_fgj") }} as dim on src.fiche = dim.fiche
left join
            {{ ref("stg_check_passepartout") }} as passp
            on passp.code_perm = src.code_perm
            and passp.id_eco = src.id_eco 
    )
select code_perm
      ,id_eco
      ,annee
      ,fiche
      ,population
      ,eco
      ,is_passepartout
      ,grp_rep
      ,dist
      ,nom_ecole
      ,ordre_ens
      ,classe
      ,class
      ,cycle_ref
      ,annee_cycle_ref
      ,plan_interv_ehdaa
      ,case
            when is_passepartout is not null
                then 'Passe-Partout'
                else niveau_scolaire
        end as niveau_scolaire
      ,difficulte
      ,is_doubleur
      ,age_30_sept
      ,type_parcours
      ,type_programme_particulier
      ,categorie_programme_particulier
      ,is_ppp
      ,type_mesure
      ,is_francisation
      ,description_type_mesure
      ,grille
      ,nom
      ,prenom
      ,nom_prenom_fiche
      ,date_naissance
      ,code_lieu_naiss_ele
      ,desc_code_lieu_naiss_ele
      ,lieu_naiss
      ,langue_mater
      ,langue_maison
      ,desc_langue_mater
      ,code_post
      ,ville
      ,tel
      ,genre
      from src