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
-- Identifier les eleves inscrits dau css
with
    tot as (
        select 
            annee
            , eco
	        , nom_ecole
            , defavorise
            , population
            , ordre_ens
            , niveau_scolaire
            , difficulte
            , is_multi
            , is_spe
            , count(*) as nb_el
        from {{ ref("fact_register_prim") }} as rec
        where type_freq in ('FIN','FRE') -- FIN (=inscriptions au 30/09) / FREQ (=inscriptions ap 30/09)
        group by annee, eco, nom_ecole, defavorise, population, ordre_ens, niveau_scolaire, difficulte, is_multi, is_spe

    -- ajout des elements qui vt nous permettre de calculer les facteurs de pondération
    ), pond as (
        select 
            tot.annee
            , tot.eco
            , tot.nom_ecole
            , tot.defavorise
            , tot.population
            , tot.ordre_ens
            , tot.niveau_scolaire
            , tot.difficulte
            , tot.is_multi
            , tot.is_spe
            , case
                when tot.is_multi = '1' and tot.defavorise = '1' then max.def_multi
                when tot.is_multi = '1' and tot.defavorise = '0' then max.non_def_multi
                when tot.is_multi = '0' and tot.defavorise = '1' then max.def_reg
                when tot.is_multi = '0' and tot.defavorise = '0' then max.non_def_reg
                when tot.ordre_ens in ('1','2') and tot.defavorise = '0' then max.non_def_reg
                when tot.ordre_ens in ('1','2') and tot.defavorise = '1' then max.def_reg
                else null
            end as max_eleve
            , case
                when tot.ordre_ens = '3' and tot.is_spe = 0 then pond.primaire
                when tot.ordre_ens in ('1','2') and tot.is_spe = 0 then pond.presco
                else null
            end as max_eleve_cat
            , tot.nb_el
        from tot
        left join {{ ref("max_primaire") }} as max
            on max.niveau_scolaire = tot.niveau_scolaire
        left join {{ ref("max_el_categorie") }} as pond
            on pond.cod_difficulte = tot.difficulte
    
    -- calcul des effectifs ponderés
    ), pond2 as (
        select 
            annee
            , eco
            , nom_ecole
            , defavorise
            , population
            , ordre_ens
            , niveau_scolaire
            , difficulte
            , is_multi
            , is_spe
            , max_eleve
            , max_eleve_cat
            , nb_el
            , case
                when max_eleve_cat is not null then round((max_eleve * 1.0 / max_eleve_cat) * nb_el, 2)
                else nb_el
            end as nb_el_pond
        from pond
)

select 
    annee
    , eco
    , nom_ecole
    , defavorise
    , population
    , niveau_scolaire
    , sum(nb_el) as nb_el
    , sum(nb_el_pond) as nb_el_pond
from pond2
group by annee, eco, nom_ecole, defavorise, population, niveau_scolaire

