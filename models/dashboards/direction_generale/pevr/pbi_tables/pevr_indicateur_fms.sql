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
{{ config(alias="indicateur_fms") }}

with
    -- Jumelage du perimetre élèves avec la table mentions
    perimetre as (
        select
            '2' as id_indicateur_cdpvd, -- Indicateur cdpvd
            sch.annee,
            sch.annee_scolaire,
            src.fiche,
            sch.school_friendly_name,
            mentions.mois_sanction,
            mentions.ind_obtention,
            row_number() over (
                partition by src.fiche, sch.annee order by mentions.date_obt_mention desc
            ) as seqid
        from {{ ref("stg_perimetre_eleve_frequentation_fms") }} as src
        inner join {{ ref("dim_mapper_schools") }} as sch on src.id_eco = sch.id_eco
        left join
            {{ ref("fact_ri_mentions") }} as mentions
            on src.fiche = mentions.fiche
            and sch.annee = mentions.annee_sanction
        where
            sch.annee
            between {{ core_dashboards_store.get_current_year() }}
            - 3 and {{ core_dashboards_store.get_current_year() }}
            and mentions.indice_cfms = 1.0  -- Filtre pour choisir la qualification fms
    ),

    -- Je cherche la cible visée du PEVR
    cible_max as (
        select
            c.id_indicateur_cdpvd,
            c.cible as cible_visee
        from pevr_dim_cible_annuelle_cdpvd c
        join (
            select
                id_indicateur_cdpvd,
                max(annee_scolaire) as annee_scolaire
            from pevr_dim_cible_annuelle_cdpvd
            group by id_indicateur_cdpvd
        ) m
            on c.id_indicateur_cdpvd = m.id_indicateur_cdpvd
        and c.annee_scolaire = m.annee_scolaire
    ),

    -- Ajout des filtres utilisés dans le tableau de bord.
    _filtre as (
        select
            perimetre.annee,
            perimetre.annee_scolaire,
            perimetre.fiche,
            ind_cdpvd.objectif,
            ind_cdpvd.id_indicateur_cdpvd,
            ind_cdpvd.id_indicateur_css,
            ind_cdpvd.description_indicateur,
            cible_annuelle.cible,
            perimetre.ind_obtention,
            cible_max.cible_visee,
            case
                when perimetre.school_friendly_name is null then '-'
                else perimetre.school_friendly_name
            end as school_friendly_name,
            case
                when perimetre.mois_sanction is null then '-'
                else perimetre.mois_sanction
            end as mois_sanction,
            case 
                when ele.genre is null then '-' 
                else ele.genre 
            end as genre,
            case
                when y_stud.plan_interv_ehdaa is null then '-'
                else y_stud.plan_interv_ehdaa
            end as plan_interv_ehdaa,
            case
                when y_stud.population is null then '-' 
                else y_stud.population
            end as population,
            case
                when y_stud.class is null then '-' 
                else y_stud.class
            end as classification,
            case 
                when y_stud.dist is null then '-' 
                else y_stud.dist 
            end as distribution,
            case
                when y_stud.grp_rep is null then '-' 
                else y_stud.grp_rep
            end as groupe_repere
        from perimetre
        inner join
            {{ ref("fact_yearly_student") }} as y_stud
            on perimetre.fiche = y_stud.fiche
            and perimetre.annee = y_stud.annee
        inner join
            {{ ref("dim_eleve") }} as ele on perimetre.fiche = ele.fiche
        inner join
            {{ ref("pevr_dim_objectif_cdpvd") }} as ind_cdpvd
            on perimetre.id_indicateur_cdpvd = ind_cdpvd.id_indicateur_cdpvd
        inner join
            {{ ref("pevr_dim_cible_annuelle_cdpvd") }} as cible_annuelle
            on ind_cdpvd.id_indicateur_cdpvd = cible_annuelle.id_indicateur_cdpvd
            and perimetre.annee_scolaire = cible_annuelle.annee_scolaire
        left join cible_max
            on ind_cdpvd.id_indicateur_cdpvd = cible_max.id_indicateur_cdpvd
        where seqid = 1
    ),

    -- Début de l'aggrégration
    agg_dip as (
        select
        -- Filtre
            annee_scolaire,
            mois_sanction,
            school_friendly_name,
            genre,
            plan_interv_ehdaa,
            population,
            classification,
            distribution,
            groupe_repere,
        -- Indicateurs
            id_indicateur_css,
            description_indicateur,
            cible,
            cible_visee,
        -- Agg
            count(fiche) nb_resultat,
            CAST(SUM(ind_obtention) as integer) as nb_ind_obtention,
            CAST(ROUND(AVG(ind_obtention), 3) AS FLOAT) AS taux_qualification_fms,
            CAST(ROUND(AVG(ind_obtention) - cible, 3) AS FLOAT) AS ecart_cible
        from _filtre
        group by
            annee_scolaire,
            id_indicateur_css,
            description_indicateur,
            cible, cible_visee, cube (
                mois_sanction,
                school_friendly_name,
                genre,
                plan_interv_ehdaa,
                population,
                classification,
                distribution,
                groupe_repere
            )
    ),

    -- Coalesce pour crée le choix 'Tout' dans les filtres.
    _coalesce as (
        select
        -- Filtre
            annee_scolaire,
            coalesce(school_friendly_name, 'CSS') as ecole,
            coalesce(mois_sanction, 'Tout') as mois_sanction,
            coalesce(genre, 'Tout') as genre,
            coalesce(plan_interv_ehdaa, 'Tout') as plan_interv_ehdaa,
            coalesce(population, 'Tout') as population,
            coalesce(classification, 'Tout') as classification,
            coalesce(distribution, 'Tout') as distribution,
            coalesce(groupe_repere, 'Tout') as groupe_repere,
        -- Indicateurs
            id_indicateur_css,
            description_indicateur,
            cible,
            cible_visee,
        -- Agg
            nb_resultat,
            nb_ind_obtention,
            taux_qualification_fms,
            ecart_cible
        from agg_dip
    )

select
-- Indicateurs
    id_indicateur_css,
    description_indicateur,
    cible,
    cible_visee,
-- Agg
    nb_resultat,
    nb_ind_obtention,
    taux_qualification_fms,
    CONCAT(
        taux_qualification_fms * 100, '%',
        CHAR(10),
        '(', nb_ind_obtention, '/', nb_resultat, ' él.) '
    ) as taux_nbEleve,
    case
        when ecart_cible >= 0 then 'V'
        when ecart_cible >= -5 then 'J'
        else 'R'
    end as ecart_cible_couleur,
    {{
        dbt_utils.generate_surrogate_key(
            [
                "ecole",
                "annee_scolaire",
                "mois_sanction",
                "plan_interv_ehdaa",
                "genre",
                "population",
                "classification",
                "distribution",
                "groupe_repere",
            ]
        )
    }} as id_filtre
from _coalesce