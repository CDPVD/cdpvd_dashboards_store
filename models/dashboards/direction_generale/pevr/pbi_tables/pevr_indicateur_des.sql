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
{{ config(alias="indicateur_des") }}
with
    -- Jumelage du perimetre élèves avec la table mentions
    perimetre as (
        select
            '1' as id_indicateur_cdpvd, -- Indicateur cdpvd
            src.fiche,
            sch.annee,
            sch.annee_scolaire,
            sch.school_friendly_name,
            mentions.mois_sanction,
            mentions.ind_obtention,
            row_number() over (
                partition by src.fiche, sch.annee order by mentions.date_obt_mention desc
            ) as seqid
        from {{ ref("stg_perimetre_eleve_frequentation_des") }} as src
        inner join {{ ref("dim_mapper_schools") }} as sch on src.id_eco = sch.id_eco
        left join
            {{ ref("fact_ri_mentions") }} as mentions
            on src.fiche = mentions.fiche
            and sch.annee = mentions.annee_sanction
        where
            sch.annee
            between {{ core_dashboards_store.get_current_year() }}
            - 3 and {{ core_dashboards_store.get_current_year() }}
            and mentions.indice_des = 1.0  -- dip DES
    ),

    -- Ajout des filtres utilisés dans le tableau de bord.
    _filtre as (
        select
            perim.annee,
            perim.annee_scolaire,
            perim.fiche,
            perim.mois_sanction,
            ind_cdpvd.objectif,
            ind_cdpvd.id_indicateur_cdpvd,
            ind_cdpvd.id_indicateur_css,
            ind_cdpvd.description_indicateur,
            cible_annuelle.cible,
            perim.ind_obtention,
            case
                when perim.school_friendly_name is null then '-'
                else perim.school_friendly_name
            end as school_friendly_name,
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
        from perimetre as perim
        inner join
            {{ ref("fact_yearly_student") }} as y_stud
            on perim.fiche = y_stud.fiche
            and perim.annee = y_stud.annee
        inner join
            {{ ref("dim_eleve") }} as ele on perim.fiche = ele.fiche
        inner join
            {{ ref("pevr_dim_objectif_cdpvd") }} as ind_cdpvd
            on perim.id_indicateur_cdpvd = ind_cdpvd.id_indicateur_cdpvd
        inner join
            {{ ref("pevr_dim_cible_annuelle_cdpvd") }} as cible_annuelle
            on ind_cdpvd.id_indicateur_cdpvd = cible_annuelle.id_indicateur_cdpvd
            and perim.annee_scolaire = cible_annuelle.annee_scolaire
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
            id_indicateur_cdpvd,
            id_indicateur_css,
            description_indicateur,
            cible,
        -- Agg
            count(fiche) nb_resultat,
            CAST(SUM(ind_obtention) as integer) as nb_ind_obtention,
            CAST(ROUND(AVG(ind_obtention), 3) AS FLOAT) AS taux_diplomation,
            CAST(ROUND(AVG(ind_obtention) - cible, 3) AS FLOAT) AS ecart_cible
        from _filtre
        group by
            annee_scolaire,
            id_indicateur_cdpvd,
            id_indicateur_css,
            description_indicateur,
            cible, cube (
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
            id_indicateur_cdpvd,
            id_indicateur_css,
            description_indicateur,
            cible,
        -- Agg
            nb_resultat,
            nb_ind_obtention,
            taux_diplomation,
            ecart_cible,
            LAG(taux_diplomation) OVER (PARTITION BY id_indicateur_cdpvd ORDER BY cast(left(annee_scolaire, 4) as int)) as taux_previous_year
        from agg_dip
    )

select
-- Indicateurs
    id_indicateur_cdpvd,
    id_indicateur_css,
    description_indicateur,
    cible,
-- Agg
    nb_resultat,
    taux_diplomation,
    CONCAT(
        taux_diplomation * 100, '%',
        CHAR(10),
        '(', nb_ind_obtention, '/', nb_resultat, ' él.) '
    ) AS taux_nbEleve,
    ecart_cible,
    CASE 
        WHEN (taux_diplomation >= cible ) THEN 2 -- Vert
        WHEN ( (taux_diplomation < taux_previous_year) AND (taux_diplomation > cible) ) THEN 2 -- Vert
        WHEN ( (taux_diplomation > taux_previous_year) AND (taux_diplomation < cible) ) THEN 1 -- Jaune
        WHEN (taux_diplomation < cible ) THEN 0 -- Rouge
    END AS variation,
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
