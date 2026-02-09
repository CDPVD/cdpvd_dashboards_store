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
{{ config(alias="indicateur_fpt") }}

with
    -- Jumelage du perimetre élèves avec la table mentions
    perimetre as (
        select
            '3' as id_indicateur_cdpvd, -- Indicateur cdpvd
            fpt.fiche,
            fpt.school_friendly_name,
            fpt.Annee_Fpt_1,
            fpt.cohorte as annee_scolaire,
            fpt.cohorte,
            fpt.freq,
            mentions.mois_sanction,
            mentions.ind_obtention
        from {{ ref("stg_perimetre_eleve_frequentation_fpt") }} as fpt
        left join
            {{ ref("fact_ri_mentions") }} as mentions
            on fpt.fiche = mentions.fiche
            and fpt.Annee_Fpt_1 = mentions.annee_sanction
            and mentions.indice_cfpt = 1.0
    ),

    -- Ajout des filtres utilisés dans le tableau de bord.
    _filtre as (
        select
            perimetre.fiche,
            y_stud.annee,
            perimetre.cohorte,
            perimetre.annee_scolaire,
            perimetre.freq,
            perimetre.mois_sanction,
            ind_cdpvd.objectif,
            ind_cdpvd.id_indicateur_cdpvd,
            ind_cdpvd.id_indicateur_css,
            ind_cdpvd.description_indicateur,
            cible_annuelle.cible,
            case
                when perimetre.ind_obtention = 1.0 then 1.0 -- L'élève doit posseder la certification
                else 0.0
            end as is_qualified,
            case
                when perimetre.school_friendly_name is null then '-'
                else perimetre.school_friendly_name
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
        from perimetre
        inner join
            {{ ref("fact_yearly_student") }} as y_stud
            on perimetre.fiche = y_stud.fiche
            and perimetre.Annee_Fpt_1 = y_stud.annee
        inner join
            {{ ref("dim_eleve") }} as ele on perimetre.fiche = ele.fiche
        inner join
            {{ ref("pevr_dim_objectif_cdpvd") }} as ind_cdpvd
            on perimetre.id_indicateur_cdpvd = ind_cdpvd.id_indicateur_cdpvd
        inner join
            {{ ref("pevr_dim_cible_annuelle_cdpvd") }} as cible_annuelle
            on ind_cdpvd.id_indicateur_cdpvd = cible_annuelle.id_indicateur_cdpvd
            and perimetre.annee_scolaire = cible_annuelle.annee_scolaire
    ),

    -- Début de l'aggrégration
    agg_dip as (
        select
        -- Filtre
            annee_scolaire,
            cohorte,
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
            CAST(SUM(is_qualified) as integer) nb_qualified,
            CAST(ROUND(AVG(is_qualified), 3) AS FLOAT) AS taux_qualification_fpt,
            CAST(ROUND(AVG(is_qualified) - cible, 3) AS FLOAT) AS ecart_cible
        from _filtre
        group by
            annee_scolaire,
            cohorte,
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
            cohorte,
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
            nb_qualified,
            taux_qualification_fpt,
            ecart_cible,
            LAG(taux_qualification_fpt) OVER (
            PARTITION BY 
                id_indicateur_cdpvd,
                coalesce(school_friendly_name, 'CSS'),
                coalesce(genre, 'Tout'),
                coalesce(plan_interv_ehdaa, 'Tout'),
                coalesce(population, 'Tout'),
                coalesce(classification, 'Tout'),
                coalesce(distribution, 'Tout')
            ORDER BY cast(left(cohorte, 4) as int)
            ) as taux_previous_year
        from agg_dip
    )

select
-- Indicateurs
    id_indicateur_cdpvd,
    id_indicateur_css,
    description_indicateur,
    cible,
-- Agg
    cohorte,
    Concat(
        'Cohorte', ' ', cohorte,
        CHAR(10), 'Suivie sur 3 ans'
    ) as concat_cohorte,
    nb_resultat,
    nb_qualified,
    taux_qualification_fpt,
    CONCAT(
        taux_qualification_fpt * 100, '%',
        CHAR(10),
        '(', nb_qualified, '/', nb_resultat, ' él.) '
    ) AS taux_nbEleve,
    ecart_cible,
    cast(left(cohorte, 4) as int) as annee,
    CASE 
        WHEN (taux_qualification_fpt >= cible ) THEN 2 -- Vert
        WHEN ( (taux_qualification_fpt < taux_previous_year) AND (taux_qualification_fpt > cible) ) THEN 2 -- Vert
        WHEN ( (taux_qualification_fpt > taux_previous_year) AND (taux_qualification_fpt < cible) ) THEN 1 -- Jaune
        WHEN (taux_qualification_fpt < cible ) THEN 0 -- Rouge
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