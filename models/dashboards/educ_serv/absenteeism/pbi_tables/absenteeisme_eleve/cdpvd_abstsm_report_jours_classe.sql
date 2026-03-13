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
{#
    Calendrier des jours de classe enrichi.
    
    Crée un calendrier complet des jours d'école par établissement et grille,
    enrichi avec des métriques mensuelles (MTD) et annuelles (YTD), ainsi que
    des indicateurs d'« année scolaire en cours » et « mois en cours » pour
    faciliter les analyses comparatives.
#}

{{ config(alias="cdpvd_report_jours_classe") }}

{% if execute %}
    {% if "nbre_annee_a_extraire" in var("dashboards")["absenteeism"] %}
        {% set nbre_annee_a_extraire = var("dashboards")["absenteeism"][
            "nbre_annee_a_extraire"
        ] %}
        {{
            log(
                "Le nombre d'années de données à extraire pour le tableau de bord d'absentéisme est : "
                ~ nbre_annee_a_extraire,
                true,
            )
        }}
    {% else %}
        {% set nbre_annee_a_extraire = 5 %}
        {{
            log(
                "Le nombre d'années de données à extraire pour le tableau de bord d'absentéisme est par défaut : "
                ~ nbre_annee_a_extraire,
                true,
            )
        }}
    {% endif %}
    {% set years_of_data_absences = var("marts")["educ_serv"]["recency"][
        "years_of_data_absences"
    ] %}
    {{
        log(
            "Le nombre d'années de données à extraire pour le comptoir d'absentéisme est : "
            ~ years_of_data_absences,
            true,
        )
    }}
{% endif %}

-- ============================================================================
-- ÉTAPE 1: Calendrier des jours d'école
-- ============================================================================
-- Extrait tous les jours avec cycle pédagogique (jour_cycle IS NOT NULL)
-- pour chaque établissement et grille, limité aux N dernières années.
-- ============================================================================

with
    expected_cal as (
        select
            annee,
            cal.id_eco,
            grille,
            date_evenement
        from {{ ref("i_gpm_t_cal") }} as cal
        inner join {{ ref("i_gpm_t_eco") }} as eco
            on cal.id_eco = eco.id_eco
        where jour_cycle is not null
            and annee >= {{ core_dashboards_store.get_current_year() }}
            - {{ nbre_annee_a_extraire }}
    ),
-- ============================================================================
-- ÉTAPE 2: Enrichissement du calendrier (dates de contexte)
-- ============================================================================
-- Ajoute les bornes mensuelles/annuelles et calcule l'année scolaire
-- (juillet = début de l'année scolaire suivante, ex: 2024-2025).
-- ============================================================================

    expected_cal_days_nbr as (
        select
            id_eco,
            grille,
            date_evenement,
            datefromparts(year(date_evenement), month(date_evenement), 1) as debut_mois,
            eomonth(date_evenement) as fin_mois,
            case when month(date_evenement) >= 7 then year(date_evenement) else year(date_evenement)-1 end as debut_annee_scolaire,
            case 
                when month(date_evenement) >= 7 
                then concat(cast(year(date_evenement) as varchar(4)),'-', cast(year(date_evenement)+1 as varchar(4)))
                else concat(cast(year(date_evenement)-1 as varchar(4)),'-', cast(year(date_evenement) as varchar(4)))
            end as labelle_annee_scolaire,            
            datefromparts( case when month(date_evenement) >= 7 then year(date_evenement) else year(date_evenement)-1 end, 7, 1) as date_debut_annee_scolaire,
            datefromparts( case when month(date_evenement) >= 7 then year(date_evenement) else year(date_evenement)-1 end + 1, 6, 30) as date_fin_annee_scolaire
        from expected_cal
        where date_evenement <= getdate() 
    ), 
-- ============================================================================
-- ÉTAPE 3: Récupération de la dernière date connue
-- ============================================================================
-- Identifie la date maximale du calendrier pour détecter les périodes
-- en cours (mois actuel, année scolaire actuelle).
-- ============================================================================

    derniere_date as (
               select max(date_evenement) as derniere_date
            from expected_cal_days_nbr
    ),
-- ============================================================================
-- ÉTAPE 4: Agrégations et window functions (MTD, YTD, marqueurs)
-- ============================================================================
-- Calcule pour chaque jour/établissement/grille :
-- - mtd_jour : cumul des jours depuis le début du mois
-- - total_jours_mois : nombre total de jours d'école du mois
-- - jours_ytd : cumul des jours depuis le début de l'année scolaire
-- - total_jours_annee_scolaire : nombre total de jours de l'année scolaire
-- ============================================================================    
    calendrier_enrichi as (
        select
            cal.id_eco,
            cal.grille,
            cal.date_evenement,
            cal.debut_mois,
            cal.fin_mois,
            cal.debut_annee_scolaire,
            cal.labelle_annee_scolaire,
            cal.date_debut_annee_scolaire,
            cal.date_fin_annee_scolaire,
            ld.derniere_date,
        -- cumul mois jusqu'à ce jour     
            count(*) over (
                partition by cal.id_eco, cal.grille, cal.debut_mois
                order by cal.date_evenement
                rows between unbounded preceding and current row
            ) as mtd_jour,            
        -- total mois
            count(*) over (
                partition by cal.id_eco, cal.grille, cal.debut_mois
            ) as total_jours_mois,             
        -- cumul année scolaire jusqu'à ce jour
            count(*) over (
                partition by cal.id_eco, cal.grille, cal.labelle_annee_scolaire
                order by cal.date_evenement
                rows between unbounded preceding and current row
            ) as jours_ytd,
        -- total année scolaire
            count(*) over (
                partition by cal.id_eco, cal.grille, cal.labelle_annee_scolaire
            ) as total_jours_annee_scolaire,            
            case 
                when year(cal.debut_mois)=year(ld.derniere_date) 
                and month(cal.debut_mois)=month(ld.derniere_date) then 1 else 0 
            end as is_mois_en_cours,

            case
                when ld.derniere_date between cal.date_debut_annee_scolaire and cal.date_fin_annee_scolaire then 1 else 0
            end as is_annee_scolaire_en_cours                          
        from expected_cal_days_nbr as cal
        cross join derniere_date as ld
    )
-- ============================================================================
-- ÉTAPE 5: Sélection finale avec clé unique
-- ============================================================================
-- Exposition du calendrier enrichi avec clé de surrogate (id_eco, grille, date)
-- pour faciliter les jointures et le filtrage dans Power BI.
-- ============================================================================    
select
    {{
        dbt_utils.generate_surrogate_key(
            [
                "id_eco",
                "grille",
                "date_evenement",
            ]
        )
    }} as id_unique, 
    id_eco,
    grille,
    date_evenement,
    debut_mois,
    fin_mois,
    debut_annee_scolaire,
    labelle_annee_scolaire,
    date_debut_annee_scolaire,
    date_fin_annee_scolaire,
    mtd_jour,
    total_jours_mois,
    jours_ytd,
    total_jours_annee_scolaire,
    is_mois_en_cours,
    is_annee_scolaire_en_cours
from calendrier_enrichi     