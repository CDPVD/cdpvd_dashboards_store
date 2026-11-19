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

{{ config(alias="rrm_report_entonnoir_categorie") }}

/*
    Entonnoir de rétention dans la catégorie d'emploi, selon la tranche
    d'expérience dans la catégorie.

    Source : rrm_report_experience_categorie (une ligne par matr / catégorie).

    Population : catégories dont l'entrée (date_debut_cat) est dans une année
    scolaire >= annee_cohorte_min (stg_rrm_parametres). Les catégories
    commencées avant sont exclues :
    leur début n'est pas observé de façon fiable.

    Grain : une ligne par employé x catégorie x tranche atteinte. Une catégorie
    dont l'expérience est dans la tranche k apparaît dans les tranches 1..k.

    Indicateurs 0/1, additifs (SUM dans Power BI) :
      effectif_initial : a atteint le début de la tranche
      en_cours         : toujours dans la catégorie, dans cette tranche (censuré)
      depart           : a quitté le CSS pendant la tranche
      sortie_autre_cat : a quitté la catégorie pour une autre pendant la tranche
      effectif_final   : a franchi la fin de la tranche
                         (= effectif initial de la tranche suivante)

    Dernière tranche (ouverte, mois_max NULL, « 5 ans et + ») : elle n'a pas
    de fin, personne ne peut la franchir. Les employés toujours dans la
    catégorie y sont comptés dans effectif_final (présents aujourd'hui) et
    en_cours y vaut 0. Taux de cette tranche : 1 - (depart + sortie_autre_cat)
    / effectif_initial (taux observé, sans censure).

    Rapprochement :
      effectif_initial - depart - sortie_autre_cat - en_cours = effectif_final

    Rétention (DAX, sur les sommes) :
      taux tranche = 1 - (depart + sortie_autre_cat) / (effectif_initial - en_cours)
      taux cumulé  = produit des taux des tranches 1..k
    Rétention au CSS seulement : retirer sortie_autre_cat du numérateur.

    effectif_observe = effectif_initial - en_cours (dénominateur des taux).
*/

SELECT
    C.matr,
    C.cat_empl,
    C.cat_emp,
    -- Tranche d'expérience de la ligne (axe de l'entonnoir)
    T.ordre AS tranche_ordre,
    T.tranche,
    1 AS effectif_initial,
    F.dans_tranche * (1 - F.est_tranche_ouverte) * C.en_cours AS en_cours,
    F.dans_tranche * C.depart AS depart,
    F.dans_tranche * C.sortie_autre_cat AS sortie_autre_cat,
    1 - F.dans_tranche
        + F.dans_tranche * F.est_tranche_ouverte * C.en_cours AS effectif_final,
    -- Effectif observé (issue connue) = effectif_initial - en_cours
    1 - F.dans_tranche * (1 - F.est_tranche_ouverte) * C.en_cours AS effectif_observe

FROM {{ ref("rrm_report_experience_categorie") }} AS C

CROSS JOIN {{ ref("stg_rrm_parametres") }} AS PAR

CROSS APPLY (
    -- Année scolaire d'entrée dans la catégorie (var mois_reference)
    SELECT
        YEAR(C.date_debut_cat)
        - CASE WHEN MONTH(C.date_debut_cat) < {{ var("mois_reference") | int }} THEN 1 ELSE 0 END
        AS cohorte
) AS COH

-- Toutes les tranches atteintes : 1..tranche de l'expérience
INNER JOIN {{ ref("dim_rrm_tranche_duree") }} AS T
    ON T.ordre <= C.experience_tranche_ordre

CROSS APPLY (
    -- 1 sur la tranche où l'expérience se termine
    SELECT
        CASE WHEN T.ordre = C.experience_tranche_ordre THEN 1 ELSE 0 END AS dans_tranche,
        -- Dernière tranche, sans borne supérieure
        CASE WHEN T.mois_max IS NULL THEN 1 ELSE 0 END AS est_tranche_ouverte
) AS F

WHERE COH.cohorte >= PAR.annee_cohorte_min
