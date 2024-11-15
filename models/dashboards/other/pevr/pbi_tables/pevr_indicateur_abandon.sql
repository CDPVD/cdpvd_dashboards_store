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

{{ config(alias="indicateur_abandon") }}

WITH
    source as (
        select
            y_stud.fiche,
            y_stud.annee,
            y_stud.nom_ecole,
            dan.motif_fin_mels,
            des_mesure.cf_descr as motif_descr
        from {{ ref("fact_yearly_student") }} as y_stud
        inner join
            {{ ref("i_gpm_e_dan") }} as dan
            on y_stud.fiche = dan.fiche
            and y_stud.id_eco = dan.id_eco
        left join
            {{ ref("i_wl_descr") }} as des_mesure
            on dan.motif_fin_mels = des_mesure.code
            and nom_table = 'MOTIF_DEPART'
        where dan.motif_fin_mels IN ('01','11','12','15') -- 01 = Abandon, 11 = Raison inconnue, 12 = Accès au marché du travail (FP/FGA), 15 = Raison personnelle
    )

select * from source

-- NOTE À MOI MÊME. La population prend seulement les dossiers actifs. Les abandons sont majoritairement inactifs!!