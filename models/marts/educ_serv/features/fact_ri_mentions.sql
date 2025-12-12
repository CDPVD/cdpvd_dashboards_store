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
-- Création de l'année brut pour une manipulation future
with
    _mentions as (
        select
            mentions.fiche,
            mentions.code_perm,
            mentions.eco_cen_off,
            mentions.prog_charl,
            case 
                when mentions.date_obt_mention = '' then mentions.date_exec_sanct
                else mentions.date_obt_mention
            end as date_obt_mention,
            mentions.ind_reus_sanct_charl,
            mentions.regime_sanct_charl
        from {{ ref("i_e_ri_mentions") }} as mentions
    ),

    -- Création de la notion de l'année dans e_ri_mentions
    mentions_annee as (
        select
            mentions.fiche,
            mentions.code_perm,
            mentions.eco_cen_off,
            mentions.prog_charl,
            mentions.regime_sanct_charl,
            mentions.date_obt_mention,
            case
                when month(mentions.date_obt_mention) between 9 and 12  -- Entre septembre et Décembre
                then year(mentions.date_obt_mention)
                when month(mentions.date_obt_mention) between 1 and 8  -- Entre Janvier et Août
                then year(mentions.date_obt_mention) - 1
            end as annee_sanction,
            cast(month(mentions.date_obt_mention) as nvarchar(12)) as mois_sanction,
            case
                when mentions.ind_reus_sanct_charl = 'O' then 1.0 else 0.0
            end as 'ind_obtention',
            case when prog.type_diplome = 'DES' then 1.0 else 0.0 end as 'indice_Des',
            case when prog.type_diplome = 'CFPT' then 1.0 else 0.0 end as 'indice_Cfpt',
            case when prog.type_diplome = 'CFMS' then 1.0 else 0.0 end as 'indice_Cfms'
        from _mentions as mentions
        inner join {{ ref("i_t_prog") }} as prog on mentions.prog_charl = prog.prog_meq
    )

select
    fiche,
    code_perm,
    eco_cen_off,
    prog_charl,
    annee_sanction,
    mois_sanction,
    ind_obtention,
    regime_sanct_charl,
    date_obt_mention,
    indice_des,
    indice_cfpt,
    indice_cfms
from mentions_annee
