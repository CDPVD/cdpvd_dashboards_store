
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

-- historique des emplois à considérer
select distinct
    phe.matr,
    phe.date_eff,
    phe.date_fin,
    phe.lieu_trav,
    phe.ref_empl,
    phe.stat_eng,
    phe.sect,
    phe.aff,

    -- Calcul du nb d'heures normales de travail par jour
    case
        when
            left(phe.corp_empl, 1) != '3'
            and phe.mode = 'h'
            and phe.nb_hre_sem != 0.0
        then phe.nb_hre_sem / 5.0
        when
            left(phe.corp_empl, 1) = '3'
            and phe.stat_eng in ('E1', 'E2', 'E3', 'E8', 'E9')
        then 6.4
        when left(phe.corp_empl, 1) = '3'
        then ptce.nb_hres_an / 200.0
        when left(phe.corp_empl, 1) = '2'
        then ptce.nb_hres_an / 260.9
        else ptce.nb_hres_an / 260.0
    end as hntj,
    -- corps d’emploi normalisé + prefix pour éviter LEFT() partout
    left(case when len(ptce.corp_empl_percos) = 4 then ptce.corp_empl_percos else ptce.corp_empl end, 1) as corp_prefix,
    case
        when len(ptce.corp_empl_percos) = 4
        then ptce.corp_empl_percos
        else ptce.corp_empl
    end as corp_emploi,
    ptce.descr as corp_emploi_descr,
    -- Nombre d’heures annuelles
    case
        when left(phe.corp_empl, 2) = '35' then 800.0 else 1080.0
    end as nb_hres_an,
    phe.pourc_post,
    phe.pourc_temp,
    -- Traitement des emplois sur un plan sabbatique à traitement différé
    case
        when ptee.trait_spec = '1' and phe.pourc_sal >= 2.0
        then
            round(
                ((phe.pourc_post * phe.pourc_temp / 100.0) - phe.pourc_sal), 4
            )
        when ptee.trait_spec = '2'
        then 0.0
        else 100.0
    end as pourc_sabbatique_manquant
from {{ ref("i_pai_hemp") }} as phe
join
    {{ ref("i_pai_tab_corp_empl_date") }} as ptce
    on phe.corp_empl = ptce.corp_empl
    and phe.date_eff between ptce.date_deb and ptce.date_fin
join {{ ref("i_pai_tab_etat_empl") }} as ptee on phe.etat = ptee.etat_empl
where phe.stat_eng not like '9_'