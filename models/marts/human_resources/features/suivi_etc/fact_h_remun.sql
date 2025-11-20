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
-- Récupérer la date de debut d'extraction
{% set date_pivot = var("marts")["human_resources"]["date_pivot"] %}

-- Récupérer les code de paiments avec des montants nuls que l'on souhaite considerer
{% set codes = get_seed_values(
    "pmnt_zero_keep", "code_pmnt", "_human_resources_seeds"
) %}
-- Transforme la liste Python en string SQL friendly
{% if codes | length > 0 %}
    {% set codes_sql = "'" ~ codes | join("','") ~ "'" %}
    {% set list_cod_pmnt = "(" ~ codes_sql ~ ")" %}

{% else %}
    -- fallback pour éviter erreur SQL quand la seed est vide #}
    {% set list_cod_pmnt = "(NULL)" %}
{% endif %}

-- periode de paie à considerer
with
    prd as (
        select gr_paie, an_budg, no_per, date_cheq, date_deb, date_fin
        from {{ ref("i_pai_tab_cal_per") }}
        where date_fin >= {d '{{ date_pivot }}'}

    -- historique des emplois à considérer
    ),
    perim as (
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
            -- Corps d'emploi
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

    -- Calculer le nbre d'heures remunerées
    ),
    hres_remun as (
        select
            prd.date_cheq,
            pmnt.no_cheq,
            prd.date_deb,
            prd.date_fin,
            pmnt.matr,
            perim.corp_emploi,
            perim.stat_eng,
            perim.sect,
            perim.aff,
            typeremun.typeremun,
            pmnt.mode,
            case
                when pmnt.lieu_trav is null then perim.lieu_trav else pmnt.lieu_trav
            end as lieu_trav,
            perim.pourc_sabbatique_manquant,
            perim.pourc_post,
            perim.pourc_temp,
            pmnt.no_seq,
            pmnt.code_pmnt,
            perim.hntj,
            pmnt.nb_unit,
            pmnt.date_deb as date_deb_pmnt,
            pmnt.date_fin as date_fin_pmnt,
            left(chq.an_budg, 4) as annee,
            chq.an_budg,
            chq.no_per,
            chq.gr_paie,
            case
                when pmnt.mode in ('1', '5', 'J')
                then perim.hntj * pmnt.nb_unit
                when pmnt.mode in ('H', 'L', '2')
                then pmnt.nb_unit
                when pmnt.mode in ('M', 'G', 'E', '6')
                then pmnt.nb_unit / 60
                when pmnt.mode = 'P'
                then pmnt.nb_unit * nb_hres_an / 1000.0
                when pmnt.mode = 'F'
                then pmnt.nb_unit * nb_hres_an / 720.0
                when pmnt.mode = 'D'
                then pmnt.nb_unit * nb_hres_an / 800.0
                when pmnt.mode = '3'
                then pmnt.nb_unit * (perim.hntj / 2.0)
                when pmnt.mode = '4'
                then pmnt.nb_unit * (perim.hntj * 3.0 / 4.0)
            end as nombre_heures_remun,
            pmnt.mnt,
            pmnt.mnt_cour_trait_diff
        from prd
        left join
            {{ ref("i_pai_hchq") }} as chq
            on chq.gr_paie = prd.gr_paie
            and chq.an_budg = prd.an_budg
            and chq.no_per = prd.no_per
            and chq.etat_cheq = 3
        left join
            {{ ref("i_pai_hchq_pmnt") }} as pmnt
            on pmnt.matr = chq.matr
            and pmnt.no_cheq = chq.no_cheq
            and pmnt.date_cheq = chq.date_cheq
        join
            perim
            on perim.matr = pmnt.matr
            and perim.ref_empl = pmnt.ref_empl
            and pmnt.date_deb between perim.date_eff and perim.date_fin
        join
            (
                select
                    code_pmnt,
                    case
                        when (code_pmnt like '101[01234]__' or code_pmnt = '105001')
                        then '1'
                        when code_pmnt like '103[56789]__'
                        then '2'
                        when code_pmnt like '1015__'
                        then '3'
                    end as typeremun
                from {{ ref("i_pai_tab_pmnt") }}
            ) as typeremun
            on typeremun.code_pmnt = pmnt.code_pmnt
            and typeremun.typeremun is not null
        where
            left(perim.corp_emploi, 1) in ('1', '2', '3', '4', '5')
            and pmnt.mode <> ' '
            -- Exclusion des paiements dont le code commence par 103 et qui ont une
            -- entrée correspondante dans pai_tab_mot_abs (code_pmnt_a_exonerer)
            and not exists (
                select 1
                from {{ ref("i_pai_tab_mot_abs") }} as ptma
                where
                    ptma.code_pmnt_a_exonerer = pmnt.code_pmnt
                    and pmnt.code_pmnt like '103%'
            )
            and (
                (
                    (
                        pmnt.code_pmnt like '101[01234]__'
                        or pmnt.code_pmnt like '103[56789]__'
                        or pmnt.code_pmnt like '1015__'
                    )
                    and pmnt.code_prov not in (
                        'A6', 'AJ', 'AW', 'AK', 'A5', 'AL', 'AJ', 'AT', 'AV', 'AS', 'AU'
                    )
                )
                or (
                    pmnt.code_pmnt = '105001'
                    and pmnt.code_prov = 'AX'
                    and pmnt.nb_unit between 0.0 and 60.0
                )
                or (
                    pmnt.code_pmnt = '105001'
                    and (pmnt.code_prov = 'AY' or pmnt.code_prov = 'A0')
                )
            )

    -- Traitement des emplois sur un plan sabbatique à traitement différé
    ),
    calc_sab as (
        select
            *,
            case
                when
                    pourc_sabbatique_manquant <> 0.0
                    and pourc_sabbatique_manquant <> 100.0
                    and date_deb_pmnt <> date_fin_pmnt
                    and mnt_cour_trait_diff <> 0.0
                then
                    (
                        ((pourc_post * pourc_temp / 100.0) * nombre_heures_remun) / (
                            (pourc_post * pourc_temp / 100.0)
                            - pourc_sabbatique_manquant
                        )
                    )
                    - nombre_heures_remun
                else 0.0
            end as heures_manquantes_sab
        from hres_remun

    -- Jointure avec la table de staging stg_dist_cmpt
    ),
    tot as (
        select
            t1.annee,
            t1.an_budg,
            t1.no_per,
            t1.gr_paie,
            t1.no_cheq,
            t1.date_cheq,
            t1.date_deb,
            t1.date_fin,
            t1.date_deb_pmnt,
            t1.date_fin_pmnt,
            t1.matr,
            t1.corp_emploi,
            t1.stat_eng,
            t1.sect,
            t1.aff,
            t1.typeremun,
            t1.mode,
            t1.lieu_trav,
            t1.no_seq,
            t1.code_pmnt,
            convert(
                numeric(7, 2),
                case
                    when
                        t1.pourc_sabbatique_manquant <> 0.0
                        and t1.pourc_sabbatique_manquant <> 100.0
                    then t1.nombre_heures_remun + t1.heures_manquantes_sab
                    when t1.pourc_sabbatique_manquant = 0.0
                    then 0.0
                    else t1.nombre_heures_remun
                end
            ) as nb_hre_remun,
            t1.mnt + t1.mnt_cour_trait_diff as mnt_tot,
            t2.no_cmpt,
            t2.lieu_trav_cpt_budg,
            case
                when t2.no_cmpt is null
                then t1.mnt + t1.mnt_cour_trait_diff
                else t2.mnt_dist
            end as mnt_dist
        from calc_sab as t1
        left join
            {{ ref("stg_dist_cmpt") }} as t2
            on t2.matr = t1.matr
            and t2.no_cheq = t1.no_cheq
            and t2.no_seq = t1.no_seq

    -- Gestion des paiements avec un montant nul mais des hres remunerees que l'on
    -- souhaite compter (ex: CNESST)
    ),
    mnt_zeros as (
        select
            *,
            sum(
                case
                    when
                        code_pmnt in {{ list_cod_pmnt }}
                        and mnt_dist = 0
                        and mnt_tot = 0
                    then 1
                    else 0
                end
            ) over (partition by an_budg, no_cheq, matr, no_seq) as nbr_no_cmpt
        from tot
    ),
    cal_renum as (
        -- Repartition des heures remunerees
        select
            annee,
            an_budg,
            no_per,
            gr_paie,
            no_cheq,
            date_cheq,
            date_deb,
            date_fin,
            date_deb_pmnt,
            date_fin_pmnt,
            matr,
            corp_emploi,
            stat_eng,
            sect,
            aff,
            typeremun,
            mode,
            no_seq,
            code_pmnt,
            no_cmpt,
            lieu_trav,
            lieu_trav_cpt_budg,
            sum(
                case
                    -- Répartir les heures CNESST malgré des mnt nuls
                    when mnt_dist = 0 and code_pmnt in {{ list_cod_pmnt }}
                    then nb_hre_remun / nbr_no_cmpt
                    when mnt_dist = 0 and code_pmnt not in {{ list_cod_pmnt }}
                    then 0
                    when mnt_tot = mnt_dist or mnt_tot = 0
                    then nb_hre_remun
                    else nb_hre_remun * (mnt_dist / mnt_tot)
                end
            ) as nb_hre_remun_dist
        from mnt_zeros
        group by
            annee,
            an_budg,
            no_per,
            gr_paie,
            no_cheq,
            date_cheq,
            date_deb,
            date_fin,
            date_deb_pmnt,
            date_fin_pmnt,
            matr,
            corp_emploi,
            stat_eng,
            sect,
            aff,
            typeremun,
            mode,
            no_seq,
            code_pmnt,
            no_cmpt,
            lieu_trav,
            lieu_trav_cpt_budg
    )
select
    annee,
    an_budg,
    no_per,
    gr_paie,
    no_cheq,
    date_cheq,
    date_deb,
    date_fin,
    date_deb_pmnt,
    date_fin_pmnt,
    matr,
    corp_emploi,
    stat_eng,
    sect,
    aff,
    typeremun,
    mode,
    no_seq,
    code_pmnt,
    stuff(stuff(stuff(no_cmpt, 4, 0, '-'), 6, 0, '-'), 12, 0, '-') as no_cmpt,
    lieu_trav,
    lieu_trav_cpt_budg as code_lieu_trav,
    nb_hre_remun_dist
from cal_renum
where lieu_trav_cpt_budg is not null and nb_hre_remun_dist != 0
