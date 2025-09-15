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

---------------- POUR CHECKER DES CAS PARTICULIERS -----------------
{# declare @matr int = NULL
declare @date_debut date = '2025-08-10'
declare @date_fin date = '2025-08-23' #}
--------------------------------------------------------------------

with trouver_le_pourc_sabbatique_manquant as ( 
    select 
        prd.date_cheq
        , prd.date_deb
        , prd.date_fin
        , pmnt.matr
        , hntj.corp_emploi
        , hntj.stat_eng
        , hntj.sect
        , hntj.aff
        , typeremun.typeremun
        , pmnt.mode
        , case 
			when pmnt.lieu_trav is null then hntj.lieu_trav 
			else pmnt.lieu_trav 
		end as lieu_trav
        , hntj.pourc_sabbatique_manquant
        , hntj.pourc_post
        , hntj.pourc_temp
        , pmnt.code_pmnt
        , hntj.hntj
        , pmnt.nb_unit 
        , pmnt.date_deb as date_deb_pmnt
        , pmnt.date_fin as date_fin_pmnt
		, left(chq.an_budg, 4) as annee
		, chq.an_budg
		, chq.no_per
        , case 
            when pmnt.mode in ('1','5','J') then hntj.hntj * pmnt.nb_unit 
            when pmnt.mode in ('H','L','2') then pmnt.nb_unit when pmnt.mode in ('M','G','E','6') then pmnt.nb_unit / 60 
            when pmnt.mode = 'P' then pmnt.nb_unit * nb_hres_an / 1000.0 
            when pmnt.mode = 'F' then pmnt.nb_unit * nb_hres_an / 720.0 
            when pmnt.mode = 'D' then pmnt.nb_unit * nb_hres_an / 800.0 
            when pmnt.mode = '3' then pmnt.nb_unit * (hntj.hntj / 2.0) 
            when pmnt.mode = '4' then pmnt.nb_unit * (hntj.hntj * 3.0 / 4.0) 
        end as nombre_heures_remun
    from  {{ ref('i_pai_hchq_pmnt') }} as pmnt 
    -- join [192.168.207.153].[paie].dbo.pai_dos d 
    --     on d.matr = pmnt.matr 
    join {{ ref('i_pai_hchq') }} as chq
        on chq.matr = pmnt.matr and chq.no_cheq = pmnt.no_cheq and chq.date_cheq = pmnt.date_cheq and chq.etat_cheq = 3 
    join ( 
			select * 
            from {{ ref('i_pai_tab_cal_per') }}
            --where cal.date_fin >= @date_fin and cal.date_deb <= @date_debut
			where date_fin >= {d '2024-07-01'} --and date_deb <= {d '2025-09-11'}
    ) as prd on prd.gr_paie = chq.gr_paie and prd.an_budg = chq.an_budg and prd.no_per = chq.no_per 
    join (
            select distinct
                phe.matr
                , phe.date_eff
                , phe.date_fin
                , phe.lieu_trav
                , phe.ref_empl
                , phe.stat_eng
                , phe.sect
                , phe.aff
				-- Calcul du nb d'heures normales de travail par jour
                , case 
                    when left(phe.corp_empl, 1) != '3' and phe.mode = 'h' and phe.nb_hre_sem != 0.0 then phe.nb_hre_sem / 5.0 
                    when left(phe.corp_empl, 1) = '3' and phe.stat_eng in ('E1','E2','E3','E8','E9') then 6.4 
                    when left(phe.corp_empl, 1) = '3' then ptce.nb_hres_an / 200.0 
                    when left(phe.corp_empl, 1) = '2' then ptce.nb_hres_an / 260.9   											-- j'ai delete '809000' != '865000'. est ce que c aa un rapport avec le fcsq?
                    else ptce.nb_hres_an / 260.0 
                end as hntj
				-- Corps d'emploi
                , case 
                    when len(ptce.corp_empl_percos) = 4 then ptce.corp_empl_percos 
                    else ptce.corp_empl 
                end as corp_emploi
                , ptce.descr as corp_emploi_descr
				-- Nombre d’heures annuelles
                , case 
                    when left(phe.corp_empl, 2) = '35' then 800.0 
                    else 1080.0 
                end as nb_hres_an
				-- Calcule un pourcentage "manquant" selon des règles liées au traitement spécial
                , phe.pourc_post
                , phe.pourc_temp
                , case 
                    when ptee.trait_spec = '1' and phe.pourc_sal >= 2.0 then round(((phe.pourc_post * phe.pourc_temp / 100.0) - phe.pourc_sal), 4) 
                    when ptee.trait_spec = '2' then 0.0 
                    else 100.0
                end as pourc_sabbatique_manquant
            from {{ ref("i_pai_hemp") }} as phe 
            join {{ ref('i_pai_tab_corp_empl_date') }} as ptce 
                on phe.corp_empl = ptce.corp_empl and phe.date_eff between ptce.date_deb and ptce.date_fin 
            join {{ ref('i_pai_tab_etat_empl') }} as ptee 
                on phe.etat = ptee.etat_empl 
    ) as hntj on hntj.matr = pmnt.matr and hntj.ref_empl = pmnt.ref_empl and pmnt.date_deb between  hntj.date_eff and hntj.date_fin 
    join (
            select 
                code_pmnt
				, case 
                    when (code_pmnt like '101[01234]__' or code_pmnt = '105001') then '1' 
                    when code_pmnt like '103[56789]__' then '2' 
                    when code_pmnt like '1015__' then '3' 
                end as typeremun
            from {{ ref("i_pai_tab_pmnt") }}
	) as typeremun on typeremun.code_pmnt = pmnt.code_pmnt and typeremun.typeremun is not null 
	where 
		---------------- POUR CHECKER DES CAS PARTICULIERS -----------------
		{# pmnt.date_cheq between @date_debut and @date_fin
		and (@matr is null or (@matr is not null and  pmnt.matr = @matr)) #}
		--------------------------------------------------------------------
		hntj.stat_eng not like '9_' 
    	and left(hntj.corp_emploi, 1) in ('1','2','3','4','5')
    	and pmnt.mode <> ' ' 
		-- Exclusion des paiements dont le code commence par 103 et qui ont une entrée correspondante dans pai_tab_mot_abs (code_pmnt_a_exonerer)
		and not exists (
			select 1 
			from {{ ref('i_pai_tab_mot_abs') }} as ptma 
			where ptma.code_pmnt_a_exonerer = pmnt.code_pmnt and pmnt.code_pmnt like '103%'
		) 
        and pmnt.mnt <> 0.0 
        and ( 
                ( 
                    (pmnt.code_pmnt like '101[01234]__' or pmnt.code_pmnt like '103[56789]__' or pmnt.code_pmnt like '1015__') 
                    and pmnt.code_prov not in ('A6','AJ','AW','AK','A5','AL','AJ','AT','AV','AS','AU') 
                ) 
                or (pmnt.code_pmnt = '105001' and pmnt.code_prov = 'AX' and pmnt.nb_unit between 0.0 and 60.0) 
                or (pmnt.code_pmnt = '105001' and (pmnt.code_prov = 'AY' or pmnt.code_prov = 'A0')) 
        )

), Calculer_les_heures_sabbatiques_manquantes as ( 
    select 
        *
        , case 
            when pourc_sabbatique_manquant <> 0.0 and pourc_sabbatique_manquant <> 100.0 and date_deb_pmnt <> date_fin_pmnt then ( ( ( pourc_post * pourc_temp / 100.0 ) * nombre_heures_remun ) / ( ( pourc_post * pourc_temp / 100.0 ) - pourc_sabbatique_manquant ) ) - nombre_heures_remun 
            else 0.0 
        end as heures_manquantes_sab
    from trouver_le_pourc_sabbatique_manquant
) 

select 
    annee
	, an_budg
	, no_per
	, date_cheq
    , date_deb
    , date_fin
    , date_deb_pmnt
    , date_fin_pmnt
    , matr
    , corp_emploi
    , stat_eng
    , sect
    , aff
    , typeremun
    , mode
    , lieu_trav
    , code_pmnt
    , hntj
    , nb_unit 
    , nombre_heures_remun
    , heures_manquantes_sab
    , convert (numeric(7,2), case 
        when pourc_sabbatique_manquant <> 0.0 and pourc_sabbatique_manquant <> 100.0 then nombre_heures_remun + heures_manquantes_sab 
        when pourc_sabbatique_manquant = 0.0 then 0.0 
        else nombre_heures_remun 
    end) as nb_hre_remun_fin
from calculer_les_heures_sabbatiques_manquantes 