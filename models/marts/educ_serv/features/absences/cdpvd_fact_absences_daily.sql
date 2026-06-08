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
	Compute the number of absences and delay for each students by day of absence / delay.
    Events are further qualified by the number of periods impacted (full day / partial day).
    Each absence is mapped to the student's etape.
#}
-- Motifs distincts par jour (toutes catégories confondues)
with
    source as (
        select
            school_year,
            date_abs,
            jour_semaine,
            fiche,
            id_eco,
            groupe,
            code_matiere,
            grille,
            event_kind,
            is_aggregate_kind,
            is_absence,
            category_abs,
            event_description,
            remarque
        from {{ ref("cdpvd_stg_absence_daily") }}
    ),
    distinct_motifs_day as (
        select distinct
            s.id_eco, s.fiche, cast(s.date_abs as date) as date_abs, s.event_description
        from source s
    ),

    motif_day_agg as (
        select
            id_eco,
            fiche,
            date_abs,
            count(*) as nb_motifs_day,
            max(event_description) as single_motif  -- valable seulement si nb_motifs_day = 1
        from distinct_motifs_day
        group by id_eco, fiche, date_abs
    ),

    -- Flags mixte M/NM par jour
    distinct_cat_day as (
        select distinct
            s.id_eco, s.fiche, cast(s.date_abs as date) as date_abs, s.category_abs
        from source s
    ),

    mix_flags as (
        select
            id_eco,
            fiche,
            date_abs,
            max(
                case when category_abs = 'Absences motivées' then 1 else 0 end
            ) as has_m,
            max(
                case when category_abs = 'Absences non motivées' then 1 else 0 end
            ) as has_nm
        from distinct_cat_day
        group by id_eco, fiche, date_abs
    )

-- Add the etape
select distinct
    src.school_year,
    src.date_abs,
    jour_semaine,
    src.fiche,
    src.id_eco,
    groupe,
    code_matiere,
    src.grille,
    src.event_kind,
    src.is_aggregate_kind,
    src.is_absence,
    src.remarque,
    case
        when has_m = 1 and has_nm = 1 then 'Absence mixte M-NM' else src.category_abs
    end as category_abs,

    -- Motif final : "Motifs multiples" seulement si >1 motif distinct dans la journée
    case
        when coalesce(nb_motifs_day, 1) > 1 then 'Motifs multiples' else single_motif  -- = le motif unique de la journée
    end as event_description,
    etp.etape,
    etp.etape_description,
    etp.seq_etape,
    etp.date_debut,
    etp.date_fin

from source as src
left join
    motif_day_agg md
    on md.id_eco = src.id_eco
    and md.fiche = src.fiche
    and md.date_abs = cast(src.date_abs as date)
left join
    mix_flags mx
    on mx.id_eco = src.id_eco
    and mx.fiche = src.fiche
    and mx.date_abs = cast(src.date_abs as date)
left join
    {{ ref("stg_fact_fiche_etapes") }} as etp
    on src.fiche = etp.fiche
    and src.id_eco = etp.id_eco
    and src.date_abs between etp.date_debut and etp.date_fin
