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
select
    c.name as nom_report,
    c.path as chemin_report,
    c.type as type_report,
    cp.name as nom_report_parent,
    cp.path as chemin_report_parent,
    c.description as description_report,
    el.username as nom_utilisateur,
    el.requesttype as type_requete,
    el.timestart as date_heure_debut,
    el.timeend as date_heure_fin,
    el.timedataretrieval + el.timeprocessing + el.timerendering as duree_total,
    el.timeprocessing as temps_traitement,
    el.timerendering as temps_rendu,
    el.timedataretrieval as dataretrievalms,
    el.source as executionsource,
    el.format as outputformat
from {{ ref("i_executionlogstorage") }} el
join {{ ref("i_catalog") }} c on c.itemid = el.reportid
left join {{ ref("i_catalog") }} cp on cp.parentid = el.reportid
