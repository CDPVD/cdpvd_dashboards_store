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
    itemid,
    path,
    name,
    parentid,
    type,
    content,
    intermediate,
    snapshotdataid,
    linksourceid,
    property,
    description,
    hidden,
    createdbyid,
    creationdate,
    modifiedbyid,
    modifieddate,
    mimetype,
    snapshotlimit,
    parameter,
    policyid,
    policyroot,
    executionflag,
    executiontime,
    subtype,
    componentid,
    contentsize
from {{ var("database_reportserver") }}.dbo.catalog
