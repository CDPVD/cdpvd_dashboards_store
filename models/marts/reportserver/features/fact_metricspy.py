"""
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
"""

def model(dbt, session):
   from pyspark.sql.functions import col, expr
   # Références aux modèles intermédiaires
   el = dbt.ref("i_executionlogstorage")
   c = dbt.ref("i_catalog")
   cp = dbt.ref("i_catalog")  # on le réutilise pour le parent
   # Jointure principale
   joined_df = el.join(c, c["itemid"] == el["reportid"], "inner") \
                 .join(cp, cp["parentid"] == el["reportid"], "left")
   # Sélection des colonnes et transformation
   result = joined_df.select(
       c["name"].alias("nom_report"),
       c["path"].alias("chemin_report"),
       c["type"].alias("type_report"),
       cp["name"].alias("nom_report_parent"),
       cp["path"].alias("chemin_report_parent"),
       c["description"].alias("description_report"),
       el["username"].alias("nom_utilisateur"),
       el["requesttype"].alias("type_requete"),
       el["timestart"].alias("date_heure_debut"),
       el["timeend"].alias("date_heure_fin"),
       (el["timedataretrieval"] + el["timeprocessing"] + el["timerendering"]).alias("duree_total"),
       el["timeprocessing"].alias("temps_traitement"),
       el["timerendering"].alias("temps_rendu"),
       el["timedataretrieval"].alias("dataretrievalms"),
       el["source"].alias("executionsource"),
       el["format"].alias("outputformat")
   )
   return result