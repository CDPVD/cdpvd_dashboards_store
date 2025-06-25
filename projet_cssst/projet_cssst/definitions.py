from dagster import Definitions, define_asset_job
from dagster_dbt import DbtCliResource
from .assets import dbt_models

# Configuration de la ressource dbt CLI
dbt_resource = DbtCliResource(
    project_dir="/home/cssst/cssst.dashboards_store",
    profiles_dir="/home/cssst/.dbt",
    profile="cssst_dashboards_store",
    executable="/home/cssst/.cache/pypoetry/virtualenvs/cdpvd-dashboards-store-PwHv-ypr-py3.10/bin/dbt",
)

# Job principal : exécute tous les assets
build_all_job = define_asset_job("build_all")

# Définition finale du dépôt Dagster
defs = Definitions(
    assets=[dbt_models],
    resources={"dbt": dbt_resource},
    jobs=[build_all_job],
)
