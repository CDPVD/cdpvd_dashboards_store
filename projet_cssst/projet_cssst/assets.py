
from pathlib import Path
from dagster_dbt import load_assets_from_dbt_manifest

dbt_models = load_assets_from_dbt_manifest(
    manifest=Path("/home/cssst/cssst.dashboards_store/target/manifest.json")
)