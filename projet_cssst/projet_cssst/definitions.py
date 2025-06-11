from dagster import Definitions

from .assets import test_dbt_cli_debug
defs = Definitions(
   assets=[test_dbt_cli_debug]
)