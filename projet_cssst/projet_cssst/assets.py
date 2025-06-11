from dagster import asset, AssetExecutionContext
import subprocess

@asset
def test_dbt_cli_debug(context: AssetExecutionContext):
   result = subprocess.run(
       ["poetry", "run", "dbt", "build", "--select", "i_gpm_e_dan"],
       cwd="/home/cssst/cdpvd_dashboards_store",
       capture_output=True,
       text=True
   )
   context.log.info("--- STDOUT ---")
   context.log.info(result.stdout)
   context.log.info("--- STDERR ---")
   context.log.info(result.stderr)
   if result.returncode != 0:
       raise Exception("dbt debug failed. See logs above.")