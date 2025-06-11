from dagster import job, op, get_dagster_logger
import subprocess
@op
def run_dbt_build():
   logger = get_dagster_logger()
   project_path = "/home/cssst/cssst.dashboards_store"
   command = ["poetry", "run", "dbt", "build", "--target", "prod"]
   result = subprocess.run(
       command,
       cwd=project_path,
       capture_output=True,
       text=True
   )
   logger.info(result.stdout)
   if result.returncode != 0:
       raise Exception(f"dbt failed: {result.stderr}")
   return result.stdout
@job
def dbt_build_job():
   run_dbt_build()