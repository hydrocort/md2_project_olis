from xml.etree.ElementPath import ops
from dagster import Definitions
from dagster_project.assets.meltano_ingestion import meltano_ingestion
from dagster_project.assets.dbt_models import dbt_run, dbt_seed, dbt_test, dbt_snapshot
from dagster_project.assets.elementary import dbt_elementary_build, elementary_report
from dagster_project.jobs.pipeline import full_pipeline_job
from dagster_project.schedules.daily_schedule import daily_pipeline_schedule
from dagster_project.resources.bigquery import bigquery_resource
from dagster_dbt import DbtCliResource
from dagster_project.resources.env_loader import load_dotenv

load_dotenv()  # Ensure .env is loaded for all resources

defs = Definitions(
    assets=[meltano_ingestion, dbt_snapshot, dbt_seed, dbt_run, dbt_test, dbt_elementary_build, elementary_report],
    jobs=[full_pipeline_job],
    schedules=[daily_pipeline_schedule],
    resources={
        "bigquery": bigquery_resource,
        "dbt": DbtCliResource(project_dir="dbt_project", profiles_dir="dbt_project"),
    },
)