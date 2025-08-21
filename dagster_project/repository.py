from dagster import Definitions
from dagster_project.assets import meltano_ingestion, dbt_models, quality_checks, analysis
from dagster_project.jobs.pipeline import full_pipeline_job
from dagster_project.schedules.daily_schedule import daily_pipeline_schedule
from dagster_project.resources.bigquery import bigquery_resource
from dagster_dbt import DbtCliResource

defs = Definitions(
     assets=[
         meltano_ingestion.meltano_ingestion,
         dbt_models.dbt_assets,
         quality_checks.run_data_quality_tests,
         analysis.exploratory_analysis,
     ],
     jobs=[full_pipeline_job],
     schedules=[daily_pipeline_schedule],
     resources={
        "bigquery": bigquery_resource.configured({"project": "polar-winter-468405-g1"}),
        # dbt CLI resource used by dagster-dbt assets
        "dbt": DbtCliResource(project_dir="dbt_project", profiles_dir="dbt_project"),
     },
)
