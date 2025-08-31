
from dagster import define_asset_job


full_pipeline_job = define_asset_job(
    name="full_pipeline",
    selection=[
        "meltano_ingestion",
        "dbt_run",
        "dbt_test",
    ],
)
