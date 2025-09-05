
from dagster import define_asset_job


full_pipeline_job = define_asset_job(
    name="full_pipeline",
    selection=[
        "meltano_ingestion",
        "dbt_snapshot",
        "dbt_seed",
        "dbt_run",
        "dbt_test",
        "dbt_elementary_build",
        "elementary_report",
    ],
)
