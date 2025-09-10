
from dagster import define_asset_job


full_pipeline_job = define_asset_job(
    name="full_pipeline",
    selection=[
        "meltano_ingestion",
        "dbt_snapshot",
        "dbt_seed",
        "dbt_staging",
        "dbt_dim",
        "dbt_facts",
        "dbt_elementary_build",
        "elementary_report",
    ],
)
