from dagster import asset
import os, subprocess

from dagster_project.assets.meltano_ingestion import meltano_ingestion

DBT_PROJECT_DIR = "dbt_project"

def _dbt_env():
    overrides = {
        "CREDENTIALS_PATH": os.getenv("CREDENTIALS_PATH", ""),
        "PROJECT_ID": os.getenv("PROJECT_ID", ""),
        "STAGING_DATASET_NAME": os.getenv("STAGING_DATASET_NAME", ""),
        "RAW_DATASET_NAME": os.getenv("RAW_DATASET_NAME", ""),
    }
    return {**os.environ, **{k: v for k, v in overrides.items() if v}}

@asset(deps=[meltano_ingestion])
def dbt_run(context):
    """Run dbt build."""
    env = _dbt_env()
    context.log.info("Running dbt build as a single materialization step")
    result = subprocess.run(["dbt", "build", "--exclude", "test"], cwd=DBT_PROJECT_DIR, check=False, env=env, capture_output=True, text=True)
    context.log.info(f"dbt build output: {result.stdout}")
    context.log.error(f"dbt build errors: {result.stderr}")
    if result.returncode != 0:
        context.log.error(f"dbt build errors:\n{result.stderr}")
        raise Exception(f"dbt build failed with exit code {result.returncode}")

@asset(deps=[dbt_run])
def dbt_test(context):
    """Run dbt test."""
    env = _dbt_env()
    context.log.info("Running dbt test as a single materialization step")
    result = subprocess.run(["dbt", "test"], cwd=DBT_PROJECT_DIR, check=False, env=env, capture_output=True, text=True)
    context.log.info(f"dbt test output: {result.stdout}")
    context.log.error(f"dbt test errors: {result.stderr}")
    if result.returncode != 0:
        context.log.error(f"dbt test errors:\n{result.stderr}")
        raise Exception(f"dbt test failed with exit code {result.returncode}")
