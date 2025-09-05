from dagster import asset
import os, subprocess
from dagster_project.assets.meltano_ingestion import meltano_ingestion
from pathlib import Path

DBT_PROJECT_DIR = "dbt_project"
STATE_DIR = Path(DBT_PROJECT_DIR) / "target"

def _dbt_env():
    overrides = {
        "CREDENTIALS_PATH": os.getenv("CREDENTIALS_PATH", ""),
        "PROJECT_ID": os.getenv("PROJECT_ID", ""),
        "ELEMENTARY_DATASET_NAME": os.getenv("ELEMENTARY_DATASET_NAME", ""),
        "STAGING_DATASET_NAME": os.getenv("STAGING_DATASET_NAME", ""),
        "SNAPSHOT_DATASET_NAME": os.getenv("SNAPSHOT_DATASET_NAME", ""),
        "RAW_DATASET_NAME": os.getenv("RAW_DATASET_NAME", ""),
    }
    return {**os.environ, **{k: v for k, v in overrides.items() if v}}

# run sql commands to delete table in raw dataset, if id is null
def _cleanup_raw_data():
    from google.cloud import bigquery
    client = bigquery.Client()
    query = f"""
    DELETE FROM `{os.getenv("PROJECT_ID")}.{os.getenv("RAW_DATASET_NAME")}.customers`
    WHERE customer_id IS NULL
    """
    query_job = client.query(query)
    query_job.result()
    print(f"Deleted {query_job.num_dml_affected_rows} rows from raw customers table.")

    query = f"""
    DELETE FROM `{os.getenv("PROJECT_ID")}.{os.getenv("RAW_DATASET_NAME")}.order_items`
    WHERE order_id IS NULL
    """
    query_job = client.query(query)
    query_job.result()
    print(f"Deleted {query_job.num_dml_affected_rows} rows from raw order_items table.")

    query = f"""
    DELETE FROM `{os.getenv("PROJECT_ID")}.{os.getenv("RAW_DATASET_NAME")}.orders`
    WHERE order_id IS NULL
    """
    query_job = client.query(query)
    query_job.result()
    print(f"Deleted {query_job.num_dml_affected_rows} rows from raw orders table.")

    query = f"""
    DELETE FROM `{os.getenv("PROJECT_ID")}.{os.getenv("RAW_DATASET_NAME")}.products`
    WHERE product_id IS NULL
    """
    query_job = client.query(query)
    query_job.result()
    print(f"Deleted {query_job.num_dml_affected_rows} rows from raw products table.")

    query = f"""
    DELETE FROM `{os.getenv("PROJECT_ID")}.{os.getenv("RAW_DATASET_NAME")}.sellers`
    WHERE seller_id IS NULL
    """
    query_job = client.query(query)
    query_job.result()
    print(f"Deleted {query_job.num_dml_affected_rows} rows from raw sellers table.")


@asset(deps=[meltano_ingestion], compute_kind="dbt", group_name="Transformation")
def dbt_snapshot(context):
    """Run dbt snapshot."""
    env = _dbt_env()
    _cleanup_raw_data()
    context.log.info("Running dbt snapshot as a single materialization step")
    result = subprocess.run(["dbt", "snapshot"], cwd=DBT_PROJECT_DIR, check=False, env=env, capture_output=True, text=True)
    context.log.info(f"dbt snapshot output: {result.stdout}")
    context.log.error(f"dbt snapshot errors: {result.stderr}")
    if result.returncode != 0:
        context.log.error(f"dbt snapshot errors:\n{result.stderr}")
        raise Exception(f"dbt snapshot failed with exit code {result.returncode}")
    
@asset(deps=[dbt_snapshot], compute_kind="dbt", group_name="Transformation")
def dbt_seed(context):
    """Run dbt seed."""
    env = _dbt_env()
    context.log.info("Running dbt seed as a single materialization step")
    result = subprocess.run(["dbt", "seed"], cwd=DBT_PROJECT_DIR, check=False, env=env, capture_output=True, text=True)
    context.log.info(f"dbt seed output: {result.stdout}")
    context.log.error(f"dbt seed errors: {result.stderr}")
    if result.returncode != 0:
        context.log.error(f"dbt seed errors:\n{result.stderr}")
        raise Exception(f"dbt seed failed with exit code {result.returncode}")

@asset(deps=[dbt_seed], compute_kind="dbt", group_name="Transformation")
def dbt_run(context):
    """Run dbt run."""
    env = _dbt_env()
    context.log.info("Running dbt run as a single materialization step")
    result = subprocess.run(["dbt", "run"], cwd=DBT_PROJECT_DIR, check=False, env=env, capture_output=True, text=True)
    context.log.info(f"dbt run output: {result.stdout}")
    context.log.error(f"dbt run errors: {result.stderr}")
    if result.returncode != 0:
        context.log.error(f"dbt run errors:\n{result.stderr}")
        raise Exception(f"dbt run failed with exit code {result.returncode}")

@asset(deps=[dbt_run], compute_kind="dbt", group_name="Transformation")
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
