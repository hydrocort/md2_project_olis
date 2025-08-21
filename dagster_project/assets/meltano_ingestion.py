
from dagster import asset
import subprocess

@asset
def meltano_ingestion(context):
    """Run Meltano ingestion (tap-csv -> target-bigquery)."""
    try:
        subprocess.run(
            ["meltano", "run", "tap-csv", "target-bigquery"],
            cwd="meltano_project",
            check=True,
        )
        context.log.info("Meltano ingestion complete ✅")
    except subprocess.CalledProcessError as e:
        raise Exception(f"Meltano ingestion failed: {e}")
    return "bigquery_staging_loaded"
