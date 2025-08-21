from dagster_dbt import dbt_assets, DbtProject
import os, json, subprocess, pathlib

DBT_PROJECT_DIR = "dbt_project"
MANIFEST_PATH = os.path.join(DBT_PROJECT_DIR, "target", "manifest.json")

# Ensure deps & manifest exist at import time (development convenience). If parse fails
# this will surface quickly. In production you might pre-build the manifest.
def _ensure_manifest():
    if not os.path.exists(MANIFEST_PATH):
        # Run dbt deps + parse to create manifest
        subprocess.run(["dbt", "deps"], cwd=DBT_PROJECT_DIR, check=True)
        subprocess.run(["dbt", "parse"], cwd=DBT_PROJECT_DIR, check=True)
    return MANIFEST_PATH

manifest = _ensure_manifest()

@dbt_assets(manifest=manifest, name="dbt_models")
def dbt_assets(context):
    # Use configured dbt CLI resource if available for event streaming
    dbt_cli = getattr(context.resources, "dbt", None)
    if dbt_cli:
        yield from dbt_cli.cli("build").stream()
    else:
        # Fallback direct subprocess build (no fine-grained events)
        context.log.info("dbt resource not configured, running subprocess dbt build")
        subprocess.run(["dbt", "build"], cwd=DBT_PROJECT_DIR, check=True)
