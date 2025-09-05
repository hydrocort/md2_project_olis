
from dagster import asset
import csv
import subprocess
import os
from pathlib import Path

@asset(group_name="Ingestion", compute_kind="meltano")
def meltano_ingestion(context):
    """Run Meltano ingestion (tap-csv -> target-bigquery)."""
    project_dir = Path("meltano_project")

    # Basic existence and permission checks before invoking Meltano
    if not project_dir.exists():
        raise FileNotFoundError(f"Meltano project directory not found: {project_dir.resolve()}")
    if not project_dir.is_dir():
        raise NotADirectoryError(f"Path exists but is not a directory: {project_dir.resolve()}")
    # Need read + execute on directory to traverse; write not strictly required for run
    if not os.access(project_dir, os.R_OK | os.X_OK):
        raise PermissionError(f"Insufficient permissions to access Meltano project directory: {project_dir.resolve()}")

    # check all csv files, rewrite with suffix _cleaned, removing BOM and trimming headers
    # This is a basic check; more complex validation can be added as needed
    data_dir = project_dir / "data"
    for csv_file in data_dir.glob("*.csv"):
        # Only process files that do NOT already have '_cleaned.csv' in their name
        if csv_file.name.endswith("_cleaned.csv"):
            continue
        tmp = csv_file.with_name(csv_file.stem + "_cleaned.csv")
        # Always overwrite the cleaned file if it exists
        with open(csv_file, "r", encoding="utf-8-sig", newline="") as f_in, \
             open(tmp, "w", encoding="utf-8", newline="") as f_out:
            r = csv.reader(f_in)
            w = csv.writer(f_out)
            # Find the first valid non-empty header row
            header = None
            while header is None:
                try:
                    possible_header = next(r)
                except StopIteration:
                    break  # skip empty files
                # Header must have at least one non-empty field and not be all empty or delimiters
                if any(field.strip() for field in possible_header) and not all(field.strip() == '' for field in possible_header):
                    header = [h.strip() for h in possible_header]
            if header is None:
                continue  # file only had empty lines
            w.writerow(header)
            for row in r:
                # SKIP empty rows
                if any(field.strip() for field in row):
                    row = [field.strip() for field in row]
                    w.writerow(row)

    try:
        result = subprocess.run(
            ["meltano", "run", "tap-csv", "target-bigquery"],
            cwd=project_dir,
            check=True,
            capture_output=True,
            text=True,
        )
        context.log.info(f"Meltano output: {result.stdout}")
        context.log.error(f"Meltano errors: {result.stderr}")
    except subprocess.CalledProcessError as e:
        context.log.error(f"Meltano failed with output: {e.stdout}")
        context.log.error(f"Meltano failed with errors: {e.stderr}")
        raise Exception(f"Meltano ingestion failed: {e}")
    return "bigquery_staging_loaded"
