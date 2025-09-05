# --- top of file ---
import os, subprocess
from pathlib import Path
from dagster import asset, MaterializeResult, MetadataValue, AssetKey

DBT_PROJECT_DIR = "/home/gendaff/sctp_project/md2_project_olis/dbt_project"

# If profiles.yml is in ~/.dbt, set this instead:
# DBT_PROFILES_DIR = os.path.expanduser("~/.dbt")
DBT_PROFILES_DIR = DBT_PROJECT_DIR
ELEMENTARY_PROFILES_DIR = DBT_PROFILES_DIR

# matches your profiles.yml
DBT_TARGET = "dev"   # dbt target name
EDR_ENV = "dev"      # Elementary environment label

REPORT_PATH = "/home/gendaff/sctp_project/md2_project_olis/artifacts/elementary_report.html"


def _run(cmd: str, logger=None, *, env: dict | None = None, cwd: str | None = None):
    """Run shell command and raise with full stdout/stderr on failure."""
    if env is None:
        env = os.environ.copy()

    if logger:
        logger.info(f"Running: {cmd}")

    p = subprocess.run(cmd, shell=True, env=env, cwd=cwd, capture_output=True, text=True)

    if logger and p.stdout:
        logger.info(p.stdout)
    if logger and p.stderr:
        logger.error(p.stderr)

    if p.returncode != 0:
        raise RuntimeError(
            f"Command failed (exit {p.returncode}): {cmd}\n\n"
            f"STDOUT:\n{p.stdout}\n\nSTDERR:\n{p.stderr}"
        )


@asset(name="dbt_elementary_build", deps=[AssetKey("dbt_test")], compute_kind="dbt", group_name="Elementary")
def dbt_elementary_build(context):
    _run(f"dbt deps --project-dir {DBT_PROJECT_DIR}", context.log)

    # Build Elementary package models into ELEMENTARY_DATASET_NAME
    _run(
        f"dbt run --select elementary --full-refresh "
        f"--project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROFILES_DIR} "
        f"--profile elementary --target {DBT_TARGET}",
        context.log,
    )

    _run(
        f"dbt test --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROFILES_DIR} "
        f"--profile elementary --target {DBT_TARGET}",
        context.log,
    )


@asset(name="elementary_monitor", deps=[AssetKey("dbt_elementary_build")], compute_kind="cli", group_name="Elementary")
def elementary_monitor(context):
    slack_webhook = os.getenv("SLACK_WEBHOOK_URL")
    slack_token = os.getenv("SLACK_BOT_TOKEN")
    slack_channel = os.getenv("SLACK_CHANNEL_NAME")  # e.g. "#data-alerts"

    base = (
        f"edr monitor --project-dir {DBT_PROJECT_DIR} "
        f"--profiles-dir {ELEMENTARY_PROFILES_DIR} "
        f"--project-profile-target {DBT_TARGET} "
        f"--env {EDR_ENV}"
    )

    if slack_webhook:
        cmd = base + f" --slack-webhook {slack_webhook}"
    elif slack_token and slack_channel:
        cmd = base + f" --slack-token {slack_token} --slack-channel-name {slack_channel}"
    else:
        context.log.warning("No Slack/Teams credentials found; skipping edr monitor.")
        return  # gracefully skip

    _run(cmd, context.log)



@asset(name="elementary_report", deps=[AssetKey("dbt_elementary_build")], compute_kind="cli", group_name="Elementary")
def elementary_report(context):
    Path(REPORT_PATH).parent.mkdir(parents=True, exist_ok=True)
    _run(
        f"edr report --project-dir {DBT_PROJECT_DIR} "
        f"--profiles-dir {ELEMENTARY_PROFILES_DIR} "
        f"--project-profile-target {DBT_TARGET} "
        f"--env {EDR_ENV} "
        f"--file-path {REPORT_PATH}",
        context.log,
    )

    return MaterializeResult(
        metadata={
            "report_path": MetadataValue.path(REPORT_PATH),
            "preview": MetadataValue.url("file://" + os.path.abspath(REPORT_PATH)),
        }
    )
