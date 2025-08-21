
from dagster import ScheduleDefinition
from dagster_project.jobs.pipeline import full_pipeline_job

daily_pipeline_schedule = ScheduleDefinition(
    job=full_pipeline_job,
    cron_schedule="0 0 * * *",
)
