from dagster import resource
from google.cloud import bigquery
import os
from dagster_project.resources.env_loader import load_dotenv


class BigQueryResource:
    def __init__(self, client, raw_dataset, staging_dataset, marts_dataset, credentials_path):
        self.client = client
        self.raw_dataset = raw_dataset
        self.staging_dataset = staging_dataset
        self.marts_dataset = marts_dataset
        self.credentials_path = credentials_path

    def query(self, *args, **kwargs):
        return self.client.query(*args, **kwargs)


@resource()
def bigquery_resource(_):
    # Load environment variables from .env
    # (load_dotenv is imported for side effect)
    project = os.getenv("PROJECT_ID")
    raw_dataset = os.getenv("RAW_DATASET_NAME")
    staging_dataset = os.getenv("STAGING_DATASET_NAME")
    marts_dataset = os.getenv("MARTS_DATASET_NAME")
    credentials_path = os.getenv("CREDENTIALS_PATH")
    if credentials_path:
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = credentials_path
    client = bigquery.Client(project=project)
    return BigQueryResource(client, raw_dataset, staging_dataset, marts_dataset, credentials_path)