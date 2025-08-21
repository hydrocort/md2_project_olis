
from dagster import resource
from google.cloud import bigquery

@resource(config_schema={"project": str})
def bigquery_resource(init_context):
    project = init_context.resource_config["project"]
    client = bigquery.Client(project=project)
    return client
