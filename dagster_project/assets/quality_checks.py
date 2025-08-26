
from dagster import asset

# Depend on any dbt-produced assets by using a pattern; since our dbt assets are named with prefix dbt_models
@asset(deps=["fact_sales"], required_resource_keys={"bigquery"})
def run_data_quality_tests(context):
    """Simple SQL-based quality checks against BigQuery."""
    bq = context.resources.bigquery
    marts_dataset = bq.marts_dataset
    q1 = f"SELECT COUNT(*) AS c FROM {marts_dataset}_marts.fact_sales WHERE customer_id IS NULL"
    c1 = bq.query(q1).to_dataframe().iloc[0]["c"]
    if c1 > 0:
        raise Exception(f"Null customer_id in fact_sales: {c1}")

    q2 = f"SELECT COUNT(*) AS c FROM {marts_dataset}_marts.fact_sales WHERE order_amount < 0"
    c2 = bq.query(q2).to_dataframe().iloc[0]["c"]
    if c2 > 0:
        raise Exception(f"Negative order_amount rows: {c2}")

    context.log.info("Data quality checks passed")
    return "quality_passed"
