
from dagster import asset

@asset(deps=["run_data_quality_tests"], required_resource_keys={"bigquery"})
def exploratory_analysis(context):
    bq = context.resources.bigquery
    marts_dataset = bq.marts_dataset
    q = f'''
    SELECT
      FORMAT_DATE('%Y-%m', order_date) AS month,
      SUM(order_amount) AS monthly_sales
    FROM {marts_dataset}_marts.fact_sales
    GROUP BY 1
    ORDER BY 1
    '''
    df = bq.query(q).to_dataframe()
    df.to_csv("outputs_monthly_sales.csv", index=False)
    context.log.info("Wrote outputs_monthly_sales.csv with %d rows" % len(df))
    return df
