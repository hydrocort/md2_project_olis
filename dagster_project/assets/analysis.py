
from dagster import asset

@asset(deps=["run_data_quality_tests"], required_resource_keys={"bigquery"})
def exploratory_analysis(context):
    bq = context.resources.bigquery
    q = '''
    SELECT
      FORMAT_DATE('%Y-%m', order_ts) AS month,
      SUM(order_amount) AS monthly_sales
    FROM my_dataset.fact_sales
    GROUP BY 1
    ORDER BY 1
    '''
    df = bq.query(q).to_dataframe()
    df.to_csv("outputs_monthly_sales.csv", index=False)
    context.log.info("Wrote outputs_monthly_sales.csv with %d rows" % len(df))
    return df
