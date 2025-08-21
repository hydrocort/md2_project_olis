
# Meltano Project

Put CSVs under `meltano_project/data/`:
- orders.csv
- customers.csv
- order_items.csv
- products.csv
- payments.csv

Run ingestion:
```
cd meltano_project
meltano install
meltano run tap-csv target-bigquery
```
