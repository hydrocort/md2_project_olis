# Olist E-commerce Analytics Dashboard

An interactive Streamlit dashboard for analyzing Brazilian e-commerce data from Olist, built on top of a BigQuery data warehouse with dbt transformations.

## 🎯 Business Questions Answered

This dashboard provides insights into 8 critical business questions:

1. **📈 Monthly Sales Trends** - Revenue, order volume, and average order values over time
2. **🏷️ Top Products & Categories** - Highest revenue and sales volume product categories
3. **🌍 Geographic Sales Distribution** - Sales patterns across Brazilian regions
4. **👥 Customer Purchase Behavior** - Customer segments, frequency, and lifetime value
5. **💳 Payment Method Impact** - How payment methods affect sales and order values
6. **🏪 Seller Performance Analysis** - Seller metrics by region and performance
7. **⭐ Reviews & Sales Correlation** - Customer review impact on sales performance
8. **🚚 Delivery Time Patterns** - Delivery performance metrics across regions

## 🏗️ Architecture

```
[BigQuery Marts] → [Streamlit Dashboard] → [Interactive Analytics]
     ↑
[dbt Transformations]
     ↑
[Raw Data (CSV → BigQuery)]
```

## 🚀 Quick Start

### Prerequisites

- Python 3.8+ with conda environment `mod2proj`
- Google Cloud Project with BigQuery enabled
- Service account with BigQuery access
- Olist data pipeline completed (stages 1-2)

### Installation

1. **Navigate to the dashboard directory:**
   ```bash
   cd streamlit_dashboard
   ```

2. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

3. **Configure authentication:**
   - Copy your GCP service account JSON to `.streamlit/secrets.toml`
   - Update the project ID and credentials in the secrets file

4. **Run the dashboard:**
   ```bash
   streamlit run streamlit_app.py
   ```

## ⚙️ Configuration

### Authentication Setup

1. **Create `.streamlit/secrets.toml`** with your GCP service account:
   ```toml
   [gcp_service_account]
   type = "service_account"
   project_id = "your-gcp-project-id"
   private_key_id = "your-private-key-id"
   private_key = "-----BEGIN PRIVATE KEY-----\nYOUR_PRIVATE_KEY\n-----END PRIVATE KEY-----\n"
   client_email = "your-service-account@project.iam.gserviceaccount.com"
   # ... other required fields
   ```

2. **Update BigQuery configuration:**
   ```toml
   [bigquery]
   project_id = "your-gcp-project-id"
   dataset_id = "your-gcp-marts-dataset-name"
   location = "US"
   ```

### Environment Variables

Set these environment variables for local development:
```bash
export GOOGLE_APPLICATION_CREDENTIALS="path/to/service-account.json"
export BIGQUERY_PROJECT_ID="your-gcp-project-id"
export BIGQUERY_DATASET_ID="your-gcp-marts-dataset-name"
```

## 📊 Dashboard Features

### Main Dashboard (`streamlit_app.py`)
- **8 Business Question Tabs** - Dedicated analysis for each business question
- **Interactive Filters** - Year and region-based filtering
- **Real-time Data** - Connected to BigQuery marts with 10-minute caching
- **Responsive Layout** - Optimized for desktop and mobile viewing

### Data Explorer (`pages/data_explorer.py`)
- **Table Validation** - Verify all marts tables exist and contain data
- **Sample Data Viewing** - Explore table contents and structure
- **Data Quality Analysis** - Table sizes, row counts, and storage usage
- **Connection Testing** - Validate BigQuery connectivity

### Key Features
- **Caching**: 10-minute TTL for query results
- **Error Handling**: Graceful failure handling with user-friendly messages
- **Responsive Design**: Works on various screen sizes
- **Interactive Charts**: Plotly-based visualizations with hover details
- **Data Export**: Download charts and data tables

## 🔧 Development

### Project Structure
```
streamlit_dashboard/
├── streamlit_app.py              # Main dashboard application
├── .streamlit/                   # Streamlit configuration
│   ├── config.toml              # Dashboard settings
│   └── secrets.toml             # GCP credentials (template)
├── utils/                        # Utility modules
│   ├── __init__.py              # Package initialization
│   ├── bigquery_client.py       # BigQuery connection management
│   ├── data_queries.py          # Business question queries
│   └── visualization_helpers.py  # Chart creation utilities
├── pages/                        # Additional pages
│   └── data_explorer.py         # Data exploration and validation
├── requirements.txt              # Python dependencies
└── README.md                     # This file
```

### Code Style
- Follow PEP 8 guidelines
- Use type hints for function parameters
- Add docstrings for all functions
- Handle exceptions gracefully
- Use Streamlit caching for performance

## 📈 Data Sources

The dashboard connects to BigQuery marts created by dbt transformations:

### Core Tables
- **`fact_sales`** - Central fact table with order item grain
- **`dim_customers`** - Customer dimensions with regional groupings
- **`dim_products`** - Product dimensions with category translations
- **`dim_sellers`** - Seller dimensions with regional classifications
- **`dim_orders`** - Order dimensions with delivery metrics
- **`dim_payments`** - Payment dimensions with method aggregations
- **`dim_reviews`** - Review dimensions with timing calculations
- **`dim_date`** - Date dimensions with business calendar attributes

### Data Refresh
- **Cache TTL**: 10 minutes (configurable)
- **Manual Refresh**: Sidebar refresh button
- **Real-time**: Direct BigQuery queries

## 🚀 Deployment

### Local Development
```bash
streamlit run streamlit_app.py --server.port 8501
```

### Streamlit Cloud
1. Push code to GitHub repository
2. Connect repository to Streamlit Cloud
3. Configure secrets in Streamlit Cloud dashboard
4. Deploy automatically on code changes

### Docker Deployment
```dockerfile
FROM python:3.9-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt
COPY . .
EXPOSE 8501
CMD ["streamlit", "run", "streamlit_app.py", "--server.port=8501", "--server.address=0.0.0.0"]
```

## 🔍 Troubleshooting

### Common Issues

1. **Authentication Errors:**
   - Verify service account JSON in secrets.toml
   - Check BigQuery permissions
   - Ensure project ID is correct

2. **Data Loading Issues:**
   - Verify marts tables exist in BigQuery
   - Check table schemas match expected structure
   - Validate data quality in dbt models

3. **Performance Issues:**
   - Check BigQuery quota limits
   - Verify caching is working
   - Optimize complex queries

### Debug Mode
Enable debug logging:
```python
import logging
logging.basicConfig(level=logging.DEBUG)
```

### Connection Testing
Use the Data Explorer page to test BigQuery connectivity and validate table access.

## 📚 API Reference

### Core Functions

#### `utils.bigquery_client`
- `init_connection()` - Initialize BigQuery client
- `execute_query(query)` - Execute SQL query
- `test_connection()` - Test BigQuery connectivity
- `get_table_info()` - Get table metadata
- `get_sample_data()` - Get sample data from table
- `validate_table_exists()` - Check if table exists

#### `utils.data_queries`
- `get_monthly_sales_trends()` - Business question 1: Monthly sales trends
- `get_top_products_categories()` - Business question 2: Top products/categories performance
- `get_sales_by_region()` - Business question 3: Geographic sales distribution
- `get_sales_by_state()` - Geographic sales by Brazilian state
- `get_customer_seller_flow()` - Customer-seller relationship analysis
- `get_customer_behavior()` - Business question 4: Customer purchase behavior
- `get_customer_segmentation()` - Customer segmentation analysis
- `get_customer_frequency_analysis()` - Customer order frequency patterns
- `get_payment_analysis()` - Business question 5: Payment method impact
- `get_installment_analysis()` - Payment installment analysis
- `get_seller_performance()` - Business question 6: Seller performance by region
- `get_top_sellers()` - Top performing sellers ranking
- `get_seller_product_diversity()` - Seller product variety analysis
- `get_reviews_sales_correlation()` - Business question 7: Reviews-sales correlation
- `get_review_score_distribution()` - Review score distribution analysis
- `get_review_timing_analysis()` - Review timing patterns
- `get_delivery_patterns()` - Business question 8: Delivery time patterns
- `get_delivery_time_distribution()` - Delivery time distribution analysis
- `get_delivery_efficiency_analysis()` - Delivery efficiency metrics
- `get_dashboard_summary()` - Overall dashboard metrics and KPIs

#### `utils.visualization_helpers`
- `create_line_chart()` - Standardized line charts
- `create_bar_chart()` - Standardized bar charts
- `create_pie_chart()` - Standardized pie charts
- `create_customer_behavior_pie_chart()` - Customer behavior visualization
- `create_customer_value_pie_chart()` - Customer value segmentation
- `create_metric_card()` - Metric display cards
- `create_sales_trend_chart()` - Multi-metric sales overview
- `create_regional_heatmap()` - Geographic analysis
- `create_payment_method_chart()` - Payment analysis
- `create_delivery_performance_chart()` - Delivery metrics

## 🤝 Contributing

1. Follow the existing code structure
2. Add tests for new functionality
3. Update documentation for new features
4. Use meaningful commit messages
5. Test locally before submitting changes

## 📄 License

This project is part of the Olist E-commerce Data Pipeline project.

## 🆘 Support

For issues and questions:
1. Check the troubleshooting section
2. Review BigQuery logs and permissions
3. Verify dbt model execution
4. Test with sample data first

---

**Built with ❤️ using Streamlit, BigQuery, and dbt**





