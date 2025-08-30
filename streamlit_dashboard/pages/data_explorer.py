"""
Data Explorer Page for Olist Analytics Dashboard
Provides additional data exploration and analysis capabilities
"""

import streamlit as st
import pandas as pd
from utils.bigquery_client import (
    init_connection, 
    test_connection, 
    get_table_info, 
    get_sample_data,
    validate_table_exists
)
from utils.data_queries import validate_marts_data
import plotly.express as px

st.set_page_config(
    page_title="Data Explorer - Olist Analytics",
    page_icon="🔍",
    layout="wide"
)

def main():
    """Data Explorer main function"""
    
    st.title("🔍 Data Explorer")
    st.markdown("Explore and validate the data in your BigQuery marts")
    
    # Sidebar for controls
    with st.sidebar:
        st.header("Explorer Controls")
        
        # Connection test
        if st.button("🔌 Test BigQuery Connection"):
            with st.spinner("Testing connection..."):
                test_connection()
        
        st.markdown("---")
        
        # Table selection
        st.subheader("Table Explorer")
        table_name = st.selectbox(
            "Select Table to Explore",
            options=[
                "fact_sales",
                "dim_customers",
                "dim_products", 
                "dim_sellers",
                "dim_orders",
                "dim_payments",
                "dim_reviews",
                "dim_date"
            ]
        )
        
        sample_limit = st.slider("Sample Size", 5, 100, 20)
        
        if st.button("📊 Load Sample Data"):
            st.session_state['explore_table'] = table_name
            st.session_state['sample_limit'] = sample_limit
    
    # Main content
    col1, col2 = st.columns([2, 1])
    
    with col1:
        st.header("Data Validation Status")
        
        # Validate marts data
        if st.button("✅ Validate All Tables"):
            with st.spinner("Validating marts data..."):
                validation_results = validate_marts_data()
                
                if validation_results:
                    st.success("Data validation completed!")
                    
                    # Display validation results
                    for table, status in validation_results.items():
                        if status:
                            st.success(f"✅ {table}: Data available")
                        else:
                            st.error(f"❌ {table}: No data or table missing")
                else:
                    st.error("Failed to validate marts data")
    
    with col2:
        st.header("Connection Info")
        
        try:
            client = init_connection()
            st.success(f"✅ Connected to: {client.project}")
            
            # Get table info
            table_info = get_table_info()
            if not table_info.empty:
                st.info(f"📊 Found {len(table_info)} tables in marts")
                
        except Exception as e:
            st.error(f"❌ Connection failed: {str(e)}")
    
    # Table exploration section
    if 'explore_table' in st.session_state:
        st.markdown("---")
        st.header(f"📊 Exploring: {st.session_state['explore_table']}")
        
        try:
            # Get sample data
            sample_data = get_sample_data(
                st.session_state['explore_table'], 
                st.session_state['sample_limit']
            )
            
            if not sample_data.empty:
                st.success(f"✅ Loaded {len(sample_data)} rows from {st.session_state['explore_table']}")
                
                # Display data info
                col1, col2 = st.columns(2)
                
                with col1:
                    st.subheader("Data Shape")
                    st.write(f"Rows: {sample_data.shape[0]}")
                    st.write(f"Columns: {sample_data.shape[1]}")
                
                with col2:
                    st.subheader("Data Types")
                    st.write(sample_data.dtypes)
                
                # Display sample data
                st.subheader("Sample Data")
                st.dataframe(sample_data, use_container_width=True)
                
                # Basic statistics for numeric columns
                numeric_cols = sample_data.select_dtypes(include=['number']).columns
                if len(numeric_cols) > 0:
                    st.subheader("Numeric Column Statistics")
                    st.write(sample_data[numeric_cols].describe())
                
                # Column value counts for categorical columns
                categorical_cols = sample_data.select_dtypes(include=['object']).columns
                if len(categorical_cols) > 0:
                    st.subheader("Categorical Column Value Counts")
                    
                    for col in categorical_cols[:3]:  # Limit to first 3 columns
                        if sample_data[col].nunique() <= 20:  # Only show if not too many unique values
                            value_counts = sample_data[col].value_counts()
                            
                            # Create bar chart
                            fig = px.bar(
                                x=value_counts.index, 
                                y=value_counts.values,
                                title=f"Value Distribution: {col}",
                                labels={'x': col, 'y': 'Count'}
                            )
                            st.plotly_chart(fig, use_container_width=True)
                        else:
                            st.write(f"**{col}**: {sample_data[col].nunique()} unique values (too many to display)")
                
            else:
                st.warning(f"⚠️ No data found in table {st.session_state['explore_table']}")
                
        except Exception as e:
            st.error(f"❌ Error exploring table: {str(e)}")
    
    # Data quality insights
    st.markdown("---")
    st.header("📈 Data Quality Insights")
    
    if st.button("🔍 Analyze Data Quality"):
        with st.spinner("Analyzing data quality..."):
            try:
                # Get table information
                table_info = get_table_info()
                
                if not table_info.empty:
                    st.success("Data quality analysis completed!")
                    
                    # Display table sizes
                    col1, col2 = st.columns(2)
                    
                    with col1:
                        st.subheader("Table Sizes")
                        size_df = table_info[['table_id', 'row_count']].copy()
                        size_df['size_mb'] = table_info['size_bytes'] / (1024 * 1024)
                        
                        # Create size visualization
                        fig = px.bar(
                            size_df,
                            x='table_id',
                            y='row_count',
                            title="Row Counts by Table",
                            labels={'table_id': 'Table', 'row_count': 'Row Count'}
                        )
                        st.plotly_chart(fig, use_container_width=True)
                    
                    with col2:
                        st.subheader("Storage Usage")
                        fig = px.pie(
                            size_df,
                            values='size_mb',
                            names='table_id',
                            title="Storage Usage by Table (MB)"
                        )
                        st.plotly_chart(fig, use_container_width=True)
                    
                    # Display detailed table info
                    st.subheader("Detailed Table Information")
                    st.dataframe(
                        table_info[['table_id', 'row_count', 'size_mb', 'created', 'last_modified']],
                        use_container_width=True
                    )
                    
                else:
                    st.warning("No table information available")
                    
            except Exception as e:
                st.error(f"❌ Error analyzing data quality: {str(e)}")
    
    # Footer
    st.markdown("---")
    st.markdown(
        "<div style='text-align: center; color: #666;'>"
        "Data Explorer | Olist Analytics Dashboard"
        "</div>",
        unsafe_allow_html=True
    )

if __name__ == "__main__":
    main()





