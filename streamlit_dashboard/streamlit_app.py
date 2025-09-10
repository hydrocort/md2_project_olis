"""
Olist E-commerce Analytics Dashboard
Main application for answering 8 critical business questions
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from utils.bigquery_client import init_connection
from utils.data_queries import (
    get_monthly_sales_trends,
    get_top_products_categories,
    get_sales_by_region,
    get_sales_by_state,
    get_customer_seller_flow,
    get_customer_behavior,
    get_customer_segmentation,
    get_customer_frequency_analysis,
    get_payment_analysis,
    get_installment_analysis,
    get_seller_performance,
    get_top_sellers,
    get_seller_product_diversity,
    get_reviews_sales_correlation,
    get_review_score_distribution,
    get_review_timing_analysis,
    get_delivery_patterns,
    get_delivery_time_distribution,
    get_delivery_efficiency_analysis
)
from utils.visualization_helpers import (
    create_line_chart,
    create_bar_chart,
    create_pie_chart,
    create_metric_card,
    create_regional_heatmap,
    create_sales_trend_chart,
    create_payment_method_chart,
    create_customer_behavior_pie_chart,
    create_customer_value_pie_chart
)

# Page configuration
st.set_page_config(
    page_title="Olist E-commerce Analytics",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for better styling and responsive tabs
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        font-weight: bold;
        color: #1f77b4;
        text-align: center;
        margin-bottom: 2rem;
    }
    .metric-card {
        background-color: #f0f2f6;
        padding: 1rem;
        border-radius: 0.5rem;
        border-left: 4px solid #1f77b4;
    }
    .tab-content {
        padding: 1rem 0;
    }
    
    /* Enhanced tab styling for better mobile experience */
    .stTabs [data-baseweb="tab-list"] {
        gap: 4px;
        overflow-x: auto;
        overflow-y: hidden;
        scrollbar-width: thin;
        scrollbar-color: #d4d4d4 #f0f2f6;
        scroll-behavior: smooth;
        padding: 4px 8px;
        white-space: nowrap;
        -webkit-overflow-scrolling: touch;
    }
    
    .stTabs [data-baseweb="tab-list"]::-webkit-scrollbar {
        height: 6px;
    }
    
    .stTabs [data-baseweb="tab-list"]::-webkit-scrollbar-track {
        background: #f0f2f6;
        border-radius: 3px;
    }
    
    .stTabs [data-baseweb="tab-list"]::-webkit-scrollbar-thumb {
        background: #d4d4d4;
        border-radius: 3px;
    }
    
    .stTabs [data-baseweb="tab-list"]::-webkit-scrollbar-thumb:hover {
        background: #b3b3b3;
    }
    
    .stTabs [data-baseweb="tab"] {
        flex-shrink: 0;
        white-space: nowrap;
        min-width: fit-content;
        font-size: 14px;
        padding: 8px 16px;
    }
    
    /* Mobile responsive adjustments */
    @media (max-width: 768px) {
        .stTabs [data-baseweb="tab"] {
            font-size: 12px;
            padding: 6px 12px;
        }
        
        .stTabs [data-baseweb="tab-list"] {
            gap: 2px;
            padding: 2px 4px;
        }
        
        .main-header {
            font-size: 2rem;
        }
    }
    
    @media (max-width: 480px) {
        .stTabs [data-baseweb="tab"] {
            font-size: 10px;
            padding: 4px 8px;
        }
        
        .main-header {
            font-size: 1.5rem;
        }
    }
    
    /* Scroll hint for mobile */
    .stTabs::before {
        content: "";
        position: absolute;
        top: 0;
        right: 0;
        width: 20px;
        height: 100%;
        background: linear-gradient(to left, rgba(255,255,255,0.8), transparent);
        pointer-events: none;
        z-index: 1;
    }
    
    @media (min-width: 769px) {
        .stTabs::before {
            display: none;
        }
    }
</style>
""", unsafe_allow_html=True)

def main():
    """Main dashboard application"""
    
    # Header
    st.markdown('<h1 class="main-header">Olist E-commerce Analytics Dashboard</h1>', unsafe_allow_html=True)
    
    # Mobile scroll hint
    if st.session_state.get('show_mobile_hint', True):
        col1, col2, col3 = st.columns([1, 2, 1])
        with col2:
            st.info("📱 On mobile? Swipe left/right on tabs to navigate all sections", icon="💡")
        if st.button("Got it!", key="dismiss_hint"):
            st.session_state.show_mobile_hint = False
            st.rerun()
    
    # Sidebar
    with st.sidebar:
        st.header("Dashboard Controls")
        
        # Data refresh button
        if st.button("🔄 Refresh Data", type="primary"):
            st.cache_data.clear()
            st.success("Cache cleared! Data will refresh on next query.")
        
        st.markdown("---")
        
        # Filter options
        st.subheader("Filters")
        selected_year = st.selectbox(
            "Select Year",
            options=["All Years", "2016", "2017", "2018"],
            index=0
        )
        
        selected_region = st.selectbox(
            "Select Region",
            options=["All Regions", "North", "Northeast", "Southeast", "South", "Central-West"],
            index=0
        )
        
        st.markdown("---")
        
        # Data info
        st.subheader("Data Info")
        st.info("""
        **Last Updated**: Data refreshed from BigQuery marts
        **Data Source**: Olist e-commerce transactions
        **Cache TTL**: 10 minutes
        """)
    
    # Main content tabs
    tab1, tab2, tab3, tab4, tab5, tab6, tab7, tab8 = st.tabs([
        "📈 Monthly Sales Trends",
        "🏷️ Top Products & Categories", 
        "🌍 Geographic Sales Distribution",
        "👥 Customer Purchase Behavior",
        "💳 Payment Method Impact",
        "🏪 Seller Performance Analysis",
        "⭐ Reviews & Sales Correlation",
        "🚚 Delivery Time Patterns"
    ])
    
    # Tab 1: Monthly Sales Trends
    with tab1:
        st.header("Monthly Sales Trends")
        st.markdown("Analyze sales revenue, order volume, and average order values over time")
        
        try:
            # Get data with filtering
            sales_data = get_monthly_sales_trends(year_filter=selected_year, region_filter=selected_region)
            
            # Display active filters
            if selected_year != "All Years" or selected_region != "All Regions":
                filter_text = []
                if selected_year != "All Years":
                    filter_text.append(f"Year: {selected_year}")
                if selected_region != "All Regions":
                    filter_text.append(f"Region: {selected_region}")
                st.info(f"🎯 Active filters: {' | '.join(filter_text)}")
            
            if not sales_data.empty:
                # Calculate summary metrics
                total_sales = sales_data['total_sales'].sum()
                total_orders = sales_data['total_orders'].sum()
                total_items = sales_data['total_items'].sum()
                avg_order_value = total_sales / total_orders if total_orders > 0 else 0
                
                # Display KPI metrics
                col1, col2, col3, col4 = st.columns(4)
                with col1:
                    st.metric(
                        "Total Sales", 
                        f"${total_sales:,.2f}",
                        help="Total revenue from all orders"
                    )
                with col2:
                    st.metric(
                        "Total Orders", 
                        f"{total_orders:,}",
                        help="Total number of orders"
                    )
                with col3:
                    st.metric(
                        "Total Items", 
                        f"{total_items:,}",
                        help="Total number of items sold"
                    )
                with col4:
                    st.metric(
                        "Avg Order Value", 
                        f"${avg_order_value:.2f}",
                        help="Average value per order"
                    )
                
                # Create comprehensive sales trend chart
                st.subheader("Sales Performance Over Time")
                sales_chart = create_sales_trend_chart(sales_data)
                st.plotly_chart(sales_chart, use_container_width=True)
                
                # Detailed data table
                st.subheader("Detailed Monthly Breakdown")
                
                # Format the data for display
                display_data = sales_data.copy()
                display_data['total_sales'] = display_data['total_sales'].apply(lambda x: f"${x:,.2f}")
                display_data['avg_order_value'] = display_data['avg_order_value'].apply(lambda x: f"${x:.2f}")
                display_data['total_orders'] = display_data['total_orders'].apply(lambda x: f"{x:,}")
                display_data['total_items'] = display_data['total_items'].apply(lambda x: f"{x:,}")
                
                # Select and rename columns for display
                display_columns = {
                    'month_year': 'Month',
                    'month_name': 'Month Name',
                    'year': 'Year',
                    'total_sales': 'Total Sales',
                    'total_orders': 'Total Orders',
                    'total_items': 'Total Items',
                    'avg_order_value': 'Avg Order Value'
                }
                
                st.dataframe(
                    display_data[list(display_columns.keys())].rename(columns=display_columns),
                    use_container_width=True,
                    hide_index=True
                )
                
                # Download option
                csv = sales_data.to_csv(index=False)
                st.download_button(
                    label="📥 Download Monthly Sales Data",
                    data=csv,
                    file_name=f"monthly_sales_trends_{selected_year if selected_year != 'All Years' else 'all_years'}.csv",
                    mime="text/csv"
                )
                
            else:
                st.warning("No sales data available for the selected filters.")
            
        except Exception as e:
            st.error(f"Error loading sales trends data: {str(e)}")
            st.info("Please check your BigQuery connection and data availability.")
    
    # Tab 2: Top Products & Categories
    with tab2:
        st.header("Top Products & Categories")
        st.markdown("Identify highest revenue and sales volume product categories")
        
        try:
            # Controls for this tab
            col1, col2 = st.columns([3, 1])
            with col1:
                # Display active filters
                if selected_year != "All Years" or selected_region != "All Regions":
                    filter_text = []
                    if selected_year != "All Years":
                        filter_text.append(f"Year: {selected_year}")
                    if selected_region != "All Regions":
                        filter_text.append(f"Region: {selected_region}")
                    st.info(f"🎯 Active filters: {' | '.join(filter_text)}")
            
            with col2:
                # Number of categories to show
                top_n = st.selectbox(
                    "Top N Categories",
                    options=[10, 15, 20, 25, 30],
                    index=2,  # Default to 20
                    help="Number of top categories to display"
                )
            
            # Get data with filtering
            categories_data = get_top_products_categories(
                limit=top_n, 
                year_filter=selected_year, 
                region_filter=selected_region
            )
            
            if not categories_data.empty:
                # Summary KPIs
                total_categories = len(categories_data)
                total_revenue = categories_data['total_revenue'].sum()
                total_products = categories_data['unique_products'].sum()
                total_items_sold = categories_data['items_sold'].sum()
                
                # Display summary metrics
                col1, col2, col3, col4 = st.columns(4)
                with col1:
                    st.metric(
                        "Categories Analyzed",
                        f"{total_categories}",
                        help="Number of product categories in results"
                    )
                with col2:
                    st.metric(
                        "Total Revenue",
                        f"${total_revenue:,.2f}",
                        help="Combined revenue from top categories"
                    )
                with col3:
                    st.metric(
                        "Unique Products",
                        f"{total_products:,}",
                        help="Total unique products in top categories"
                    )
                with col4:
                    st.metric(
                        "Items Sold",
                        f"{total_items_sold:,}",
                        help="Total items sold across top categories"
                    )
                
                st.markdown("---")
                
                # Main visualizations
                col1, col2 = st.columns([3, 2])
                
                with col1:
                    # Bar chart for top categories by revenue
                    st.subheader("Top Categories by Revenue")
                    revenue_chart = create_bar_chart(
                        categories_data.head(15),  # Show top 15 for readability
                        x_col='category',
                        y_col='total_revenue',
                        title='Category Revenue Performance',
                        x_title='Product Category',
                        y_title='Total Revenue ($)',
                        height=500
                    )
                    # Rotate x-axis labels for better readability
                    revenue_chart.update_xaxes(tickangle=45)
                    st.plotly_chart(revenue_chart, use_container_width=True)
            
                with col2:
                    # Pie chart for revenue distribution
                    st.subheader("Revenue Distribution")
                    top_10_for_pie = categories_data.head(10).copy()
                    if len(categories_data) > 10:
                        others_revenue = categories_data.tail(len(categories_data) - 10)['total_revenue'].sum()
                        others_row = pd.DataFrame({
                            'category': ['Others'],
                            'total_revenue': [others_revenue]
                        })
                        top_10_for_pie = pd.concat([top_10_for_pie[['category', 'total_revenue']], others_row], ignore_index=True)
                    
                    pie_chart = create_pie_chart(
                        top_10_for_pie,
                        names_col='category',
                        values_col='total_revenue',
                        title='Revenue Share by Category',
                        height=500
                    )
                    st.plotly_chart(pie_chart, use_container_width=True)
                
                # Secondary visualizations
                col1, col2 = st.columns(2)
                
                with col1:
                    # Items sold vs revenue scatter plot
                    st.subheader("Items Sold vs Revenue")
                    scatter_fig = px.scatter(
                        categories_data,
                        x='items_sold',
                        y='total_revenue',
                        size='unique_products',
                        hover_name='category',
                        title='Category Performance Matrix',
                        labels={
                            'items_sold': 'Items Sold',
                            'total_revenue': 'Total Revenue ($)',
                            'unique_products': 'Unique Products'
                        },
                        height=400
                    )
                    scatter_fig.update_layout(
                        title_x=0.5,
                        plot_bgcolor='white',
                        paper_bgcolor='white'
                    )
                    st.plotly_chart(scatter_fig, use_container_width=True)
                
                with col2:
                    # Average item value by category
                    st.subheader("Average Item Value by Category")
                    avg_value_chart = create_bar_chart(
                        categories_data.head(10),
                        x_col='category',
                        y_col='avg_item_value',
                        title='Average Item Value per Category',
                        x_title='Product Category',
                        y_title='Avg Item Value ($)',
                        height=400
                    )
                    avg_value_chart.update_xaxes(tickangle=45)
                    st.plotly_chart(avg_value_chart, use_container_width=True)
                
                # Detailed performance table
                st.subheader("Detailed Category Performance")
                
                # Format data for display
                display_data = categories_data.copy()
                display_data['total_revenue'] = display_data['total_revenue'].apply(lambda x: f"${x:,.2f}")
                display_data['avg_item_value'] = display_data['avg_item_value'].apply(lambda x: f"${x:.2f}")
                display_data['total_freight'] = display_data['total_freight'].apply(lambda x: f"${x:,.2f}")
                display_data['total_item_price'] = display_data['total_item_price'].apply(lambda x: f"${x:,.2f}")
                display_data['items_sold'] = display_data['items_sold'].apply(lambda x: f"{x:,}")
                display_data['unique_products'] = display_data['unique_products'].apply(lambda x: f"{x:,}")
                
                # Select and rename columns for display
                display_columns = {
                    'category': 'Category',
                    'total_revenue': 'Total Revenue',
                    'items_sold': 'Items Sold',
                    'unique_products': 'Unique Products',
                    'avg_item_value': 'Avg Item Value',
                    'total_item_price': 'Total Item Price',
                    'total_freight': 'Total Freight'
                }
                
                st.dataframe(
                    display_data[list(display_columns.keys())].rename(columns=display_columns),
                    use_container_width=True,
                    hide_index=True
                )
                
                # Download option
                csv = categories_data.to_csv(index=False)
                st.download_button(
                    label="📥 Download Category Performance Data",
                    data=csv,
                    file_name=f"top_categories_{selected_year if selected_year != 'All Years' else 'all_years'}_{selected_region if selected_region != 'All Regions' else 'all_regions'}.csv",
                    mime="text/csv"
                )
                
            else:
                st.warning("No category data available for the selected filters.")
                
        except Exception as e:
            st.error(f"Error loading product data: {str(e)}")
            st.info("Please check your BigQuery connection and data availability.")
    
    # Tab 3: Geographic Sales Distribution
    with tab3:
        st.header("Geographic Sales Distribution")
        st.markdown("Analyze sales patterns across Brazilian regions, states, and cities")
        
        try:
            # Display active filters
            if selected_year != "All Years":
                st.info(f"🎯 Active filter: Year: {selected_year}")
            
            # Get regional data
            regional_data = get_sales_by_region(year_filter=selected_year)
            state_data = get_sales_by_state(year_filter=selected_year, region_filter=selected_region)
            flow_data = get_customer_seller_flow(year_filter=selected_year)
            
            if not regional_data.empty:
                # Summary metrics by region
                customer_region_summary = regional_data.groupby('customer_region').agg({
                    'total_sales': 'sum',
                    'total_orders': 'sum',
                    'unique_customers': 'sum'
                }).reset_index()
                
                seller_region_summary = regional_data.groupby('seller_region').agg({
                    'total_sales': 'sum',
                    'total_orders': 'sum', 
                    'unique_sellers': 'sum'
                }).reset_index()
                
                # KPI metrics
                col1, col2, col3, col4 = st.columns(4)
                with col1:
                    st.metric(
                        "Total Regions",
                        len(customer_region_summary),
                        help="Number of Brazilian regions in analysis"
                    )
                with col2:
                    total_sales = regional_data['total_sales'].sum()
                    st.metric(
                        "Total Sales",
                        f"${total_sales:,.2f}",
                        help="Combined sales across all regions"
                    )
                with col3:
                    total_customers = customer_region_summary['unique_customers'].sum()
                    st.metric(
                        "Total Customers",
                        f"{total_customers:,}",
                        help="Unique customers across all regions"
                    )
                with col4:
                    total_sellers = seller_region_summary['unique_sellers'].sum()
                    st.metric(
                        "Total Sellers",
                        f"{total_sellers:,}",
                        help="Unique sellers across all regions"
                    )
                
                # Analysis insights
                col1, col2 = st.columns(2)
                
                with col1:
                    # Customer regions performance
                    st.subheader("Sales by Customer Region")
                    customer_chart = create_bar_chart(
                        customer_region_summary.sort_values('total_sales', ascending=False),
                        x_col='customer_region',
                        y_col='total_sales',
                        title='Sales Revenue by Customer Region',
                        x_title='Customer Region',
                        y_title='Total Sales ($)',
                        height=400
                    )
                    st.plotly_chart(customer_chart, use_container_width=True)
                
                with col2:
                    # Seller regions performance
                    st.subheader("Sales by Seller Region")
                    seller_chart = create_bar_chart(
                        seller_region_summary.sort_values('total_sales', ascending=False),
                        x_col='seller_region',
                        y_col='total_sales',
                        title='Sales Revenue by Seller Region',
                        x_title='Seller Region',
                        y_title='Total Sales ($)',
                        height=400
                    )
                    st.plotly_chart(seller_chart, use_container_width=True)
                
                # Customer-Seller Flow Analysis
                if not flow_data.empty:
                    st.subheader("Customer-Seller Transaction Flow")
                    
                    # Same vs Cross region analysis
                    flow_summary = flow_data.groupby('transaction_type').agg({
                        'total_sales': 'sum',
                        'total_orders': 'sum'
                    }).reset_index()
                    
                    col1, col2 = st.columns(2)
                    
                    with col1:
                        # Transaction type pie chart
                        pie_chart = create_pie_chart(
                            flow_summary,
                            names_col='transaction_type',
                            values_col='total_sales',
                            title='Sales: Same vs Cross-Region Transactions',
                            height=400
                        )
                        st.plotly_chart(pie_chart, use_container_width=True)
                    
                    with col2:
                        # Cross-region flow details
                        cross_region = flow_data[flow_data['transaction_type'] == 'Cross Region'].head(10)
                        if not cross_region.empty:
                            st.write("**Top Cross-Region Flows**")
                            cross_region_display = cross_region.copy()
                            cross_region_display['Flow'] = cross_region_display['customer_region'] + ' → ' + cross_region_display['seller_region']
                            cross_region_display['total_sales'] = cross_region_display['total_sales'].apply(lambda x: f"${x:,.2f}")
                            
                            st.dataframe(
                                cross_region_display[['Flow', 'total_sales', 'total_orders']].rename(columns={
                                    'Flow': 'Customer → Seller',
                                    'total_sales': 'Sales',
                                    'total_orders': 'Orders'
                                }),
                                use_container_width=True,
                                hide_index=True
                            )
                
                # Regional heatmap
                st.subheader("Customer-Seller Regional Flow")
                st.markdown("Heatmap showing sales flow between customer and seller regions")
                
                heatmap_chart = create_regional_heatmap(regional_data)
                st.plotly_chart(heatmap_chart, use_container_width=True)
                
                # State-level breakdown
                if not state_data.empty:
                    st.subheader("State-Level Performance Breakdown")
                    
                    # Top states by sales
                    col1, col2 = st.columns([2, 1])
                    
                    with col1:
                        top_states_chart = create_bar_chart(
                            state_data.head(15),
                            x_col='customer_state',
                            y_col='total_sales',
                            title='Top 15 States by Sales Revenue',
                            x_title='State',
                            y_title='Total Sales ($)',
                            height=450
                        )
                        top_states_chart.update_xaxes(tickangle=45)
                        st.plotly_chart(top_states_chart, use_container_width=True)
                    
                    with col2:
                        # Regional distribution of states
                        region_state_count = state_data.groupby('customer_region').size().reset_index(name='state_count')
                        region_pie = create_pie_chart(
                            region_state_count,
                            names_col='customer_region',
                            values_col='state_count',
                            title='States by Region',
                            height=450
                        )
                        st.plotly_chart(region_pie, use_container_width=True)
                    
                    # Detailed state data table
                    if selected_region != "All Regions":
                        st.write(f"**Detailed breakdown for {selected_region} region:**")
                    else:
                        st.write("**Detailed state performance data:**")
                    
                    # Format state data for display
                    display_state_data = state_data.copy()
                    display_state_data['total_sales'] = display_state_data['total_sales'].apply(lambda x: f"${x:,.2f}")
                    display_state_data['avg_order_value'] = display_state_data['avg_order_value'].apply(lambda x: f"${x:.2f}")
                    display_state_data['total_orders'] = display_state_data['total_orders'].apply(lambda x: f"{x:,}")
                    display_state_data['total_items'] = display_state_data['total_items'].apply(lambda x: f"{x:,}")
                    display_state_data['unique_customers'] = display_state_data['unique_customers'].apply(lambda x: f"{x:,}")
                    
                    display_columns = {
                        'customer_state': 'State',
                        'customer_region': 'Region',
                        'total_sales': 'Total Sales',
                        'total_orders': 'Total Orders',
                        'total_items': 'Total Items',
                        'unique_customers': 'Customers',
                        'avg_order_value': 'Avg Order Value'
                    }
                    
                    st.dataframe(
                        display_state_data[list(display_columns.keys())].rename(columns=display_columns),
                        use_container_width=True,
                        hide_index=True
                    )
                
                # Download options
                col1, col2 = st.columns(2)
                with col1:
                    regional_csv = regional_data.to_csv(index=False)
                    st.download_button(
                        label="📥 Download Regional Data",
                        data=regional_csv,
                        file_name=f"regional_sales_{selected_year if selected_year != 'All Years' else 'all_years'}.csv",
                        mime="text/csv"
                    )
                
                with col2:
                    if not state_data.empty:
                        state_csv = state_data.to_csv(index=False)
                        st.download_button(
                            label="📥 Download State Data", 
                            data=state_csv,
                            file_name=f"state_sales_{selected_year if selected_year != 'All Years' else 'all_years'}_{selected_region if selected_region != 'All Regions' else 'all_regions'}.csv",
                            mime="text/csv"
                        )
                
            else:
                st.warning("No geographic data available for the selected filters.")
            
        except Exception as e:
            st.error(f"Error loading geographic data: {str(e)}")
            st.info("Please check your BigQuery connection and data availability.")
    
    # Tab 4: Customer Purchase Behavior
    with tab4:
        st.header("Customer Purchase Behavior")
        st.markdown("Understand customer purchasing patterns, frequency, and lifetime value")
        
        try:
            # Display active filters
            if selected_year != "All Years" or selected_region != "All Regions":
                filter_text = []
                if selected_year != "All Years":
                    filter_text.append(f"Year: {selected_year}")
                if selected_region != "All Regions":
                    filter_text.append(f"Region: {selected_region}")
                st.info(f"🎯 Active filters: {' | '.join(filter_text)}")
            
            # Get customer behavior data
            behavior_data = get_customer_behavior(year_filter=selected_year, region_filter=selected_region)
            segmentation_data = get_customer_segmentation(year_filter=selected_year, region_filter=selected_region)
            frequency_data = get_customer_frequency_analysis(year_filter=selected_year, region_filter=selected_region)
            
            if not behavior_data.empty:
                # Summary KPI metrics
                total_customers = behavior_data['customer_count'].sum()
                total_one_time = behavior_data['one_time_customers'].sum()
                total_repeat = behavior_data['repeat_customers'].sum()
                avg_lifetime_value = behavior_data['avg_customer_lifetime_value'].mean()
                repeat_rate = (total_repeat / total_customers * 100) if total_customers > 0 else 0
                
                # Display KPIs
                col1, col2, col3, col4 = st.columns(4)
                with col1:
                    st.metric("Total Customers", f"{total_customers:,}", help="Total unique customers")
                with col2:
                    st.metric("Avg Lifetime Value", f"${avg_lifetime_value:.2f}", help="Average customer lifetime value")
                with col3:
                    st.metric("Repeat Rate", f"{repeat_rate:.1f}%", help="Percentage of repeat customers")
                with col4:
                    st.metric("One-Time Customers", f"{total_one_time:,}", help="Single purchase customers")
                
                st.markdown("---")
                
                # Regional behavior analysis
                col1, col2 = st.columns(2)
                with col1:
                    st.subheader("Lifetime Value by Region")
                    ltv_chart = create_bar_chart(
                        behavior_data.sort_values('avg_customer_lifetime_value', ascending=False),
                        x_col='customer_region', y_col='avg_customer_lifetime_value',
                        title='Average Customer Lifetime Value by Region',
                        x_title='Region', y_title='Avg Lifetime Value ($)', height=400
                    )
                    st.plotly_chart(ltv_chart, use_container_width=True)
                
                with col2:
                    st.subheader("Customer Distribution by Behavior")
                    if not segmentation_data.empty:
                        behavior_segments = segmentation_data.groupby('customer_segment')['customer_count'].sum().reset_index()
                        seg_pie = create_customer_behavior_pie_chart(behavior_segments, 'customer_segment', 'customer_count', 
                                                 'Customer Distribution by Behavior', height=400)
                        st.plotly_chart(seg_pie, use_container_width=True)
                    else:
                        st.info("No segmentation data available")
                
                # Customer Retention and Value Analysis
                col1, col2 = st.columns(2)
                with col1:
                    st.subheader("Customer Retention by Region")
                    behavior_display = behavior_data.copy()
                    
                    # Calculate retention rate
                    behavior_display['retention_rate'] = (behavior_display['repeat_customers'] / behavior_display['customer_count'] * 100)
                    
                    retention_chart = create_bar_chart(
                        behavior_display.sort_values('retention_rate', ascending=False),
                        x_col='customer_region', y_col='retention_rate',
                        title='Customer Retention Rate by Region',
                        x_title='Region', y_title='Retention Rate (%)', height=400
                    )
                    st.plotly_chart(retention_chart, use_container_width=True)
                
                with col2:
                    st.subheader("Revenue by Customer Value Segment")
                    if not segmentation_data.empty:
                        value_segments = segmentation_data.groupby('value_segment')['segment_total_value'].sum().reset_index()
                        val_pie = create_customer_value_pie_chart(value_segments, 'value_segment', 'segment_total_value',
                                                 'Revenue by Customer Value Segment', height=400)
                        st.plotly_chart(val_pie, use_container_width=True)
                    else:
                        st.info("No segmentation data available")
                
                # Purchase Frequency Analysis
                if not frequency_data.empty:
                    st.subheader("Purchase Frequency Distribution")
                    
                    # Ensure order_count is treated as categorical
                    frequency_display = frequency_data.copy()
                    frequency_display['order_count'] = frequency_display['order_count'].astype(str)
                    
                    freq_chart = create_bar_chart(
                        frequency_display.head(10), 'order_count', 'customer_count',
                        'Customer Count by Purchase Frequency', 'Number of Orders', 'Customer Count', height=400
                    )
                    
                    # Force x-axis to be categorical
                    freq_chart.update_xaxes(type='category')
                    
                    st.plotly_chart(freq_chart, use_container_width=True)
                    
                    # Interactive Purchase Frequency Details Table
                    st.markdown("**Purchase Frequency Details**")
                    
                    # Get available order counts and find the one with highest customer count
                    available_orders = frequency_data['order_count'].tolist()
                    default_order = frequency_data.loc[frequency_data['customer_count'].idxmax(), 'order_count']
                    
                    # Create dropdown selector
                    col1, col2 = st.columns([2, 3])
                    with col1:
                        selected_order_count = st.selectbox(
                            "Select Number of Orders to View Details:",
                            options=available_orders,
                            index=available_orders.index(default_order),
                            help="Choose an order count to see detailed customer information"
                        )
                    
                    with col2:
                        st.markdown(f"**Showing details for customers with {selected_order_count} order{'s' if selected_order_count != 1 else ''}**")
                    
                    # Filter data for selected order count
                    selected_data = frequency_data[frequency_data['order_count'] == selected_order_count]
                    
                    # Display the table
                    if not selected_data.empty:
                        display_freq_data = selected_data.copy()
                        display_freq_data['customer_count'] = display_freq_data['customer_count'].apply(lambda x: f"{x:,}")
                        display_freq_data['avg_customer_value'] = display_freq_data['avg_customer_value'].apply(lambda x: f"${x:.2f}")
                        display_freq_data['total_segment_value'] = display_freq_data['total_segment_value'].apply(lambda x: f"${x:,.2f}")
                        display_freq_data['percentage_of_customers'] = display_freq_data['percentage_of_customers'].apply(lambda x: f"{x}%")
                        
                        # Rename columns for display
                        display_columns = {
                            'order_count': 'Number of Orders',
                            'customer_count': 'Customer Count', 
                            'percentage_of_customers': 'Percentage',
                            'avg_customer_value': 'Avg Customer Value',
                            'total_segment_value': 'Total Segment Value'
                        }
                        
                        st.dataframe(
                            display_freq_data[list(display_columns.keys())].rename(columns=display_columns),
                            use_container_width=True,
                            hide_index=True
                        )
                    else:
                        st.info(f"No data available for {selected_order_count} orders")
                
                # Data tables
                st.subheader("Regional Behavior Summary")
                display_data = behavior_data.copy()
                display_data['avg_customer_lifetime_value'] = display_data['avg_customer_lifetime_value'].apply(lambda x: f"${x:.2f}")
                display_data['avg_order_value'] = display_data['avg_order_value'].apply(lambda x: f"${x:.2f}")
                display_data['customer_count'] = display_data['customer_count'].apply(lambda x: f"{x:,}")
                
                st.dataframe(display_data[['customer_region', 'customer_count', 'avg_orders_per_customer', 
                                         'avg_customer_lifetime_value', 'avg_order_value']].rename(columns={
                    'customer_region': 'Region', 'customer_count': 'Customers', 
                    'avg_orders_per_customer': 'Avg Orders', 'avg_customer_lifetime_value': 'Avg LTV',
                    'avg_order_value': 'Avg Order Value'
                }), use_container_width=True, hide_index=True)
                
                # Download option
                csv = behavior_data.to_csv(index=False)
                st.download_button("📥 Download Customer Behavior Data", data=csv,
                                 file_name=f"customer_behavior_{selected_year if selected_year != 'All Years' else 'all_years'}.csv",
                                 mime="text/csv")
                
            else:
                st.warning("No customer behavior data available for the selected filters.")
            
        except Exception as e:
            st.error(f"Error loading customer data: {str(e)}")
            st.info("Please check your BigQuery connection and data availability.")
    
    # Tab 5: Payment Method Impact
    with tab5:
        st.header("Payment Method Impact")
        st.markdown("Analyze how payment methods affect sales volumes and order values")
        
        try:
            # Display active filters
            if selected_year != "All Years" or selected_region != "All Regions":
                filter_text = []
                if selected_year != "All Years":
                    filter_text.append(f"Year: {selected_year}")
                if selected_region != "All Regions":
                    filter_text.append(f"Region: {selected_region}")
                st.info(f"🎯 Active filters: {' | '.join(filter_text)}")
            
            # Get payment data
            payment_data = get_payment_analysis(year_filter=selected_year, region_filter=selected_region)
            installment_data = get_installment_analysis(year_filter=selected_year, region_filter=selected_region)
            
            if not payment_data.empty:
                # Summary KPI metrics
                total_orders = payment_data['total_orders'].sum()
                total_sales = payment_data['total_sales'].sum()
                total_payments = payment_data['total_payments'].sum()
                avg_installments = payment_data['avg_installments'].mean()
                
                # Display KPIs
                col1, col2, col3, col4 = st.columns(4)
                with col1:
                    st.metric("Total Orders", f"{total_orders:,}", help="Total orders analyzed")
                with col2:
                    st.metric("Total Sales", f"${total_sales:,.2f}", help="Total sales value")
                with col3:
                    st.metric("Total Payments", f"${total_payments:,.2f}", help="Total payment value")
                with col4:
                    st.metric("Avg Installments", f"{avg_installments:.1f}", help="Average installments per order")
                
                st.markdown("---")
                
                # Payment method analysis charts
                st.subheader("Payment Method Performance")
                payment_chart = create_payment_method_chart(payment_data)
                st.plotly_chart(payment_chart, use_container_width=True)
                
                # Payment method comparison
                col1, col2 = st.columns(2)
                
                with col1:
                    st.subheader("Order Volume by Payment Type")
                    orders_chart = create_bar_chart(
                        payment_data.sort_values('total_orders', ascending=False),
                        x_col='primary_payment_type', y_col='total_orders',
                        title='Orders by Payment Method',
                        x_title='Payment Method', y_title='Number of Orders', height=400
                    )
                    orders_chart.update_xaxes(tickangle=45)
                    st.plotly_chart(orders_chart, use_container_width=True)
                
                with col2:
                    st.subheader("Average Order Value by Payment Type")
                    aov_chart = create_bar_chart(
                        payment_data.sort_values('avg_order_value', ascending=False),
                        x_col='primary_payment_type', y_col='avg_order_value',
                        title='Avg Order Value by Payment Method',
                        x_title='Payment Method', y_title='Avg Order Value ($)', height=400
                    )
                    aov_chart.update_xaxes(tickangle=45)
                    st.plotly_chart(aov_chart, use_container_width=True)
                               
                # Installment analysis
                if not installment_data.empty:
                    st.subheader("Installment Usage Analysis")
                    
                    col1, col2 = st.columns(2)
                    
                    with col1:
                        st.write("**Order Distribution by Installments**")
                        installment_chart = create_bar_chart(
                            installment_data.head(12),  # Show up to 12 installments
                            x_col='total_installments', y_col='order_count',
                            title='Orders by Number of Installments',
                            x_title='Number of Installments', y_title='Order Count', height=400
                        )
                        st.plotly_chart(installment_chart, use_container_width=True)
                    
                    with col2:
                        st.write("**Average Order Value by Installments**")
                        installment_value_chart = create_bar_chart(
                            installment_data.head(12),
                            x_col='total_installments', y_col='avg_order_value',
                            title='Avg Order Value by Installments',
                            x_title='Number of Installments', y_title='Avg Order Value ($)', height=400
                        )
                        st.plotly_chart(installment_value_chart, use_container_width=True)
                
                # Detailed payment data table
                st.subheader("Payment Method Performance Summary")
                display_data = payment_data.copy()
                display_data['total_sales'] = display_data['total_sales'].apply(lambda x: f"${x:,.2f}")
                display_data['avg_order_value'] = display_data['avg_order_value'].apply(lambda x: f"${x:.2f}")
                display_data['total_payments'] = display_data['total_payments'].apply(lambda x: f"${x:,.2f}")
                display_data['total_orders'] = display_data['total_orders'].apply(lambda x: f"{x:,}")
                
                payment_columns = {
                    'primary_payment_type': 'Payment Method',
                    'total_orders': 'Total Orders',
                    'total_sales': 'Total Sales',
                    'avg_order_value': 'Avg Order Value',
                    'avg_installments': 'Avg Installments',
                    'total_payments': 'Total Payments'
                }
                
                st.dataframe(
                    display_data[list(payment_columns.keys())].rename(columns=payment_columns),
                    use_container_width=True, hide_index=True
                )
                
                # Installment summary table
                if not installment_data.empty:
                    st.subheader("Installment Usage Summary")
                    installment_display = installment_data.head(10).copy()
                    installment_display['total_sales'] = installment_display['total_sales'].apply(lambda x: f"${x:,.2f}")
                    installment_display['avg_order_value'] = installment_display['avg_order_value'].apply(lambda x: f"${x:.2f}")
                    installment_display['order_count'] = installment_display['order_count'].apply(lambda x: f"{x:,}")
                    
                    installment_columns = {
                        'total_installments': 'Installments',
                        'order_count': 'Order Count',
                        'total_sales': 'Total Sales',
                        'avg_order_value': 'Avg Order Value',
                        'percentage_of_orders': 'Percentage of Orders'
                    }
                    
                    st.dataframe(
                        installment_display[list(installment_columns.keys())].rename(columns=installment_columns),
                        use_container_width=True, hide_index=True
                    )
                
                # Download options
                col1, col2 = st.columns(2)
                with col1:
                    payment_csv = payment_data.to_csv(index=False)
                    st.download_button(
                        "📥 Download Payment Analysis", data=payment_csv,
                        file_name=f"payment_analysis_{selected_year if selected_year != 'All Years' else 'all_years'}.csv",
                        mime="text/csv"
                    )
                
                with col2:
                    if not installment_data.empty:
                        installment_csv = installment_data.to_csv(index=False)
                        st.download_button(
                            "📥 Download Installment Analysis", data=installment_csv,
                            file_name=f"installment_analysis_{selected_year if selected_year != 'All Years' else 'all_years'}.csv",
                            mime="text/csv"
                        )
                
            else:
                st.warning("No payment data available for the selected filters.")
            
        except Exception as e:
            st.error(f"Error loading payment data: {str(e)}")
            st.info("Please check your BigQuery connection and data availability.")
    
    # Tab 6: Seller Performance Analysis
    with tab6:
        st.header("Seller Performance Analysis")
        st.markdown("Evaluate seller performance by region and revenue generation")
        
        try:
            # Display active filters
            if selected_year != "All Years" or selected_region != "All Regions":
                filter_text = []
                if selected_year != "All Years":
                    filter_text.append(f"Year: {selected_year}")
                if selected_region != "All Regions":
                    filter_text.append(f"Region: {selected_region}")
                st.info(f"🎯 Active filters: {' | '.join(filter_text)}")
            
            # Controls for this tab
            col1, col2 = st.columns([3, 1])
            with col2:
                top_n = st.selectbox("Top N Sellers", options=[10, 15, 20, 25, 30], index=2, help="Number of top sellers to display")
            
            # Get seller data
            seller_data = get_seller_performance(year_filter=selected_year, region_filter=selected_region)
            top_sellers_data = get_top_sellers(limit=top_n, year_filter=selected_year, region_filter=selected_region)
            diversity_data = get_seller_product_diversity(year_filter=selected_year, region_filter=selected_region)
            
            if not seller_data.empty:
                # Summary KPI metrics
                total_sellers = seller_data['unique_sellers'].sum()
                total_revenue = seller_data['total_revenue'].sum()
                avg_revenue_per_seller = total_revenue / total_sellers if total_sellers > 0 else 0
                total_products = seller_data['unique_products_sold'].sum()
                
                # Display KPIs
                col1, col2, col3, col4 = st.columns(4)
                with col1:
                    st.metric("Total Sellers", f"{total_sellers:,}", help="Total unique sellers")
                with col2:
                    st.metric("Total Revenue", f"${total_revenue:,.2f}", help="Combined seller revenue")
                with col3:
                    st.metric("Avg Revenue/Seller", f"${avg_revenue_per_seller:,.2f}", help="Average revenue per seller")
                with col4:
                    st.metric("Total Products", f"{total_products:,}", help="Total unique products sold")
                
                st.markdown("---")
                
                # Regional seller performance
                col1, col2 = st.columns(2)
                
                with col1:
                    st.subheader("Revenue by Seller Region")
                    revenue_chart = create_bar_chart(
                        seller_data.sort_values('total_revenue', ascending=False),
                        x_col='seller_region', y_col='total_revenue',
                        title='Total Revenue by Seller Region',
                        x_title='Seller Region', y_title='Total Revenue ($)', height=400
                    )
                    st.plotly_chart(revenue_chart, use_container_width=True)
                
                with col2:
                    st.subheader("Revenue per Seller by Region")
                    efficiency_chart = create_bar_chart(
                        seller_data.sort_values('revenue_per_seller', ascending=False),
                        x_col='seller_region', y_col='revenue_per_seller',
                        title='Average Revenue per Seller by Region',
                        x_title='Seller Region', y_title='Revenue per Seller ($)', height=400
                    )
                    st.plotly_chart(efficiency_chart, use_container_width=True)
                
                # Seller distribution analysis
                col1, col2 = st.columns(2)
                
                with col1:
                    st.subheader("Seller Distribution")
                    seller_pie = create_pie_chart(
                        seller_data, 'seller_region', 'unique_sellers',
                        'Seller Count by Region', height=400
                    )
                    st.plotly_chart(seller_pie, use_container_width=True)
                
                with col2:
                    st.subheader("Product Diversity by Region")
                    products_chart = create_bar_chart(
                        seller_data.sort_values('unique_products_sold', ascending=False),
                        x_col='seller_region', y_col='unique_products_sold',
                        title='Unique Products Sold by Region',
                        x_title='Seller Region', y_title='Unique Products', height=400
                    )
                    st.plotly_chart(products_chart, use_container_width=True)
                
                # Top sellers analysis
                if not top_sellers_data.empty:
                    st.subheader(f"Top {top_n} Sellers Performance")
                    
                    col1, col2 = st.columns(2)
                    
                    with col1:
                        st.write("**Revenue Leaders**")
                        top_revenue_chart = create_bar_chart(
                            top_sellers_data.head(15), 'seller_key', 'total_revenue',
                            'Top Sellers by Revenue', 'Seller ID', 'Revenue ($)', height=400
                        )
                        top_revenue_chart.update_xaxes(tickangle=45)
                        st.plotly_chart(top_revenue_chart, use_container_width=True)
                    
                    with col2:
                        st.write("**Regional Distribution of Top Sellers**")
                        top_sellers_region = top_sellers_data.groupby('seller_region').size().reset_index(name='seller_count')
                        top_region_pie = create_pie_chart(
                            top_sellers_region, 'seller_region', 'seller_count',
                            'Top Sellers by Region', height=400
                        )
                        st.plotly_chart(top_region_pie, use_container_width=True)
                
                # Seller diversity analysis
                if not diversity_data.empty:
                    st.subheader("Seller Product Diversity Analysis")
                    
                    col1, col2 = st.columns(2)
                    
                    with col1:
                        st.write("**Average Categories per Seller**")
                        diversity_chart = create_bar_chart(
                            diversity_data.sort_values('avg_categories_per_seller', ascending=False),
                            x_col='seller_region', y_col='avg_categories_per_seller',
                            title='Avg Product Categories per Seller',
                            x_title='Seller Region', y_title='Avg Categories', height=400
                        )
                        st.plotly_chart(diversity_chart, use_container_width=True)
                    
                    with col2:
                        st.write("**Seller Specialization Analysis**")
                        # Create specialization vs diversification chart
                        specialization_data = diversity_data.copy()
                        specialization_data['specialization_rate'] = (specialization_data['single_category_sellers'] / specialization_data['seller_count'] * 100)
                        
                        spec_chart = create_bar_chart(
                            specialization_data.sort_values('specialization_rate', ascending=False),
                            x_col='seller_region', y_col='specialization_rate',
                            title='Single-Category Seller Rate by Region',
                            x_title='Seller Region', y_title='Specialization Rate (%)', height=400
                        )
                        st.plotly_chart(spec_chart, use_container_width=True)
                
                # Performance correlation analysis
                st.subheader("Seller Performance Correlation")
                if not top_sellers_data.empty:
                    correlation_fig = px.scatter(
                        top_sellers_data, x='unique_products_sold', y='total_revenue',
                        size='total_orders', color='seller_region',
                        hover_name='seller_key',
                        title='Product Diversity vs Revenue (Top Sellers)',
                        labels={'unique_products_sold': 'Unique Products Sold', 'total_revenue': 'Total Revenue ($)', 'total_orders': 'Orders'},
                        height=500
                    )
                    correlation_fig.update_layout(title_x=0.5, plot_bgcolor='white', paper_bgcolor='white')
                    st.plotly_chart(correlation_fig, use_container_width=True)
                
                # Detailed data tables
                st.subheader("Regional Seller Performance Summary")
                display_data = seller_data.copy()
                display_data['total_revenue'] = display_data['total_revenue'].apply(lambda x: f"${x:,.2f}")
                display_data['revenue_per_seller'] = display_data['revenue_per_seller'].apply(lambda x: f"${x:,.2f}")
                display_data['avg_item_value'] = display_data['avg_item_value'].apply(lambda x: f"${x:.2f}")
                display_data['total_freight_revenue'] = display_data['total_freight_revenue'].apply(lambda x: f"${x:,.2f}")
                display_data['unique_sellers'] = display_data['unique_sellers'].apply(lambda x: f"{x:,}")
                display_data['total_orders'] = display_data['total_orders'].apply(lambda x: f"{x:,}")
                
                regional_columns = {
                    'seller_region': 'Region',
                    'unique_sellers': 'Sellers',
                    'total_revenue': 'Total Revenue',
                    'revenue_per_seller': 'Revenue/Seller',
                    'total_orders': 'Orders',
                    'unique_products_sold': 'Products',
                    'avg_item_value': 'Avg Item Value'
                }
                
                st.dataframe(
                    display_data[list(regional_columns.keys())].rename(columns=regional_columns),
                    use_container_width=True, hide_index=True
                )
                
                # Top sellers detailed table
                if not top_sellers_data.empty:
                    st.subheader(f"Top {min(10, len(top_sellers_data))} Sellers Details")
                    top_display = top_sellers_data.head(10).copy()
                    top_display['total_revenue'] = top_display['total_revenue'].apply(lambda x: f"${x:,.2f}")
                    top_display['avg_item_value'] = top_display['avg_item_value'].apply(lambda x: f"${x:.2f}")
                    top_display['total_orders'] = top_display['total_orders'].apply(lambda x: f"{x:,}")
                    top_display['unique_products_sold'] = top_display['unique_products_sold'].apply(lambda x: f"{x:,}")
                    
                    top_columns = {
                        'seller_key': 'Seller ID',
                        'seller_region': 'Region',
                        'seller_state': 'State',
                        'total_revenue': 'Revenue',
                        'total_orders': 'Orders',
                        'unique_products_sold': 'Products',
                        'avg_item_value': 'Avg Item Value'
                    }
                    
                    st.dataframe(
                        top_display[list(top_columns.keys())].rename(columns=top_columns),
                        use_container_width=True, hide_index=True
                    )
                
                # Download options
                col1, col2 = st.columns(2)
                with col1:
                    seller_csv = seller_data.to_csv(index=False)
                    st.download_button(
                        "📥 Download Regional Performance", data=seller_csv,
                        file_name=f"seller_performance_{selected_year if selected_year != 'All Years' else 'all_years'}.csv",
                        mime="text/csv"
                    )
                
                with col2:
                    if not top_sellers_data.empty:
                        top_csv = top_sellers_data.to_csv(index=False)
                        st.download_button(
                            "📥 Download Top Sellers", data=top_csv,
                            file_name=f"top_sellers_{selected_year if selected_year != 'All Years' else 'all_years'}.csv",
                            mime="text/csv"
                        )
                
            else:
                st.warning("No seller data available for the selected filters.")
            
        except Exception as e:
            st.error(f"Error loading seller data: {str(e)}")
            st.info("Please check your BigQuery connection and data availability.")
    
    # Tab 7: Reviews & Sales Correlation
    with tab7:
        st.header("Product Reviews & Sales Correlation")
        st.markdown("Analyze how customer reviews impact sales performance")
        
        try:
            # Display active filters
            if selected_year != "All Years" or selected_region != "All Regions":
                filter_text = []
                if selected_year != "All Years":
                    filter_text.append(f"Year: {selected_year}")
                if selected_region != "All Regions":
                    filter_text.append(f"Region: {selected_region}")
                st.info(f"🎯 Active filters: {' | '.join(filter_text)}")
            
            # Get review data
            correlation_data = get_reviews_sales_correlation(year_filter=selected_year, region_filter=selected_region)
            score_distribution = get_review_score_distribution(year_filter=selected_year, region_filter=selected_region)
            timing_data = get_review_timing_analysis(year_filter=selected_year, region_filter=selected_region)
            
            if not correlation_data.empty:
                # Summary KPI metrics
                total_items = correlation_data['total_items'].sum()
                total_sales = correlation_data['total_sales'].sum()
                total_reviews = correlation_data['reviews_count'].sum()
                no_reviews = correlation_data['no_review_count'].sum()
                review_coverage = (total_reviews / (total_reviews + no_reviews) * 100) if (total_reviews + no_reviews) > 0 else 0
                
                # Display KPIs
                col1, col2, col3, col4 = st.columns(4)
                with col1:
                    st.metric("Total Items", f"{total_items:,}", help="Total items analyzed")
                with col2:
                    st.metric("Total Sales", f"${total_sales:,.2f}", help="Total sales value")
                with col3:
                    st.metric("Review Coverage", f"{review_coverage:.1f}%", help="Percentage of orders with reviews")
                with col4:
                    st.metric("Total Reviews", f"{total_reviews:,}", help="Number of actual reviews")
                
                st.markdown("---")
                
                # Review impact analysis
                st.subheader("Review Impact on Sales Performance")
                col1, col2 = st.columns(2)
                
                with col1:
                    st.write("**Sales by Review Category**")
                    sales_chart = create_bar_chart(
                        correlation_data.sort_values('total_sales', ascending=False),
                        x_col='review_category', y_col='total_sales',
                        title='Total Sales by Review Category',
                        x_title='Review Category', y_title='Total Sales ($)', height=400
                    )
                    sales_chart.update_xaxes(tickangle=45)
                    st.plotly_chart(sales_chart, use_container_width=True)
                
                with col2:
                    st.write("**Average Item Value by Review Category**")
                    value_chart = create_bar_chart(
                        correlation_data.sort_values('avg_item_value', ascending=False),
                        x_col='review_category', y_col='avg_item_value',
                        title='Avg Item Value by Review Category',
                        x_title='Review Category', y_title='Avg Item Value ($)', height=400
                    )
                    value_chart.update_xaxes(tickangle=45)
                    st.plotly_chart(value_chart, use_container_width=True)
                
                # Review availability analysis
                col1, col2 = st.columns(2)
                
                with col1:
                    st.subheader("Review Availability Distribution")
                    # Create availability data
                    availability_data = pd.DataFrame({
                        'category': ['Has Reviews', 'No Reviews'],
                        'items': [total_reviews, no_reviews],
                        'sales': [
                            correlation_data[correlation_data['review_category'] != 'No Review']['total_sales'].sum(),
                            correlation_data[correlation_data['review_category'] == 'No Review']['total_sales'].sum()
                        ]
                    })
                    
                    availability_pie = create_pie_chart(
                        availability_data, 'category', 'items',
                        'Item Distribution by Review Availability', height=400
                    )
                    st.plotly_chart(availability_pie, use_container_width=True)
                
                with col2:
                    st.subheader("Sales Distribution by Review Availability")
                    sales_pie = create_pie_chart(
                        availability_data, 'category', 'sales',
                        'Sales Distribution by Review Availability', height=400
                    )
                    st.plotly_chart(sales_pie, use_container_width=True)
                
                # Detailed review score analysis
                if not score_distribution.empty:
                    st.subheader("Detailed Review Score Distribution")
                    
                    col1, col2 = st.columns(2)
                    
                    with col1:
                        st.write("**Item Count by Review Score**")
                        score_chart = create_bar_chart(
                            score_distribution, 'review_score', 'total_items',
                            'Items by Review Score (0 = No Review)',
                            'Review Score', 'Total Items', height=400
                        )
                        st.plotly_chart(score_chart, use_container_width=True)
                    
                    with col2:
                        st.write("**Sales Value by Review Score**")
                        score_sales_chart = create_bar_chart(
                            score_distribution, 'review_score', 'total_sales',
                            'Sales by Review Score (0 = No Review)',
                            'Review Score', 'Total Sales ($)', height=400
                        )
                        st.plotly_chart(score_sales_chart, use_container_width=True)
                
                # Review timing analysis
                if not timing_data.empty:
                    st.subheader("Review Timing Impact Analysis")
                    
                    col1, col2 = st.columns(2)
                    
                    with col1:
                        st.write("**Review Score by Timing**")
                        timing_score_chart = create_bar_chart(
                            timing_data, 'timing_category', 'avg_review_score',
                            'Average Review Score by Review Timing',
                            'Review Timing', 'Avg Review Score', height=400
                        )
                        timing_score_chart.update_xaxes(tickangle=45)
                        st.plotly_chart(timing_score_chart, use_container_width=True)
                    
                    with col2:
                        st.write("**Sales Value by Review Timing**")
                        timing_sales_chart = create_bar_chart(
                            timing_data, 'timing_category', 'total_sales',
                            'Total Sales by Review Timing',
                            'Review Timing', 'Total Sales ($)', height=400
                        )
                        timing_sales_chart.update_xaxes(tickangle=45)
                        st.plotly_chart(timing_sales_chart, use_container_width=True)
                
                # Review correlation insights
                st.subheader("Review-Sales Correlation Insights")
                
                # Create correlation matrix visualization
                if len(correlation_data) > 1:
                    correlation_fig = px.scatter(
                        correlation_data[correlation_data['review_category'] != 'No Review'], 
                        x='avg_review_score', y='avg_item_value',
                        size='total_items', color='review_category',
                        hover_name='review_category',
                        title='Review Score vs Item Value Correlation',
                        labels={'avg_review_score': 'Avg Review Score', 'avg_item_value': 'Avg Item Value ($)', 'total_items': 'Items'},
                        height=500
                    )
                    correlation_fig.update_layout(title_x=0.5, plot_bgcolor='white', paper_bgcolor='white')
                    st.plotly_chart(correlation_fig, use_container_width=True)
                
                # Detailed data tables
                st.subheader("Review Category Performance Summary")
                display_data = correlation_data.copy()
                display_data['total_sales'] = display_data['total_sales'].apply(lambda x: f"${x:,.2f}")
                display_data['avg_item_value'] = display_data['avg_item_value'].apply(lambda x: f"${x:.2f}")
                display_data['total_items'] = display_data['total_items'].apply(lambda x: f"{x:,}")
                display_data['reviews_count'] = display_data['reviews_count'].apply(lambda x: f"{x:,}")
                display_data['no_review_count'] = display_data['no_review_count'].apply(lambda x: f"{x:,}")
                
                review_columns = {
                    'review_category': 'Review Category',
                    'total_items': 'Total Items',
                    'total_sales': 'Total Sales',
                    'avg_item_value': 'Avg Item Value',
                    'avg_review_score': 'Avg Review Score',
                    'reviews_count': 'Reviews Count',
                    'no_review_count': 'No Review Count'
                }
                
                st.dataframe(
                    display_data[list(review_columns.keys())].rename(columns=review_columns),
                    use_container_width=True, hide_index=True
                )
                
                # Score distribution table
                if not score_distribution.empty:
                    st.subheader("Review Score Distribution Details")
                    score_display = score_distribution.copy()
                    score_display['total_sales'] = score_display['total_sales'].apply(lambda x: f"${x:,.2f}")
                    score_display['avg_item_value'] = score_display['avg_item_value'].apply(lambda x: f"${x:.2f}")
                    score_display['total_items'] = score_display['total_items'].apply(lambda x: f"{x:,}")
                    score_display['actual_reviews'] = score_display['actual_reviews'].apply(lambda x: f"{x:,}")
                    
                    score_columns = {
                        'review_score': 'Review Score',
                        'total_items': 'Total Items',
                        'total_sales': 'Total Sales',
                        'avg_item_value': 'Avg Item Value',
                        'actual_reviews': 'Actual Reviews',
                        'percentage_of_items': '% of Items'
                    }
                    
                    st.dataframe(
                        score_display[list(score_columns.keys())].rename(columns=score_columns),
                        use_container_width=True, hide_index=True
                    )
                
                # Download options
                col1, col2 = st.columns(2)
                with col1:
                    correlation_csv = correlation_data.to_csv(index=False)
                    st.download_button(
                        "📥 Download Review Correlation", data=correlation_csv,
                        file_name=f"review_correlation_{selected_year if selected_year != 'All Years' else 'all_years'}.csv",
                        mime="text/csv"
                    )
                
                with col2:
                    if not score_distribution.empty:
                        score_csv = score_distribution.to_csv(index=False)
                        st.download_button(
                            "📥 Download Score Distribution", data=score_csv,
                            file_name=f"review_scores_{selected_year if selected_year != 'All Years' else 'all_years'}.csv",
                            mime="text/csv"
                        )
                
            else:
                st.warning("No review data available for the selected filters.")
            
        except Exception as e:
            st.error(f"Error loading review data: {str(e)}")
            st.info("Please check your BigQuery connection and data availability.")
    
    # Tab 8: Delivery Time Patterns
    with tab8:
        st.header("Delivery Time Patterns")
        st.markdown("Examine delivery performance metrics across regions")
        
        try:
            # Display active filters
            if selected_year != "All Years" or selected_region != "All Regions":
                filter_text = []
                if selected_year != "All Years":
                    filter_text.append(f"Year: {selected_year}")
                if selected_region != "All Regions":
                    filter_text.append(f"Region: {selected_region}")
                st.info(f"🎯 Active filters: {' | '.join(filter_text)}")
            
            # Get delivery data
            delivery_data = get_delivery_patterns(year_filter=selected_year, region_filter=selected_region)
            distribution_data = get_delivery_time_distribution(year_filter=selected_year, region_filter=selected_region)
            efficiency_data = get_delivery_efficiency_analysis(year_filter=selected_year, region_filter=selected_region)
            
            if not delivery_data.empty:
                # Summary KPI metrics
                total_orders = delivery_data['total_orders'].sum()
                total_sales = delivery_data['total_sales'].sum()
                avg_delivery_days = (delivery_data['avg_delivery_days'] * delivery_data['total_orders']).sum() / total_orders if total_orders > 0 else 0
                overall_on_time_rate = (delivery_data['on_time_deliveries'].sum() / total_orders * 100) if total_orders > 0 else 0
                
                # Display KPIs
                col1, col2, col3, col4 = st.columns(4)
                with col1:
                    st.metric("Total Orders", f"{total_orders:,}", help="Total orders with delivery data")
                with col2:
                    st.metric("Total Sales", f"${total_sales:,.2f}", help="Total sales value")
                with col3:
                    st.metric("Avg Delivery Time", f"{avg_delivery_days:.1f} days", help="Average delivery time across regions")
                with col4:
                    st.metric("On-Time Rate", f"{overall_on_time_rate:.1f}%", help="Overall on-time delivery rate")
                
                st.markdown("---")
                
                # Regional delivery performance
                st.subheader("Regional Delivery Performance")
                col1, col2 = st.columns(2)
                
                with col1:
                    st.write("**Average Delivery Time by Region**")
                    delivery_chart = create_bar_chart(
                        delivery_data.sort_values('avg_delivery_days', ascending=True),
                        x_col='customer_region', y_col='avg_delivery_days',
                        title='Average Delivery Days by Region',
                        x_title='Customer Region', y_title='Average Days', height=400
                    )
                    st.plotly_chart(delivery_chart, use_container_width=True)
                
                with col2:
                    st.write("**On-Time Delivery Rate by Region**")
                    ontime_chart = create_bar_chart(
                        delivery_data.sort_values('on_time_delivery_rate', ascending=False),
                        x_col='customer_region', y_col='on_time_delivery_rate',
                        title='On-Time Delivery Rate by Region',
                        x_title='Customer Region', y_title='On-Time Rate (%)', height=400
                    )
                    st.plotly_chart(ontime_chart, use_container_width=True)
                
                # Delivery performance distribution
                col1, col2 = st.columns(2)
                
                with col1:
                    st.subheader("Order Volume Distribution")
                    volume_pie = create_pie_chart(
                        delivery_data, 'customer_region', 'total_orders',
                        'Order Volume by Region', height=400
                    )
                    st.plotly_chart(volume_pie, use_container_width=True)
                
                with col2:
                    st.subheader("Delivery vs Estimate Performance")
                    estimate_chart = create_bar_chart(
                        delivery_data.sort_values('avg_delivery_vs_estimate', ascending=True),
                        x_col='customer_region', y_col='avg_delivery_vs_estimate',
                        title='Delivery vs Estimate (Negative = Early)',
                        x_title='Customer Region', y_title='Days vs Estimate', height=400
                    )
                    st.plotly_chart(estimate_chart, use_container_width=True)
                
                # Delivery time distribution analysis
                if not distribution_data.empty:
                    st.subheader("Delivery Time Distribution Analysis")
                    
                    col1, col2 = st.columns(2)
                    
                    with col1:
                        st.write("**Order Count by Delivery Speed**")
                        speed_chart = create_bar_chart(
                            distribution_data, 'delivery_speed_category', 'total_orders',
                            'Orders by Delivery Speed Category',
                            'Delivery Speed', 'Total Orders', height=400
                        )
                        speed_chart.update_xaxes(tickangle=45)
                        st.plotly_chart(speed_chart, use_container_width=True)
                    
                    with col2:
                        st.write("**On-Time Rate by Delivery Speed**")
                        speed_ontime_chart = create_bar_chart(
                            distribution_data, 'delivery_speed_category', 'on_time_rate',
                            'On-Time Rate by Delivery Speed',
                            'Delivery Speed', 'On-Time Rate (%)', height=400
                        )
                        speed_ontime_chart.update_xaxes(tickangle=45)
                        st.plotly_chart(speed_ontime_chart, use_container_width=True)
                
                # Same vs cross-region delivery efficiency
                if not efficiency_data.empty:
                    st.subheader("Delivery Efficiency: Same vs Cross-Region")
                    
                    # Aggregate by delivery type
                    efficiency_summary = efficiency_data.groupby('delivery_type').agg({
                        'total_orders': 'sum',
                        'avg_delivery_days': lambda x: (x * efficiency_data.loc[x.index, 'total_orders']).sum() / efficiency_data.loc[x.index, 'total_orders'].sum(),
                        'on_time_rate': lambda x: (efficiency_data.loc[x.index, 'on_time_rate'] * efficiency_data.loc[x.index, 'total_orders']).sum() / efficiency_data.loc[x.index, 'total_orders'].sum(),
                        'total_sales': 'sum',
                        'avg_freight_cost': lambda x: (x * efficiency_data.loc[x.index, 'total_orders']).sum() / efficiency_data.loc[x.index, 'total_orders'].sum()
                    }).round(1).reset_index()
                    
                    col1, col2 = st.columns(2)
                    
                    with col1:
                        st.write("**Delivery Time: Same vs Cross-Region**")
                        efficiency_chart = create_bar_chart(
                            efficiency_summary, 'delivery_type', 'avg_delivery_days',
                            'Average Delivery Days by Type',
                            'Delivery Type', 'Avg Days', height=400
                        )
                        st.plotly_chart(efficiency_chart, use_container_width=True)
                    
                    with col2:
                        st.write("**Freight Cost: Same vs Cross-Region**")
                        freight_chart = create_bar_chart(
                            efficiency_summary, 'delivery_type', 'avg_freight_cost',
                            'Average Freight Cost by Type',
                            'Delivery Type', 'Avg Freight Cost ($)', height=400
                        )
                        st.plotly_chart(freight_chart, use_container_width=True)
                
                # Performance correlation analysis
                st.subheader("Delivery Performance Correlation")
                if len(delivery_data) > 1:
                    correlation_fig = px.scatter(
                        delivery_data, x='avg_delivery_days', y='on_time_delivery_rate',
                        size='total_orders', color='customer_region',
                        hover_name='customer_region',
                        title='Delivery Time vs On-Time Rate Correlation',
                        labels={'avg_delivery_days': 'Avg Delivery Days', 'on_time_delivery_rate': 'On-Time Rate (%)', 'total_orders': 'Orders'},
                        height=500
                    )
                    correlation_fig.update_layout(title_x=0.5, plot_bgcolor='white', paper_bgcolor='white')
                    st.plotly_chart(correlation_fig, use_container_width=True)
                
                # Detailed data tables
                st.subheader("Regional Delivery Performance Summary")
                display_data = delivery_data.copy()
                display_data['total_sales'] = display_data['total_sales'].apply(lambda x: f"${x:,.2f}")
                display_data['total_orders'] = display_data['total_orders'].apply(lambda x: f"{x:,}")
                display_data['on_time_deliveries'] = display_data['on_time_deliveries'].apply(lambda x: f"{x:,}")
                display_data['late_deliveries'] = display_data['late_deliveries'].apply(lambda x: f"{x:,}")
                
                delivery_columns = {
                    'customer_region': 'Region',
                    'total_orders': 'Total Orders',
                    'total_sales': 'Total Sales',
                    'avg_delivery_days': 'Avg Delivery Days',
                    'on_time_delivery_rate': 'On-Time Rate (%)',
                    'avg_delivery_vs_estimate': 'Vs Estimate (Days)',
                    'min_delivery_days': 'Min Days',
                    'max_delivery_days': 'Max Days'
                }
                
                st.dataframe(
                    display_data[list(delivery_columns.keys())].rename(columns=delivery_columns),
                    use_container_width=True, hide_index=True
                )
                
                # Speed distribution table
                if not distribution_data.empty:
                    st.subheader("Delivery Speed Distribution Details")
                    speed_display = distribution_data.copy()
                    speed_display['total_sales'] = speed_display['total_sales'].apply(lambda x: f"${x:,.2f}")
                    speed_display['total_orders'] = speed_display['total_orders'].apply(lambda x: f"{x:,}")
                    speed_display['on_time_orders'] = speed_display['on_time_orders'].apply(lambda x: f"{x:,}")
                    
                    speed_columns = {
                        'delivery_speed_category': 'Speed Category',
                        'total_orders': 'Total Orders',
                        'total_sales': 'Total Sales',
                        'avg_days_in_category': 'Avg Days',
                        'on_time_rate': 'On-Time Rate (%)',
                        'avg_vs_estimate': 'Vs Estimate (Days)'
                    }
                    
                    st.dataframe(
                        speed_display[list(speed_columns.keys())].rename(columns=speed_columns),
                        use_container_width=True, hide_index=True
                    )
                
                # Download options
                col1, col2 = st.columns(2)
                with col1:
                    delivery_csv = delivery_data.to_csv(index=False)
                    st.download_button(
                        "📥 Download Regional Performance", data=delivery_csv,
                        file_name=f"delivery_performance_{selected_year if selected_year != 'All Years' else 'all_years'}.csv",
                        mime="text/csv"
                    )
                
                with col2:
                    if not distribution_data.empty:
                        distribution_csv = distribution_data.to_csv(index=False)
                        st.download_button(
                            "📥 Download Speed Distribution", data=distribution_csv,
                            file_name=f"delivery_distribution_{selected_year if selected_year != 'All Years' else 'all_years'}.csv",
                            mime="text/csv"
                        )
                
            else:
                st.warning("No delivery data available for the selected filters.")
                
        except Exception as e:
            st.error(f"Error loading delivery data: {str(e)}")
            st.info("Please check your BigQuery connection and data availability.")
    
    # Footer
    st.markdown("---")
    st.markdown(
        "<div style='text-align: center; color: #666;'>"
        "Olist E-commerce Analytics Dashboard | Built with Streamlit & BigQuery"
        "</div>",
        unsafe_allow_html=True
    )

if __name__ == "__main__":
    main()





