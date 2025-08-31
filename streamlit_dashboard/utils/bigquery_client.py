"""
BigQuery Client Utility for Olist Analytics Dashboard
Handles authentication, connection management, and query execution
"""

import streamlit as st
from google.cloud import bigquery
from google.oauth2 import service_account
import pandas as pd
import logging
from typing import Optional, Dict, Any
import os
from pathlib import Path
from dotenv import load_dotenv

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# --- Load .env (prefer .env, fallback to .env.sample / env_sample) ---
root_dir = Path(__file__).resolve().parents[2]  # go up 2 levels
env_candidates = [root_dir / ".env", root_dir / ".env.sample", root_dir / "env_sample"]

for p in env_candidates:
    if p.exists():
        load_dotenv(dotenv_path=p)
        logger.info("Loaded environment variables from %s", p)
        break
else:
    logger.warning("No .env file found; relying on existing environment variables")

PROJECT_ID = os.getenv("PROJECT_ID")  # or your project var
# --- Resolve marts dataset name robustly ---
RAW_NAME = os.getenv("MARTS_DATASET_NAME", "").strip()

def ensure_marts_suffix(name: str) -> str:
    """Append '_marts' only if not already present; raise if empty."""
    if not name:
        raise EnvironmentError("MARTS_DATASET_NAME is not set (or empty).")
    return name if name.endswith("_marts") else f"{name}_marts"

try:
    marts_dataset = ensure_marts_suffix(RAW_NAME)
except EnvironmentError as e:
    # You can choose to default or hard-fail; here we hard-fail loudly.
    logger.error(str(e))
    raise

@st.cache_resource
def init_connection() -> bigquery.Client:
    """
    Initialize and cache BigQuery client connection using service account
    
    Returns:
        bigquery.Client: Authenticated BigQuery client
        
    Raises:
        Exception: If authentication fails or client cannot be created
    """
    try:
        # Get service account credentials from Streamlit secrets
        service_account_info = st.secrets["gcp_service_account"]
        
        # Create credentials object
        credentials = service_account.Credentials.from_service_account_info(
            service_account_info,
            scopes=["https://www.googleapis.com/auth/cloud-platform"]
        )
        
        # Create BigQuery client
        client = bigquery.Client(
            credentials=credentials,
            project=service_account_info.get("project_id")
        )
        
        logger.info("BigQuery client initialized successfully")
        return client
        
    except KeyError as e:
        error_msg = f"Missing required secret: {e}. Please check your .streamlit/secrets.toml file."
        logger.error(error_msg)
        st.error(error_msg)
        raise Exception(error_msg)
        
    except Exception as e:
        error_msg = f"Failed to initialize BigQuery client: {str(e)}"
        logger.error(error_msg)
        st.error(error_msg)
        raise Exception(error_msg)

def execute_query(query: str, client: Optional[bigquery.Client] = None) -> pd.DataFrame:
    """
    Execute a BigQuery SQL query and return results as pandas DataFrame
    
    Args:
        query (str): SQL query to execute
        client (bigquery.Client, optional): BigQuery client. If None, will initialize new one.
        
    Returns:
        pd.DataFrame: Query results
        
    Raises:
        Exception: If query execution fails
    """
    try:
        # Initialize client if not provided
        if client is None:
            client = init_connection()
        
        # Execute query
        logger.info(f"Executing query: {query[:100]}...")
        query_job = client.query(query)
        
        # Wait for completion and get results
        results = query_job.result()
        
        # Convert to pandas DataFrame
        df = results.to_dataframe()
        
        logger.info(f"Query executed successfully. Returned {len(df)} rows.")
        return df
        
    except Exception as e:
        error_msg = f"Query execution failed: {str(e)}"
        logger.error(error_msg)
        st.error(error_msg)
        raise Exception(error_msg)

def test_connection() -> bool:
    """
    Test BigQuery connection by executing a simple query
    
    Returns:
        bool: True if connection successful, False otherwise
    """
    try:
        client = init_connection()
        
        # Simple test query
        test_query = f"""
        SELECT 
            COUNT(*) as table_count
        FROM `{PROJECT_ID}.{marts_dataset}.__TABLES__`
        WHERE table_id LIKE 'dim_%' OR table_id LIKE 'fact_%'
        """
        
        result = execute_query(test_query, client)
        
        if len(result) > 0 and result.iloc[0]['table_count'] > 0:
            st.success("✅ BigQuery connection successful! Found marts tables.")
            return True
        else:
            st.warning("⚠️ Connection successful but no marts tables found.")
            return False
            
    except Exception as e:
        st.error(f"❌ BigQuery connection failed: {str(e)}")
        return False

def get_table_info(dataset_id: str = f"{marts_dataset}") -> pd.DataFrame:
    """
    Get information about tables in the specified dataset
    
    Args:
        dataset_id (str): BigQuery dataset ID
        
    Returns:
        pd.DataFrame: Table information including name, row count, and size
    """
    try:
        client = init_connection()
        
        query = f"""
        SELECT 
            table_id,
            row_count,
            size_bytes,
            TIMESTAMP_MILLIS(creation_time) as created,
            TIMESTAMP_MILLIS(last_modified_time) as last_modified
        FROM `{PROJECT_ID}.{dataset_id}.__TABLES__`
        ORDER BY table_id
        """
        
        return execute_query(query, client)
        
    except Exception as e:
        logger.error(f"Failed to get table info: {str(e)}")
        return pd.DataFrame()

def get_sample_data(table_name: str, limit: int = 5) -> pd.DataFrame:
    """
    Get sample data from a specific table
    
    Args:
        table_name (str): Name of the table to sample
        limit (int): Number of rows to return
        
    Returns:
        pd.DataFrame: Sample data from the table
    """
    try:
        client = init_connection()
        
        query = f"""
        SELECT *
        FROM `{PROJECT_ID}.{marts_dataset}.{table_name}`
        LIMIT {limit}
        """
        
        return execute_query(query, client)
        
    except Exception as e:
        logger.error(f"Failed to get sample data from {table_name}: {str(e)}")
        return pd.DataFrame()

def validate_table_exists(table_name: str, dataset_id: str = marts_dataset) -> bool:
    """
    Check if a table exists in the specified dataset
    
    Args:
        table_name (str): Name of the table to check
        dataset_id (str): BigQuery dataset ID
        
    Returns:
        bool: True if table exists, False otherwise
    """
    try:
        client = init_connection()
        
        query = f"""
        SELECT COUNT(*) as table_count
        FROM `{PROJECT_ID}.{dataset_id}.__TABLES__`
        WHERE table_id = '{table_name}'
        """
        
        result = execute_query(query, client)
        return result.iloc[0]['table_count'] > 0
        
    except Exception as e:
        logger.error(f"Failed to validate table {table_name}: {str(e)}")
        return False