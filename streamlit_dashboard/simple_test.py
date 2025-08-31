#!/usr/bin/env python3
"""
Simple BigQuery connection test without Streamlit context
"""

import json
from google.cloud import bigquery
from google.oauth2 import service_account
import os
from pathlib import Path
from dotenv import load_dotenv
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# --- Load .env (prefer .env, fallback to .env.sample / env_sample) ---
root_dir = Path(__file__).resolve().parents[1]  # go up 1 level
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

credentials_path = os.environ.get("CREDENTIALS_PATH")
project_id = os.environ.get("PROJECT_ID")

def test_credentials():
    """Test BigQuery credentials directly"""
    
    try:
        print("🔌 Testing BigQuery credentials directly...")
        
        # Create credentials object
        credentials = service_account.Credentials.from_service_account_file(
            credentials_path,
            scopes=["https://www.googleapis.com/auth/cloud-platform"]
        )
        
        print("✅ Credentials loaded successfully")
        
        # Create BigQuery client
        client = bigquery.Client(
            credentials=credentials,
            project=project_id
        )
        
        print("✅ BigQuery client created successfully")
        
        # Test a simple query
        query = f"SELECT COUNT(*) as count FROM `{project_id}.{marts_dataset}.__TABLES__`"
        print(f"🔍 Executing test query: {query}")
        
        query_job = client.query(query)
        results = query_job.result()
        
        for row in results:
            print(f"✅ Query successful! Found {row.count} tables")
        
        return True
        
    except Exception as e:
        print(f"❌ Error: {str(e)}")
        return False

if __name__ == "__main__":
    success = test_credentials()
    if success:
        print("\n🎉 BigQuery connection test successful!")
    else:
        print("\n💥 BigQuery connection test failed!")
