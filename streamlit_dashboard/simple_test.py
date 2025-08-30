#!/usr/bin/env python3
"""
Simple BigQuery connection test without Streamlit context
"""

import json
from google.cloud import bigquery
from google.oauth2 import service_account

def test_credentials():
    """Test BigQuery credentials directly"""
    
    # Load credentials from the JSON file directly
    credentials_path = "/home/chrisfkh/sctp-ds-ai/mod2/sctp-dsai-mod2-project-team4/credentials/sctp-dsai-468313-f5bc3e6b4ebe-innergritx.json"
    
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
            project="sctp-dsai-468313"
        )
        
        print("✅ BigQuery client created successfully")
        
        # Test a simple query
        query = "SELECT COUNT(*) as count FROM `sctp-dsai-468313.olist_marts.__TABLES__`"
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
