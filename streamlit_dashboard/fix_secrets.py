#!/usr/bin/env python3
"""
Fix the secrets.toml file by properly formatting the private key
"""

import toml
import json

def fix_secrets():
    """Fix the secrets.toml file"""
    
    # Load the original JSON credentials
    with open("../credentials/sctp-dsai-468313-f5bc3e6b4ebe-innergritx.json", "r") as f:
        creds = json.load(f)
    
    # Create the secrets.toml content
    secrets_content = {
        "gcp_service_account": {
            "type": creds["type"],
            "project_id": creds["project_id"],
            "private_key_id": creds["private_key_id"],
            "private_key": creds["private_key"],  # This should be the raw string
            "client_email": creds["client_email"],
            "client_id": creds["client_id"],
            "auth_uri": creds["auth_uri"],
            "token_uri": creds["token_uri"],
            "auth_provider_x509_cert_url": creds["auth_provider_x509_cert_url"],
            "client_x509_cert_url": creds["client_x509_cert_url"],
            "universe_domain": creds.get("universe_domain", "googleapis.com")
        },
        "bigquery": {
            "project_id": creds["project_id"],
            "dataset_id": "olist_marts",
            "location": "US"
        },
        "streamlit": {
            "cache_ttl": 600,
            "max_query_timeout": 300
        }
    }
    
    # Write the fixed secrets.toml
    with open(".streamlit/secrets.toml", "w") as f:
        toml.dump(secrets_content, f)
    
    print("✅ Fixed secrets.toml file created")
    print("📝 Private key length:", len(creds["private_key"]))
    print("🔑 Private key starts with:", creds["private_key"][:50])

if __name__ == "__main__":
    fix_secrets()
