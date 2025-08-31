#!/usr/bin/env python3
"""
Fix the secrets.toml file by properly formatting the private key
"""


import toml
import json
import os
from pathlib import Path
from dotenv import load_dotenv

# Load .env from the parent directory
env_path = Path(__file__).resolve().parent.parent / ".env"
if env_path.exists():
    load_dotenv(dotenv_path=env_path)
else:
    # fallback: try env_sample if .env does not exist
    sample_env_path = Path(__file__).resolve().parent.parent / "env_sample"
    if sample_env_path.exists():
        load_dotenv(dotenv_path=sample_env_path)

def fix_secrets():
    """Fix the secrets.toml file"""

    credentials_path = os.environ.get("CREDENTIALS_PATH")
    if not credentials_path:
        raise EnvironmentError("CREDENTIALS_PATH environment variable not set")
    olis_marts = os.environ.get("MARTS_DATASET_NAME")
    if not olis_marts:
        raise EnvironmentError("MARTS_DATASET_NAME environment variable not set")

    # Load the original JSON credentials
    with open(credentials_path, "r") as f:
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
            "dataset_id": f"{olis_marts}_marts",
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
