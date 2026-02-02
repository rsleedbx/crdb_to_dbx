"""
CockroachDB CDC Configuration Loader

This module loads and processes configuration for CockroachDB CDC to Databricks streaming.
Extracted from cockroachdb-cdc-tutorial.ipynb cells 1-62.
"""

import json
import os
from urllib.parse import quote
from typing import Dict, Any, Optional


def load_config(config_file: Optional[str] = None) -> Dict[str, Any]:
    """
    Load configuration from a JSON file with embedded fallback.
    
    Args:
        config_file: Path to the JSON configuration file. If None, uses embedded config.
    
    Returns:
        Dictionary containing the configuration
    """
    config = None
    
    # Try to load from file if provided
    if config_file:
        try:
            with open(config_file, 'r') as f:
                config = json.load(f)
            print(f"✅ Configuration loaded from: {config_file}")
        except Exception as e:
            print(f"ℹ️  Using embedded configuration (config file error: {e})")
            return(None)

def process_config(config: Dict[str, Any]) -> Dict[str, Any]:
    """
    Process configuration by extracting values and applying transformations.
    
    This function:
    - Extracts all configuration values into individual variables
    - URL-encodes the Azure storage account key
    - Auto-suffixes table names with CDC mode and column family mode if enabled
    - Generates the CDC path in Azure storage
    
    Args:
        config: Raw configuration dictionary
    
    Returns:
        Processed configuration dictionary with additional computed fields
    """
    # Extract configuration values
    cockroachdb_host = config["cockroachdb"]["host"]
    cockroachdb_port = config["cockroachdb"]["port"]
    cockroachdb_user = config["cockroachdb"]["user"]
    cockroachdb_password = config["cockroachdb"]["password"]
    cockroachdb_database = config["cockroachdb"]["database"]

    source_catalog = config["cockroachdb_source"]["catalog"]
    source_schema = config["cockroachdb_source"]["schema"]
    source_table = config["cockroachdb_source"]["table_name"]

    storage_account_name = config["azure_storage"]["account_name"]
    storage_account_key = config["azure_storage"]["account_key"]
    storage_account_key_encoded = quote(storage_account_key, safe='')
    container_name = config["azure_storage"]["container_name"]

    target_catalog = config["databricks_target"]["catalog"]
    target_schema = config["databricks_target"]["schema"]
    target_table = config["databricks_target"]["table_name"]

    cdc_mode = config["cdc_config"]["mode"]
    column_family_mode = config["cdc_config"]["column_family_mode"]
    primary_key_columns = config["cdc_config"]["primary_key_columns"]

    snapshot_count = config["workload_config"]["snapshot_count"]
    insert_count = config["workload_config"]["insert_count"]
    update_count = config["workload_config"]["update_count"]
    delete_count = config["workload_config"]["delete_count"]

    # Auto-suffix table names with mode and column family if enabled
    auto_suffix = config["cdc_config"].get("auto_suffix_mode_family", False)
    if auto_suffix:
        suffix = f"_{cdc_mode}_{column_family_mode}"
        
        # Add suffix to source_table if not already present
        if not source_table.endswith(suffix):
            source_table = f"{source_table}{suffix}"
        
        # Add suffix to target_table if not already present
        if not target_table.endswith(suffix):
            target_table = f"{target_table}{suffix}"

        # Update config dict with suffixed table names
        config["cockroachdb_source"]["table_name"] = source_table
        config["databricks_target"]["table_name"] = target_table

    # Extract format for reuse (default: parquet)
    cdc_format = config["cdc_config"].get("format", "parquet")

    # Set the path in azure
    path = f"{cdc_format}/{source_catalog}/{source_schema}/{source_table}/{target_table}"
    config["cdc_config"]["path"] = path

    # Print configuration summary
    print("✅ Configuration loaded")
    print(f"   CDC Processing Mode: {cdc_mode}")
    print(f"   Column Family Mode: {column_family_mode}")
    print(f"   Primary Keys: {primary_key_columns}")
    print(f"   Target Table: {target_table}")
    print(f"   CDC Workload: {snapshot_count} snapshot → +{insert_count} INSERTs, ~{update_count} UPDATEs, -{delete_count} DELETEs")

    return config


def load_and_process_config(config_file: Optional[str] = None) -> Dict[str, Any]:
    """
    Load and process configuration in one step.
    
    Args:
        config_file: Path to the JSON configuration file. If None, uses embedded config.
    
    Returns:
        Fully processed configuration dictionary
    """
    config = load_config(config_file)
    return process_config(config)


if __name__ == "__main__":
    # Example usage
    config = load_and_process_config("../.env/cockroachdb_cdc_tutorial_config_append_single_cf.json")
