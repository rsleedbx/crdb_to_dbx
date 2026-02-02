"""
CockroachDB CDC Configuration Loader

This module loads and processes configuration for CockroachDB CDC to Databricks streaming.
Extracted from cockroachdb-cdc-tutorial.ipynb cells 1-62.
"""

import json
import os
from dataclasses import dataclass
from urllib.parse import quote
from typing import Dict, Any, List


@dataclass
class CockroachDBConfig:
    """CockroachDB connection configuration."""
    host: str
    port: int
    user: str
    password: str
    database: str


@dataclass
class TableConfig:
    """Source and destination table configuration."""
    source_catalog: str
    source_schema: str
    source_table_name: str
    destination_catalog: str
    destination_schema: str
    destination_table_name: str


@dataclass
class AzureStorageConfig:
    """Azure storage account configuration."""
    account_name: str
    account_key: str
    account_key_encoded: str
    container_name: str


@dataclass
class CDCConfig:
    """CDC changefeed configuration."""
    mode: str
    column_family_mode: str
    primary_key_columns: List[str]
    auto_suffix_mode_family: bool
    format: str
    path: str
    use_resolved_watermark: bool = True  # Use RESOLVED timestamps for column family completeness


@dataclass
class WorkloadConfig:
    """Workload configuration for testing."""
    snapshot_count: int
    insert_count: int
    update_count: int
    delete_count: int


@dataclass
class Config:
    """Complete configuration for CockroachDB CDC to Databricks."""
    cockroachdb: CockroachDBConfig
    tables: TableConfig
    azure_storage: AzureStorageConfig
    cdc_config: CDCConfig
    workload_config: WorkloadConfig


def load_config(config_file: str) -> Dict[str, Any]:
    """
    Load configuration from a JSON file.
    
    Args:
        config_file: Path to the JSON configuration file.
    
    Returns:
        Dictionary containing the configuration, or None if file cannot be loaded
    """
    try:
        with open(config_file, 'r') as f:
            config = json.load(f)
        print(f"✅ Configuration loaded from: {config_file}")
        return config
    except Exception as e:
        print(f"⚠️  Error loading config file: {e}")
        return None

def process_config(config: Dict[str, Any]) -> Config:
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
        Processed configuration as a Config dataclass instance
    """
    # Extract configuration values
    source_catalog = config["cockroachdb_source"]["catalog"]
    source_schema = config["cockroachdb_source"]["schema"]
    source_table = config["cockroachdb_source"]["table_name"]

    storage_account_key = config["azure_storage"]["account_key"]
    storage_account_key_encoded = quote(storage_account_key, safe='')

    target_table = config["databricks_target"]["table_name"]

    cdc_mode = config["cdc_config"]["mode"]
    column_family_mode = config["cdc_config"]["column_family_mode"]
    primary_key_columns = config["cdc_config"]["primary_key_columns"]

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

    # Extract format for reuse (default: parquet)
    cdc_format = config["cdc_config"].get("format", "parquet")

    # Set the path in azure
    path = f"{cdc_format}/{source_catalog}/{source_schema}/{source_table}/{target_table}"

    # Create dataclass instances
    cockroachdb_config = CockroachDBConfig(
        host=config["cockroachdb"]["host"],
        port=config["cockroachdb"]["port"],
        user=config["cockroachdb"]["user"],
        password=config["cockroachdb"]["password"],
        database=config["cockroachdb"]["database"]
    )
    
    table_config = TableConfig(
        source_catalog=source_catalog,
        source_schema=source_schema,
        source_table_name=source_table,
        destination_catalog=config["databricks_target"]["catalog"],
        destination_schema=config["databricks_target"]["schema"],
        destination_table_name=target_table
    )
    
    azure_storage_config = AzureStorageConfig(
        account_name=config["azure_storage"]["account_name"],
        account_key=storage_account_key,
        account_key_encoded=storage_account_key_encoded,
        container_name=config["azure_storage"]["container_name"]
    )
    
    cdc_config_obj = CDCConfig(
        mode=cdc_mode,
        column_family_mode=column_family_mode,
        primary_key_columns=primary_key_columns,
        auto_suffix_mode_family=auto_suffix,
        format=cdc_format,
        path=path
    )
    
    workload_config = WorkloadConfig(
        snapshot_count=config["workload_config"]["snapshot_count"],
        insert_count=config["workload_config"]["insert_count"],
        update_count=config["workload_config"]["update_count"],
        delete_count=config["workload_config"]["delete_count"]
    )
    
    # Create main config object
    processed_config = Config(
        cockroachdb=cockroachdb_config,
        tables=table_config,
        azure_storage=azure_storage_config,
        cdc_config=cdc_config_obj,
        workload_config=workload_config
    )

    # Print configuration summary
    print("✅ Configuration loaded")
    print(f"   CDC Processing Mode: {cdc_mode}")
    print(f"   Column Family Mode: {column_family_mode}")
    print(f"   Primary Keys: {primary_key_columns}")
    print(f"   Target Table: {target_table}")
    print(f"   CDC Workload: {workload_config.snapshot_count} snapshot → +{workload_config.insert_count} INSERTs, ~{workload_config.update_count} UPDATEs, -{workload_config.delete_count} DELETEs")

    return processed_config


def load_and_process_config(config_file: str) -> Config:
    """
    Load and process configuration in one step.
    
    Args:
        config_file: Path to the JSON configuration file.
    
    Returns:
        Fully processed configuration as a Config dataclass instance, or None if config cannot be loaded
    """
    config = load_config(config_file)
    if config is None:
        return None
    return process_config(config)


if __name__ == "__main__":
    # Example usage
    config = load_and_process_config("../.env/cockroachdb_cdc_tutorial_config_append_single_cf.json")
