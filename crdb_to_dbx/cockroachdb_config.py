"""
CockroachDB CDC Configuration Loader

This module loads and processes configuration for CockroachDB CDC to Databricks streaming.
Extracted from cockroachdb-cdc-tutorial.ipynb cells 1-62.
"""

import json
import os
from dataclasses import dataclass
from urllib.parse import quote
from typing import Dict, Any, List, Optional


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
class UCVolumeConfig:
    """Unity Catalog external volume configuration."""
    volume_catalog: str
    volume_schema: str
    volume_name: str
    volume_full_path: str
    volume_id: str


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
    # Optional: base path for streaming checkpoints. checkpoint_path = checkpoint_base_path / table_name [+ mode_suffix].
    # If unset (default): /Volumes/{destination_catalog}/{destination_schema}/checkpoints (create a Volume named "checkpoints" in the target schema).
    checkpoint_base_path: Optional[str] = None


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
    cdc_config: CDCConfig
    workload_config: WorkloadConfig
    data_source: str = "azure_storage"  # "azure_storage" or "uc_external_volume"
    azure_storage: Optional[AzureStorageConfig] = None
    uc_volume: Optional[UCVolumeConfig] = None


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
    - URL-encodes the Azure storage account key (if Azure storage is used)
    - Auto-suffixes table names with CDC mode and column family mode if enabled
    - Generates the CDC path in storage (Azure or UC Volume)
    - Supports both azure_storage and uc_external_volume data sources
    
    Args:
        config: Raw configuration dictionary
    
    Returns:
        Processed configuration as a Config dataclass instance
    """
    # Extract configuration values
    source_catalog = config["cockroachdb_source"]["catalog"]
    source_schema = config["cockroachdb_source"]["schema"]
    source_table = config["cockroachdb_source"]["table_name"]

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

    # Set the path structure (same for both Azure and UC Volume)
    path = f"{cdc_format}/{source_catalog}/{source_schema}/{source_table}/{target_table}"

    # Determine data source (default to azure_storage for backward compatibility)
    data_source = config["cdc_config"].get("data_source", "azure_storage")

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
    
    # Process Azure storage config if present
    azure_storage_config = None
    if "azure_storage" in config and config["azure_storage"]:
        storage_account_key = config["azure_storage"]["account_key"]
        storage_account_key_encoded = quote(storage_account_key, safe='')
        
        azure_storage_config = AzureStorageConfig(
            account_name=config["azure_storage"]["account_name"],
            account_key=storage_account_key,
            account_key_encoded=storage_account_key_encoded,
            container_name=config["azure_storage"]["container_name"]
        )
    
    # Process UC Volume config if present
    uc_volume_config = None
    if "uc_external_volume" in config and config["uc_external_volume"]:
        uc_volume_config = UCVolumeConfig(
            volume_catalog=config["uc_external_volume"]["volume_catalog"],
            volume_schema=config["uc_external_volume"]["volume_schema"],
            volume_name=config["uc_external_volume"]["volume_name"],
            volume_full_path=config["uc_external_volume"]["volume_full_path"],
            volume_id=config["uc_external_volume"]["volume_id"]
        )
    
    # Checkpoint base path: from config, or default = /Volumes/{destination_catalog}/{destination_schema}/checkpoints
    dest_catalog = config["databricks_target"]["catalog"]
    dest_schema = config["databricks_target"]["schema"]
    checkpoint_base_path = config["cdc_config"].get("checkpoint_base_path")
    if not checkpoint_base_path or not str(checkpoint_base_path).strip():
        checkpoint_base_path = f"/Volumes/{dest_catalog}/{dest_schema}/checkpoints"

    cdc_config_obj = CDCConfig(
        mode=cdc_mode,
        column_family_mode=column_family_mode,
        primary_key_columns=primary_key_columns,
        auto_suffix_mode_family=auto_suffix,
        format=cdc_format,
        path=path,
        use_resolved_watermark=config["cdc_config"].get("use_resolved_watermark", True),
        checkpoint_base_path=checkpoint_base_path
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
        cdc_config=cdc_config_obj,
        workload_config=workload_config,
        data_source=data_source,
        azure_storage=azure_storage_config,
        uc_volume=uc_volume_config
    )

    # Print configuration summary
    print("✅ Configuration loaded")
    print(f"   Data Source: {data_source}")
    if data_source == "azure_storage" and azure_storage_config:
        print(f"   Azure Storage Account: {azure_storage_config.account_name}")
        print(f"   Container: {azure_storage_config.container_name}")
    elif data_source == "uc_external_volume" and uc_volume_config:
        print(f"   UC Volume: {uc_volume_config.volume_full_path}")
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


def get_storage_path(config: Config) -> str:
    """
    Get the storage path based on the data source configuration.
    
    Args:
        config: Processed Config dataclass instance
    
    Returns:
        str: Full storage path for the CDC data
        
    Raises:
        ValueError: If data source is not configured properly
    
    Example:
        >>> config = load_and_process_config("config.json")
        >>> path = get_storage_path(config)
        >>> # Azure: abfss://container@account.dfs.core.windows.net/parquet/...
        >>> # UC Volume: /Volumes/catalog/schema/volume/parquet/...
    """
    if config.data_source == "azure_storage":
        if not config.azure_storage:
            raise ValueError("Azure storage is selected but azure_storage config is missing")
        return f"abfss://{config.azure_storage.container_name}@{config.azure_storage.account_name}.dfs.core.windows.net/{config.cdc_config.path}"
    
    elif config.data_source == "uc_external_volume":
        if not config.uc_volume:
            raise ValueError("UC external volume is selected but uc_external_volume config is missing")
        return f"/Volumes/{config.uc_volume.volume_catalog}/{config.uc_volume.volume_schema}/{config.uc_volume.volume_name}/{config.cdc_config.path}"
    
    else:
        raise ValueError(f"Unknown data source: {config.data_source}")


def get_volume_path(config: Config) -> str:
    """
    Get the Unity Catalog volume path (for use with cockroachdb_uc_volume.py functions).
    
    Args:
        config: Processed Config dataclass instance
    
    Returns:
        str: UC Volume base path
        
    Raises:
        ValueError: If UC volume is not configured
    
    Example:
        >>> config = load_and_process_config("config.json")
        >>> volume_path = get_volume_path(config)
        >>> # Returns: /Volumes/catalog/schema/volume
    """
    if not config.uc_volume:
        raise ValueError("UC external volume is not configured")
    
    return f"/Volumes/{config.uc_volume.volume_catalog}/{config.uc_volume.volume_schema}/{config.uc_volume.volume_name}"


if __name__ == "__main__":
    # Example usage
    config = load_and_process_config("../.env/cockroachdb_cdc_tutorial_config_append_single_cf.json")
    if config:
        print(f"\nStorage path: {get_storage_path(config)}")
