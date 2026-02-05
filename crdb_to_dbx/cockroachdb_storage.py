"""
CockroachDB CDC Storage Abstraction Layer

This module provides unified functions that work with both Azure Blob Storage
and Unity Catalog External Volumes. Functions automatically switch based on
the `data_source` configuration field.

Use these functions in notebooks and scripts to support both storage modes
without code changes.
"""

from typing import Dict, Any, Optional


def schema_path_relative(
    source_catalog: str,
    source_schema: str,
    source_table: str,
    format_type: str = "parquet",
) -> str:
    """
    Relative path for schema file (same for Azure and UC Volume).
    Returns: {format}/{catalog}/{schema}/{table}/_metadata/schema.json
    """
    return f"{format_type}/{source_catalog}/{source_schema}/{source_table}/_metadata/schema.json"


def load_schema(
    config,
    spark=None,
) -> Optional[Dict[str, Any]]:
    """
    Load schema file from storage. Schema is stored in Azure (same blob as changefeed);
    UC Volume is a view over that blob, so we try Azure first when credentials exist,
    then UC Volume (for the same path, visible via the volume).

    Args:
        config: Config dataclass from cockroachdb_config.py
        spark: SparkSession (required for UC Volume load when used, ignored for Azure)

    Returns:
        Schema dict (primary_keys, columns, create_statement, ...) or None if not found.

    Example:
        >>> from cockroachdb_storage import load_schema
        >>> schema_info = load_schema(config, spark)
        >>> if schema_info:
        ...     primary_key_columns = schema_info["primary_keys"]
    """
    if config.azure_storage:
        import cockroachdb_azure
        out = cockroachdb_azure.load_schema_from_azure(
            storage_account_name=config.azure_storage.account_name,
            storage_account_key=config.azure_storage.account_key,
            container_name=config.azure_storage.container_name,
            source_catalog=config.tables.source_catalog,
            source_schema=config.tables.source_schema,
            source_table=config.tables.source_table_name,
            format_type=config.cdc_config.format,
        )
        if out is not None:
            return out
    if config.data_source == "uc_external_volume" and config.uc_volume and spark:
        import cockroachdb_uc_volume
        from cockroachdb_config import get_volume_path
        volume_path = get_volume_path(config)
        return cockroachdb_uc_volume.load_schema_from_uc_volume(
            volume_path=volume_path,
            source_catalog=config.tables.source_catalog,
            source_schema=config.tables.source_schema,
            source_table=config.tables.source_table_name,
            spark=spark,
            format_type=config.cdc_config.format,
        )
    return None


def write_schema(
    config,
    schema_info: Dict[str, Any],
    spark=None,
    verbose: bool = True,
) -> str:
    """
    Write schema file to blob storage (Azure). CockroachDB writes only to Azure/other blob;
    UC Volume is a view over that same storage, so writing to Azure makes the schema visible
    to both Azure and UC Volume. Never write via Spark to the UC Volume path.

    When config.azure_storage is present we write to Azure. Otherwise we require
    data_source == "azure_storage" with azure_storage config.

    Args:
        config: Config dataclass from cockroachdb_config.py
        schema_info: Dict from get_table_schema() (primary_keys, columns, ...)
        spark: Unused (kept for API compatibility)
        verbose: If True, print the path written (default: True)

    Returns:
        Path or identifier of the written schema (e.g. blob path).
    """
    if not config.azure_storage:
        raise ValueError(
            "Schema must be written to Azure (or other blob storage). "
            "CockroachDB cannot write to UC Volume; add azure_storage to config so we can write the schema to the same blob the changefeed uses. UC Volume will see it automatically."
        )
    import cockroachdb_azure
    return cockroachdb_azure.write_schema_to_azure(
        storage_account_name=config.azure_storage.account_name,
        storage_account_key=config.azure_storage.account_key,
        container_name=config.azure_storage.container_name,
        source_catalog=config.tables.source_catalog,
        source_schema=config.tables.source_schema,
        source_table=config.tables.source_table_name,
        schema_info=schema_info,
        format_type=config.cdc_config.format,
        verbose=verbose,
    )


def ensure_schema_in_storage(
    config,
    spark,
    conn=None,
    verbose: bool = True,
) -> None:
    """
    Ensure the schema file (primary keys, columns, etc.) exists in storage (Azure or UC Volume).
    Call this during prep, before running the autoloader/ingestion, so the backend can run
    without source (CockroachDB) credentials. If the schema file already exists, this is a no-op.

    - If schema is already in storage: returns immediately.
    - If not and conn is provided: fetches schema from CockroachDB (get_table_schema) and
      writes it to storage (write_schema).
    - If not and conn is not provided: raises ValueError (run prep with a connection).

    Args:
        config: Config dataclass from cockroachdb_config.py
        spark: SparkSession (required for UC Volume; used for write_schema)
        conn: CockroachDB connection (required only when schema file does not yet exist)
        verbose: If True, print progress (default: True)

    Raises:
        ValueError: If schema file is not in storage and conn is not provided.
    """
    schema_info = load_schema(config, spark=spark)
    if schema_info and schema_info.get("primary_keys"):
        if verbose:
            print(f"✅ Schema already in storage (primary keys: {schema_info['primary_keys']})")
        return
    if conn is None:
        raise ValueError(
            "Schema file not in storage and no CockroachDB connection provided. "
            "Run prep with a connection (e.g. ensure_schema_in_storage(config, spark, conn)) "
            "so we can write the schema to storage; then ingestion can run without source credentials."
        )
    from cockroachdb_sql import get_table_schema
    schema_info = get_table_schema(
        conn,
        config.tables.source_catalog,
        config.tables.source_schema,
        config.tables.source_table_name,
        verbose=verbose,
    )
    write_schema(config, schema_info, spark=spark, verbose=verbose)
    if verbose:
        print("✅ Schema written to storage; autoloader can resolve primary keys without source credentials.")


def check_files(
    config,
    spark=None,
    verbose: bool = True
) -> Dict[str, Any]:
    """
    Check for changefeed files in storage (Azure or UC Volume).
    
    This function automatically switches between Azure Blob Storage and
    Unity Catalog External Volume based on config.data_source.
    
    Args:
        config: Config dataclass from cockroachdb_config.py
        spark: SparkSession (required for UC Volume, optional for Azure)
        verbose: Print detailed output (default: True)
    
    Returns:
        dict with 'data_files' and 'resolved_files' lists
        
        data_files: List of file info dicts with keys:
            - name: filename
            - path: full path
            - size: file size in bytes
        
        resolved_files: List of RESOLVED file info dicts (same structure)
    
    Example:
        >>> from cockroachdb_config import load_and_process_config
        >>> from cockroachdb_storage import check_files
        >>> 
        >>> config = load_and_process_config("config.json")
        >>> result = check_files(config, spark)
        >>> print(f"Found {len(result['data_files'])} data files")
    """
    # Determine storage mode
    if config.data_source == "azure_storage":
        # Use Azure Blob Storage
        if not config.azure_storage:
            raise ValueError("Azure storage is selected but azure_storage config is missing")
        
        import cockroachdb_azure
        
        return cockroachdb_azure.check_azure_files(
            storage_account_name=config.azure_storage.account_name,
            storage_account_key=config.azure_storage.account_key,
            container_name=config.azure_storage.container_name,
            source_catalog=config.tables.source_catalog,
            source_schema=config.tables.source_schema,
            source_table=config.tables.source_table_name,
            target_table=config.tables.destination_table_name,
            verbose=verbose,
            format=config.cdc_config.format
        )
    
    elif config.data_source == "uc_external_volume":
        # Use Unity Catalog External Volume
        if not config.uc_volume:
            raise ValueError("UC external volume is selected but uc_external_volume config is missing")
        
        if not spark:
            raise ValueError("spark is required for UC Volume access")
        
        import cockroachdb_uc_volume
        from cockroachdb_config import get_volume_path
        
        volume_path = get_volume_path(config)
        
        return cockroachdb_uc_volume.check_volume_files(
            volume_path=volume_path,
            source_catalog=config.tables.source_catalog,
            source_schema=config.tables.source_schema,
            source_table=config.tables.source_table_name,
            target_table=config.tables.destination_table_name,
            spark=spark,
            verbose=verbose,
            format=config.cdc_config.format
        )
    
    else:
        raise ValueError(f"Unknown data source: {config.data_source}")


def wait_for_files(
    config,
    spark=None,
    max_wait: int = 120,
    check_interval: int = 5,
    stabilization_wait: Optional[int] = None,
    wait_for_resolved: bool = True
) -> Dict[str, Any]:
    """
    Wait for changefeed files to appear in storage (Azure or UC Volume).
    
    This function automatically switches between Azure Blob Storage and
    Unity Catalog External Volume based on config.data_source.
    
    This function can operate in two modes:
    1. RESOLVED mode (wait_for_resolved=True): Waits for .RESOLVED file ✅ RECOMMENDED
       - CRITICAL for column family completeness guarantee
       - Returns the RESOLVED filename for coordination
       - No stabilization wait needed (RESOLVED guarantees completeness)
       - Recommended for production multi-CF tables
    
    2. Data file mode (wait_for_resolved=False): Waits for data files with stabilization
       - Legacy mode for backward compatibility
       - Uses stabilization_wait to detect when all files have landed
       - Not recommended for production (use RESOLVED mode instead)
    
    Args:
        config: Config dataclass from cockroachdb_config.py
        spark: SparkSession (required for UC Volume, optional for Azure)
        max_wait: Maximum seconds to wait for files (default: 120)
        check_interval: Seconds between checks (default: 5)
        stabilization_wait: Seconds to wait for file count to stabilize (default: None)
                           - If None: Defaults to 5s in legacy mode, unused in RESOLVED mode
                           - Only used when wait_for_resolved=False
                           - Ignored in RESOLVED mode (not needed)
        wait_for_resolved: If True, wait for RESOLVED file (recommended, default)
                          If False, wait for data files (legacy mode)
    
    Returns:
        dict with:
        - 'success': bool - True if files/RESOLVED found
        - 'resolved_file': str or None - RESOLVED filename if wait_for_resolved=True
        - 'elapsed_time': int - Total seconds waited
        - 'file_count': int - Number of files found
    
    Example (RESOLVED mode - Recommended):
        >>> from cockroachdb_config import load_and_process_config
        >>> from cockroachdb_storage import wait_for_files
        >>> 
        >>> config = load_and_process_config("config.json")
        >>> result = wait_for_files(
        ...     config, spark,
        ...     max_wait=300,
        ...     wait_for_resolved=True  # ✅ Wait for RESOLVED
        ... )
        >>> if result['success']:
        ...     print(f"RESOLVED file: {result['resolved_file']}")
    """
    # Determine storage mode
    if config.data_source == "azure_storage":
        # Use Azure Blob Storage
        if not config.azure_storage:
            raise ValueError("Azure storage is selected but azure_storage config is missing")
        
        import cockroachdb_azure
        
        return cockroachdb_azure.wait_for_changefeed_files(
            storage_account_name=config.azure_storage.account_name,
            storage_account_key=config.azure_storage.account_key,
            container_name=config.azure_storage.container_name,
            source_catalog=config.tables.source_catalog,
            source_schema=config.tables.source_schema,
            source_table=config.tables.source_table_name,
            target_table=config.tables.destination_table_name,
            max_wait=max_wait,
            check_interval=check_interval,
            stabilization_wait=stabilization_wait,
            format=config.cdc_config.format,
            wait_for_resolved=wait_for_resolved
        )
    
    elif config.data_source == "uc_external_volume":
        # Use Unity Catalog External Volume
        if not config.uc_volume:
            raise ValueError("UC external volume is selected but uc_external_volume config is missing")
        
        if not spark:
            raise ValueError("spark is required for UC Volume access")
        
        import cockroachdb_uc_volume
        from cockroachdb_config import get_volume_path
        
        volume_path = get_volume_path(config)
        
        return cockroachdb_uc_volume.wait_for_changefeed_files(
            volume_path=volume_path,
            source_catalog=config.tables.source_catalog,
            source_schema=config.tables.source_schema,
            source_table=config.tables.source_table_name,
            target_table=config.tables.destination_table_name,
            spark=spark,
            max_wait=max_wait,
            check_interval=check_interval,
            stabilization_wait=stabilization_wait,
            format=config.cdc_config.format,
            wait_for_resolved=wait_for_resolved
        )
    
    else:
        raise ValueError(f"Unknown data source: {config.data_source}")


# ============================================================================
# Backward Compatibility Aliases
# ============================================================================

# For backward compatibility with existing notebooks
check_azure_files = check_files  # Works with both Azure and UC Volume
wait_for_changefeed_files = wait_for_files  # Works with both Azure and UC Volume


# ============================================================================
# Usage Examples
# ============================================================================

if __name__ == "__main__":
    print("CockroachDB CDC Storage Abstraction Layer")
    print("=" * 80)
    print()
    print("This module provides unified functions that work with both:")
    print("  - Azure Blob Storage")
    print("  - Unity Catalog External Volumes")
    print()
    print("Usage:")
    print()
    print("  from cockroachdb_config import load_and_process_config")
    print("  from cockroachdb_storage import check_files, wait_for_files")
    print()
    print("  # Load config (auto-detects storage mode)")
    print("  config = load_and_process_config('config.json')")
    print()
    print("  # Check files (works with both Azure and UC Volume)")
    print("  result = check_files(config, spark)")
    print()
    print("  # Wait for files (works with both Azure and UC Volume)")
    print("  result = wait_for_files(config, spark, wait_for_resolved=True)")
    print()
    print("  # Load schema (primary keys, etc.) from storage")
    print("  schema_info = load_schema(config, spark)")
    print("  # Write schema to storage (e.g. after get_table_schema from cockroachdb_sql)")
    print("  write_schema(config, schema_info, spark)")
    print()
    print("Switch storage modes by changing config.data_source:")
    print("  - 'azure_storage': Uses Azure Blob Storage")
    print("  - 'uc_external_volume': Uses Unity Catalog External Volume")
