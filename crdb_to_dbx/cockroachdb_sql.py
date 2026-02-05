"""
CockroachDB SQL Operations

This module provides high-level functions for common CockroachDB SQL operations,
particularly for CDC changefeeds and table management.

Supports both Azure Blob Storage and Unity Catalog External Volumes.

Functions:
    - create_changefeed_from_config() - Create CDC changefeed (Azure or UC Volume)
    - get_existing_changefeeds() - Find existing changefeeds
    - cancel_changefeeds() - Cancel changefeeds
    - drop_table() - Drop a table
    - get_table_schema() - Extract table schema (primary keys, columns) from CockroachDB

Usage:
    from cockroachdb_sql import create_changefeed_from_config
    
    # Azure mode
    result = create_changefeed_from_config(conn, config, spark)
    
    # UC Volume mode (dbutils no longer needed - Spark handles file operations)
    result = create_changefeed_from_config(conn, config, spark)
    
    if result['created']:
        print(f"Changefeed created: {result['job_id']}")
    else:
        print(f"Changefeed already exists: {result['existing_count']}")
"""

from datetime import datetime
from typing import List, Dict, Any, Optional, Tuple


def _get_container_identifier_and_pattern(config, spark=None) -> Tuple[str, str]:
    """
    Get container/bucket identifier and build sink pattern for changefeed matching.
    
    Returns same pattern format for both Azure direct and UC Volume.
    
    Args:
        config: Config dataclass
        spark: SparkSession (required for UC Volume)
    
    Returns:
        Tuple[str, str]: (container_identifier, sink_pattern)
    
    Raises:
        ValueError: If configuration is invalid
    """
    if config.data_source == "azure":
        container_identifier = config.azure_storage.container_name
        
    elif config.data_source == "uc_external_volume":
        if spark is None:
            raise ValueError("spark required for UC Volume operations")
        
        # Query UC to get storage location
        volume_info = spark.sql(f"""
            SELECT storage_location 
            FROM system.information_schema.volumes 
            WHERE volume_catalog = '{config.uc_volume.volume_catalog}'
              AND volume_schema = '{config.uc_volume.volume_schema}'
              AND volume_name = '{config.uc_volume.volume_name}'
        """).collect()
        
        if not volume_info:
            raise ValueError(
                f"Volume not found: {config.uc_volume.volume_catalog}."
                f"{config.uc_volume.volume_schema}.{config.uc_volume.volume_name}"
            )
        
        storage_url = volume_info[0][0].rstrip('/')
        
        # Extract container from storage URL
        if 'abfss://' in storage_url:
            container_identifier = storage_url.split('abfss://')[1].split('@')[0]
        elif 's3://' in storage_url:
            container_identifier = storage_url.split('://')[1].split('/')[0]
        elif 'gs://' in storage_url:
            container_identifier = storage_url.split('://')[1].split('/')[0]
        else:
            raise ValueError(f"Unsupported storage URL: {storage_url}")
    
    else:
        raise ValueError(f"Unsupported data_source: {config.data_source}")
    
    # Build pattern - same for all storage types
    sink_pattern = f"%{container_identifier}/{config.cdc_config.path}%"
    return container_identifier, sink_pattern


def _parse_uc_volume_storage_for_changefeed(storage_location_url: str, config) -> Tuple[str, str]:
    """
    Parse Unity Catalog Volume storage location and convert to CockroachDB changefeed format.
    
    This function validates that:
    1. The storage location is supported by CockroachDB
    2. Required credentials are provided in config
    3. For Azure: the container in UC Volume matches the container in azure_storage config
    
    Args:
        storage_location_url: Storage location from Unity Catalog Volume 
                             (e.g., abfss://container@account.dfs.core.windows.net/)
        config: Config dataclass with azure_storage, uc_volume, and cdc_config
    
    Returns:
        Tuple[str, str]: (changefeed_path, container_identifier)
        - changefeed_path: CockroachDB-compatible sink URI (azure://, s3://, gs://)
        - container_identifier: Container/bucket name for pattern matching
    
    Raises:
        ValueError: If storage format is unsupported or credentials are missing
        ValueError: If Azure container in config doesn't match UC Volume container
    """
    # Parse storage URL and convert to CockroachDB-compatible format
    if 'abfss://' in storage_location_url:
        # Azure: abfss://container@account.dfs.core.windows.net/
        uc_container = storage_location_url.split('abfss://')[1].split('@')[0]
        
        if not config.azure_storage:
            raise ValueError(
                f"Azure credentials required for UC Volume backed by Azure.\n"
                f"UC Volume: {storage_location_url}\n"
                f"Container: {uc_container}\n"
                f"Add 'azure_storage' to config with container_name='{uc_container}'"
            )
        
        # Validate container match
        if config.azure_storage.container_name != uc_container:
            raise ValueError(
                f"Container mismatch: UC Volume={uc_container}, "
                f"Azure config={config.azure_storage.container_name}"
            )
        
        # Build azure:// URI (CockroachDB format)
        changefeed_path = (
            f"azure://{uc_container}/{config.cdc_config.path}"
            f"?AZURE_ACCOUNT_NAME={config.azure_storage.account_name}"
            f"&AZURE_ACCOUNT_KEY={config.azure_storage.account_key_encoded}"
        )
        container_identifier = uc_container
        
    elif 's3://' in storage_location_url:
        container_identifier = storage_location_url.split('://')[1].split('/')[0]
        changefeed_path = f"s3://{container_identifier}/{config.cdc_config.path}"
        
    elif 'gs://' in storage_location_url:
        container_identifier = storage_location_url.split('://')[1].split('/')[0]
        changefeed_path = f"gs://{container_identifier}/{config.cdc_config.path}"
        
    else:
        raise ValueError(f"Unsupported storage URL: {storage_location_url}")
    
    # Final validation
    if 'abfss://' in changefeed_path:
        raise ValueError(f"BUG: abfss:// in changefeed path: {changefeed_path}")
    
    return changefeed_path, container_identifier


def create_changefeed_from_config(
    conn,
    config,
    spark=None,
    wait_for_files: bool = True,
    max_wait: int = 300,
    check_interval: int = 5
) -> Dict[str, Any]:
    """
    Create a CDC changefeed using configuration from Config dataclass.
    
    Supports both Azure Blob Storage and Unity Catalog External Volumes.
    
    IMPORTANT: When using UC Volumes backed by Azure, Azure credentials are still required!
    - UC Volumes are for Databricks to READ data (Unity Catalog managed access)
    - CockroachDB changefeeds still need azure:// protocol with credentials to WRITE
    - CockroachDB doesn't support abfss:// URLs (only azure://, s3://, gs://)
    
    This function:
    1. Checks for existing changefeeds to avoid duplicates
    2. Builds storage URI based on data source (converts abfss:// to azure:// for CockroachDB)
    3. Creates changefeed with appropriate options (single_cf vs multi_cf)
    4. Optionally waits for initial files to appear in storage
    
    Args:
        conn: pg8000 connection object
        config: Config dataclass from cockroachdb_config.py
        spark: Spark session (required if wait_for_files=True)
        wait_for_files: If True, wait for initial CDC files to appear (default: True)
        max_wait: Maximum seconds to wait for files (default: 300)
        check_interval: Seconds between file checks (default: 5)
    
    Returns:
        dict with keys:
            - created: bool - True if new changefeed was created
            - job_id: int or None - Job ID if created
            - existing_count: int - Number of existing changefeeds found
            - existing_jobs: List[Tuple] - List of (job_id, status, sink_uri)
            - duplicate_warning: bool - True if multiple changefeeds exist
    
    Raises:
        ValueError: If wait_for_files=True but spark is None
        ValueError: If data_source is not recognized
    
    Example:
        >>> from cockroachdb_conn import get_cockroachdb_connection
        >>> from cockroachdb_sql import create_changefeed_from_config
        >>> 
        >>> conn = get_cockroachdb_connection(...)
        >>> 
        >>> # Works for both Azure and UC Volume
        >>> result = create_changefeed_from_config(conn, config, spark)
        >>> 
        >>> if result['created']:
        >>>     print(f"Created changefeed: {result['job_id']}")
        >>> else:
        >>>     print(f"Already exists: {result['existing_count']} changefeed(s)")
    """
    if wait_for_files and spark is None:
        raise ValueError("spark required when wait_for_files=True")
    
    # Build changefeed path
    if config.data_source == "azure":
        changefeed_path = (
            f"azure://{config.azure_storage.container_name}/{config.cdc_config.path}"
            f"?AZURE_ACCOUNT_NAME={config.azure_storage.account_name}"
            f"&AZURE_ACCOUNT_KEY={config.azure_storage.account_key_encoded}"
        )
        
    elif config.data_source == "uc_external_volume":
        # Query UC for storage URL
        volume_info = spark.sql(f"""
            SELECT storage_location 
            FROM system.information_schema.volumes 
            WHERE volume_catalog = '{config.uc_volume.volume_catalog}'
              AND volume_schema = '{config.uc_volume.volume_schema}'
              AND volume_name = '{config.uc_volume.volume_name}'
        """).collect()
        
        if not volume_info:
            raise ValueError(
                f"Volume not found: {config.uc_volume.volume_catalog}."
                f"{config.uc_volume.volume_schema}.{config.uc_volume.volume_name}"
            )
        
        storage_url = volume_info[0][0].rstrip('/')
        changefeed_path, _ = _parse_uc_volume_storage_for_changefeed(storage_url, config)
        
    else:
        raise ValueError(f"Unsupported data_source: {config.data_source}")
    
    # Get container identifier and sink pattern (same for both Azure and UC Volume)
    container_identifier, sink_pattern = _get_container_identifier_and_pattern(config, spark)
    
    # Build changefeed options based on column family mode
    if config.cdc_config.column_family_mode == "multi_cf":
        changefeed_options = """
    format='parquet',
    updated,
    resolved='10s',
    split_column_families
"""
    else:
        changefeed_options = """
    format='parquet',
    updated,
    resolved='10s'
"""
    
    # Create changefeed SQL
    create_changefeed_sql = f"""
CREATE CHANGEFEED FOR TABLE {config.tables.source_table_name}
INTO '{changefeed_path}'
WITH {changefeed_options}
"""
    
    with conn.cursor() as cur:
        # Check for existing changefeeds
        cur.execute("""
            SELECT job_id, status, sink_uri
            FROM [SHOW CHANGEFEED JOBS] 
            WHERE sink_uri LIKE %s
            AND status IN ('running', 'paused')
        """, (sink_pattern,))
        
        existing_changefeeds = cur.fetchall()
        
        if existing_changefeeds:
            print(f"✅ Found {len(existing_changefeeds)} existing changefeed(s)")
            for job_id, status, sink_uri in existing_changefeeds:
                print(f"   Job {job_id}: {status}")
                print(f"   Sink URI: {sink_uri}")
            
            if len(existing_changefeeds) > 1:
                print(f"⚠️  Multiple changefeeds may cause duplicates")
            
            return {
                'created': False,
                'job_id': None,
                'existing_count': len(existing_changefeeds),
                'existing_jobs': existing_changefeeds,
                'duplicate_warning': len(existing_changefeeds) > 1
            }
        
        # Create new changefeed
        cur.execute(create_changefeed_sql)
        result = cur.fetchone()
        
        if not result:
            raise RuntimeError("Changefeed creation returned no result")
        
        job_id = result[0]
        print(f"✅ Changefeed created: Job {job_id}")
        print(f"   {config.tables.source_table_name} → {config.tables.destination_table_name}")
        
        # Wait for files to appear
        if wait_for_files:
            if config.data_source == "azure":
                from cockroachdb_azure import wait_for_changefeed_files
                wait_for_changefeed_files(
                    config.azure_storage.account_name,
                    config.azure_storage.account_key,
                    config.azure_storage.container_name,
                    config.tables.source_catalog,
                    config.tables.source_schema,
                    config.tables.source_table_name,
                    config.tables.destination_table_name,
                    max_wait=max_wait,
                    check_interval=check_interval,
                    format=config.cdc_config.format
                )
            else:  # uc_external_volume
                import cockroachdb_uc_volume
                from cockroachdb_config import get_volume_path
                cockroachdb_uc_volume.wait_for_changefeed_files(
                    volume_path=get_volume_path(config),
                    source_catalog=config.tables.source_catalog,
                    source_schema=config.tables.source_schema,
                    source_table=config.tables.source_table_name,
                    target_table=config.tables.destination_table_name,
                    spark=spark,
                    max_wait=max_wait,
                    check_interval=check_interval,
                    format=config.cdc_config.format
                )
        
        return {
            'created': True,
            'job_id': job_id,
            'existing_count': 0,
            'existing_jobs': [],
            'duplicate_warning': False
        }


def get_existing_changefeeds(
    conn,
    config,
    spark=None
) -> List[Tuple[int, str, str]]:
    """
    Get existing changefeeds for a given source → target mapping.
    
    Supports both Azure and UC Volume.
    
    Args:
        conn: pg8000 connection object
        config: Config dataclass
        spark: SparkSession (required for UC Volume)
    
    Returns:
        List of tuples: [(job_id, status, sink_uri), ...]
    """
    _, sink_pattern = _get_container_identifier_and_pattern(config, spark)
    
    with conn.cursor() as cur:
        cur.execute("""
            SELECT job_id, status, sink_uri
            FROM [SHOW CHANGEFEED JOBS] 
            WHERE sink_uri LIKE %s
            AND status IN ('running', 'paused')
        """, (sink_pattern,))
        
        return cur.fetchall()


def cancel_changefeeds(
    conn,
    config,
    spark=None,
    verbose: bool = True
) -> Dict[str, Any]:
    """
    Cancel all changefeeds for a given source → target mapping.
    
    Args:
        conn: pg8000 connection object
        config: Config dataclass
        spark: SparkSession (required for UC Volume)
        verbose: If True, print detailed output (default: True)
    
    Returns:
        dict with cancelled_count and job_ids
    """
    changefeeds = get_existing_changefeeds(conn, config, spark)
    
    if not changefeeds:
        if verbose:
            print("No changefeeds found")
        return {'cancelled_count': 0, 'job_ids': []}
    
    if verbose:
        print(f"Cancelling {len(changefeeds)} changefeed(s)")
    
    cancelled_job_ids = []
    with conn.cursor() as cur:
        for job_id, status, sink_uri in changefeeds:
            cur.execute(f"CANCEL JOB {job_id}")
            cancelled_job_ids.append(job_id)
            if verbose:
                print(f"  ✅ Job {job_id}")
    
    return {'cancelled_count': len(cancelled_job_ids), 'job_ids': cancelled_job_ids}


def drop_table(conn, table_name: str, cascade: bool = True, verbose: bool = True) -> None:
    """Drop a table from CockroachDB."""
    cascade_clause = "CASCADE" if cascade else ""
    with conn.cursor() as cur:
        cur.execute(f"DROP TABLE IF EXISTS {table_name} {cascade_clause}")
        conn.commit()
    
    if verbose:
        print(f"✅ Dropped table '{table_name}'")


def get_table_schema(
    conn,
    catalog: str,
    schema: str,
    table_name: str,
    verbose: bool = True
) -> Dict[str, Any]:
    """
    Extract complete table schema from CockroachDB (primary keys, columns, metadata).

    Queries information_schema and SHOW CREATE TABLE. Use this to populate
    primary_key_columns for config or to write a schema file to blob/volume.

    Args:
        conn: pg8000 connection (must be connected to the database equal to catalog)
        catalog: Database/catalog name (e.g. 'defaultdb'); used in returned dict for storage paths
        schema: Schema name (e.g. 'public')
        table_name: Table name
        verbose: If True, print a short summary (default: True)

    Returns:
        Dict with: table_name, catalog, schema, primary_keys, columns (list of {name, type, nullable}),
        create_statement, has_column_families, schema_version, created_at.

    Raises:
        ValueError: If table has no primary key.

    Example:
        >>> schema_info = get_table_schema(conn, 'defaultdb', 'public', 'usertable')
        >>> primary_keys = schema_info['primary_keys']
        >>> # Write to Azure: write_schema_to_azure(..., schema_info=schema_info)
    """
    with conn.cursor() as cur:
        # SHOW CREATE TABLE: use schema.table (conn is already in catalog/database)
        target = f"{schema}.{table_name}" if schema != "public" else table_name
        cur.execute(f"SHOW CREATE TABLE {target}")
        row = cur.fetchone()
        create_statement = row[1] if row else ""

        # Primary keys (ordered by ordinal_position)
        pk_query = """
            SELECT kcu.column_name
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
              ON tc.constraint_name = kcu.constraint_name
              AND tc.table_schema = kcu.table_schema
              AND tc.table_name = kcu.table_name
            WHERE tc.constraint_type = 'PRIMARY KEY'
              AND tc.table_schema = %s
              AND tc.table_name = %s
            ORDER BY kcu.ordinal_position
        """
        cur.execute(pk_query, (schema, table_name))
        primary_keys = [r[0] for r in cur.fetchall()]
        if not primary_keys:
            raise ValueError(f"Table '{schema}.{table_name}' has no primary key.")

        # Columns
        col_query = """
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s
            ORDER BY ordinal_position
        """
        cur.execute(col_query, (schema, table_name))
        columns = [
            {"name": r[0], "type": r[1], "nullable": r[2] == "YES"}
            for r in cur.fetchall()
        ]

    has_column_families = create_statement.upper().count("FAMILY ") > 1
    schema_info = {
        "table_name": table_name,
        "catalog": catalog,
        "schema": schema,
        "primary_keys": primary_keys,
        "columns": columns,
        "create_statement": create_statement,
        "has_column_families": has_column_families,
        "schema_version": 1,
        "created_at": datetime.utcnow().isoformat() + "Z",
    }
    if verbose:
        print(f"✅ Schema for {catalog}.{schema}.{table_name}: {len(primary_keys)} PK(s), {len(columns)} columns")
    return schema_info


def show_all_changefeeds(conn, verbose: bool = True) -> List[Tuple]:
    """Show all changefeeds in the database."""
    with conn.cursor() as cur:
        cur.execute("SELECT job_id, status, sink_uri, description FROM [SHOW CHANGEFEED JOBS]")
        changefeeds = cur.fetchall()
    
    if verbose:
        print(f"Found {len(changefeeds)} changefeed(s)")
        for job_id, status, sink_uri, description in changefeeds:
            print(f"  Job {job_id}: {status}")
    
    return changefeeds


def cancel_all_changefeeds(conn, verbose: bool = True) -> Dict[str, Any]:
    """Cancel ALL changefeeds in database (use with caution!)."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT job_id, status, sink_uri
            FROM [SHOW CHANGEFEED JOBS] 
            WHERE status IN ('running', 'paused')
        """)
        changefeeds = cur.fetchall()
    
    if not changefeeds:
        if verbose:
            print("No changefeeds found")
        return {'cancelled_count': 0, 'job_ids': []}
    
    if verbose:
        print(f"⚠️  Cancelling ALL {len(changefeeds)} changefeed(s)")
    
    cancelled_job_ids = []
    with conn.cursor() as cur:
        for job_id, status, sink_uri in changefeeds:
            cur.execute(f"CANCEL JOB {job_id}")
            cancelled_job_ids.append(job_id)
            if verbose:
                print(f"  ✅ Job {job_id}")
    
    return {'cancelled_count': len(cancelled_job_ids), 'job_ids': cancelled_job_ids}
