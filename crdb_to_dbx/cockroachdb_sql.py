"""
CockroachDB SQL Operations

This module provides high-level functions for common CockroachDB SQL operations,
particularly for CDC changefeeds and table management.

Functions:
    - create_changefeed_from_config() - Create CDC changefeed
    - get_existing_changefeeds() - Find existing changefeeds
    - cancel_changefeeds() - Cancel changefeeds
    - drop_table() - Drop a table

Usage:
    from cockroachdb_sql import create_changefeed_from_config
    
    result = create_changefeed_from_config(conn, config, spark)
    if result['created']:
        print(f"Changefeed created: {result['job_id']}")
    else:
        print(f"Changefeed already exists: {result['existing_count']}")
"""

from typing import List, Dict, Any, Optional, Tuple


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
    
    This function:
    1. Checks for existing changefeeds to avoid duplicates
    2. Builds Azure Blob Storage URI with encoded credentials
    3. Creates changefeed with appropriate options (single_cf vs multi_cf)
    4. Optionally waits for initial files to appear in Azure
    
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
    
    Example:
        >>> from cockroachdb_conn import get_cockroachdb_connection
        >>> from cockroachdb_sql import create_changefeed_from_config
        >>> 
        >>> conn = get_cockroachdb_connection(...)
        >>> result = create_changefeed_from_config(conn, config, spark)
        >>> 
        >>> if result['created']:
        >>>     print(f"Created changefeed: {result['job_id']}")
        >>> else:
        >>>     print(f"Already exists: {result['existing_count']} changefeed(s)")
    """
    if wait_for_files and spark is None:
        raise ValueError("spark parameter required when wait_for_files=True")
    
    # Build Azure Blob Storage URI with table-specific path
    changefeed_path = (
        f"azure://{config.azure_storage.container_name}/{config.cdc_config.path}"
        f"?AZURE_ACCOUNT_NAME={config.azure_storage.account_name}"
        f"&AZURE_ACCOUNT_KEY={config.azure_storage.account_key_encoded}"
    )
    
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
        sink_uri_pattern = f"%{config.azure_storage.container_name}/{config.cdc_config.path}%"
        
        cur.execute("""
            SELECT job_id, status, sink_uri
            FROM [SHOW CHANGEFEED JOBS] 
            WHERE sink_uri LIKE %s
            AND status IN ('running', 'paused')
        """, (sink_uri_pattern,))
        
        existing_changefeeds = cur.fetchall()
        
        if existing_changefeeds:
            # Changefeed(s) already exist
            print(f"✅ Changefeed(s) already exist for this source → target mapping")
            print(f"   Found {len(existing_changefeeds)} changefeed(s):")
            for job_id, status, sink_uri in existing_changefeeds:
                print(f"   • Job ID: {job_id}, Status: {status}")
                print(f"     Sink URI: {sink_uri[:80]}...")
            
            if len(existing_changefeeds) > 1:
                print(f"\n⚠️  WARNING: Multiple changefeeds detected for same destination!")
                print(f"   This may cause duplicate data. Consider calling cancel_changefeeds().")
            
            if config.cdc_config.column_family_mode == "multi_cf":
                print(f"\n   Expected: Column family fragments")
            
            return {
                'created': False,
                'job_id': None,
                'existing_count': len(existing_changefeeds),
                'existing_jobs': existing_changefeeds,
                'duplicate_warning': len(existing_changefeeds) > 1
            }
        else:
            # Create new changefeed
            cur.execute(create_changefeed_sql)
            result = cur.fetchone()
            job_id = result[0]
            
            print(f"✅ Changefeed created")
            print(f"   Job ID: {job_id}")
            print(f"   Source: {config.tables.source_catalog}.{config.tables.source_schema}.{config.tables.source_table_name}")
            print(f"   Target path: .../{config.tables.source_table_name}/{config.tables.destination_table_name}/")
            print(f"   Format: Parquet")
            if config.cdc_config.column_family_mode == "multi_cf":
                print(f"   Split column families: TRUE (fragments will be generated)")
            else:
                print(f"   Split column families: FALSE (single file per event)")
            print(f"   Destination: Azure Blob Storage")
            print()
            
            # Wait for files to appear
            if wait_for_files:
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
            
            return {
                'created': True,
                'job_id': job_id,
                'existing_count': 0,
                'existing_jobs': [],
                'duplicate_warning': False
            }


def get_existing_changefeeds(
    conn,
    config
) -> List[Tuple[int, str, str]]:
    """
    Get existing changefeeds for a given source → target mapping.
    
    Args:
        conn: pg8000 connection object
        config: Config dataclass from cockroachdb_config.py
    
    Returns:
        List of tuples: [(job_id, status, sink_uri), ...]
    
    Example:
        >>> changefeeds = get_existing_changefeeds(conn, config)
        >>> print(f"Found {len(changefeeds)} existing changefeeds")
        >>> for job_id, status, sink_uri in changefeeds:
        >>>     print(f"Job {job_id}: {status}")
    """
    sink_uri_pattern = f"%{config.azure_storage.container_name}/{config.cdc_config.path}%"
    
    with conn.cursor() as cur:
        cur.execute("""
            SELECT job_id, status, sink_uri
            FROM [SHOW CHANGEFEED JOBS] 
            WHERE sink_uri LIKE %s
            AND status IN ('running', 'paused')
        """, (sink_uri_pattern,))
        
        return cur.fetchall()


def cancel_changefeeds(
    conn,
    config,
    verbose: bool = True
) -> Dict[str, Any]:
    """
    Cancel all changefeeds for a given source → target mapping.
    
    Args:
        conn: pg8000 connection object
        config: Config dataclass from cockroachdb_config.py
        verbose: If True, print detailed output (default: True)
    
    Returns:
        dict with keys:
            - cancelled_count: int - Number of changefeeds cancelled
            - job_ids: List[int] - List of cancelled job IDs
    
    Example:
        >>> result = cancel_changefeeds(conn, config)
        >>> print(f"Cancelled {result['cancelled_count']} changefeed(s)")
    """
    # Get existing changefeeds
    changefeeds = get_existing_changefeeds(conn, config)
    
    if not changefeeds:
        if verbose:
            print("ℹ️  No changefeeds found to cancel")
        return {
            'cancelled_count': 0,
            'job_ids': []
        }
    
    if verbose:
        print(f"🗑️  Cancelling {len(changefeeds)} changefeed(s)...")
    
    cancelled_job_ids = []
    
    with conn.cursor() as cur:
        for job_id, status, sink_uri in changefeeds:
            cur.execute(f"CANCEL JOB {job_id}")
            cancelled_job_ids.append(job_id)
            
            if verbose:
                print(f"   ✅ Cancelled Job ID: {job_id}")
                print(f"      Sink URI: {sink_uri[:80]}...")
        
        if verbose and len(changefeeds) > 1:
            print(f"\n💡 Tip: Multiple changefeeds may have caused duplicate data")
    
    return {
        'cancelled_count': len(cancelled_job_ids),
        'job_ids': cancelled_job_ids
    }


def drop_table(
    conn,
    table_name: str,
    cascade: bool = True,
    verbose: bool = True
) -> None:
    """
    Drop a table from CockroachDB.
    
    Args:
        conn: pg8000 connection object
        table_name: Table name to drop (e.g., 'usertable_append_only_single_cf')
        cascade: If True, use CASCADE option (default: True)
        verbose: If True, print output (default: True)
    
    Example:
        >>> drop_table(conn, config.tables.source_table_name)
        >>> # ✅ Table 'usertable_append_only_single_cf' dropped from CockroachDB
    """
    cascade_clause = "CASCADE" if cascade else ""
    
    with conn.cursor() as cur:
        cur.execute(f"DROP TABLE IF EXISTS {table_name} {cascade_clause}")
        conn.commit()
    
    if verbose:
        print(f"✅ Table '{table_name}' dropped from CockroachDB")


def show_all_changefeeds(
    conn,
    verbose: bool = True
) -> List[Tuple]:
    """
    Show all changefeeds in the database (regardless of status).
    
    Args:
        conn: pg8000 connection object
        verbose: If True, print formatted output (default: True)
    
    Returns:
        List of tuples with changefeed information
    
    Example:
        >>> changefeeds = show_all_changefeeds(conn)
        >>> print(f"Total changefeeds: {len(changefeeds)}")
    """
    with conn.cursor() as cur:
        cur.execute("SELECT job_id, status, sink_uri, description FROM [SHOW CHANGEFEED JOBS]")
        changefeeds = cur.fetchall()
    
    if verbose:
        if not changefeeds:
            print("ℹ️  No changefeeds found")
        else:
            print(f"📊 All Changefeeds ({len(changefeeds)} total):")
            print("=" * 80)
            for job_id, status, sink_uri, description in changefeeds:
                print(f"Job ID: {job_id}")
                print(f"Status: {status}")
                print(f"Sink URI: {sink_uri[:80]}...")
                print(f"Description: {description[:60]}...")
                print("-" * 80)
    
    return changefeeds


def cancel_all_changefeeds(
    conn,
    verbose: bool = True
) -> Dict[str, Any]:
    """
    Cancel ALL changefeeds in the database (use with caution!).
    
    Args:
        conn: pg8000 connection object
        verbose: If True, print detailed output (default: True)
    
    Returns:
        dict with keys:
            - cancelled_count: int - Number of changefeeds cancelled
            - job_ids: List[int] - List of cancelled job IDs
    
    Warning:
        This cancels ALL changefeeds, not just ones matching config.
        Use cancel_changefeeds() instead for targeted cancellation.
    
    Example:
        >>> result = cancel_all_changefeeds(conn)
        >>> print(f"Cancelled {result['cancelled_count']} changefeed(s)")
    """
    with conn.cursor() as cur:
        # Get all running/paused changefeeds
        cur.execute("""
            SELECT job_id, status, sink_uri
            FROM [SHOW CHANGEFEED JOBS] 
            WHERE status IN ('running', 'paused')
        """)
        changefeeds = cur.fetchall()
    
    if not changefeeds:
        if verbose:
            print("ℹ️  No changefeeds found to cancel")
        return {
            'cancelled_count': 0,
            'job_ids': []
        }
    
    if verbose:
        print(f"⚠️  Cancelling ALL {len(changefeeds)} changefeed(s)...")
    
    cancelled_job_ids = []
    
    with conn.cursor() as cur:
        for job_id, status, sink_uri in changefeeds:
            cur.execute(f"CANCEL JOB {job_id}")
            cancelled_job_ids.append(job_id)
            
            if verbose:
                print(f"   ✅ Cancelled Job ID: {job_id} ({status})")
    
    if verbose:
        print(f"\n✅ Cancelled {len(cancelled_job_ids)} changefeed(s)")
    
    return {
        'cancelled_count': len(cancelled_job_ids),
        'job_ids': cancelled_job_ids
    }
