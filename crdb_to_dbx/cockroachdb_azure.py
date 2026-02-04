"""
CockroachDB CDC Azure Utilities

This module provides Azure Blob Storage utilities for CockroachDB CDC changefeeds.
"""

import time
from typing import Dict, Any, List, Optional
from azure.storage.blob import BlobServiceClient


def check_azure_files(
    storage_account_name: str,
    storage_account_key: str,
    container_name: str,
    source_catalog: str,
    source_schema: str,
    source_table: str,
    target_table: str,
    verbose: bool = True,
    format: str = "parquet"
) -> Dict[str, Any]:
    """
    Check for changefeed files in Azure Blob Storage.
    
    Args:
        storage_account_name: Azure storage account name
        storage_account_key: Azure storage account key
        container_name: Azure container name
        source_catalog: CockroachDB catalog (database)
        source_schema: CockroachDB schema
        source_table: Source table name
        target_table: Target table name
        verbose: Print detailed output
        format: Changefeed format (default: "parquet")
    
    Returns:
        dict with 'data_files' and 'resolved_files' lists
    
    Example:
        >>> result = check_azure_files(
        ...     storage_account_name="mystorageaccount",
        ...     storage_account_key="<key>",
        ...     container_name="cockroachcdc",
        ...     source_catalog="defaultdb",
        ...     source_schema="public",
        ...     source_table="usertable",
        ...     target_table="usertable_append_only_multi_cf"
        ... )
        >>> print(f"Found {len(result['data_files'])} data files")
    """
    # Connect to Azure
    connection_string = f"DefaultEndpointsProtocol=https;AccountName={storage_account_name};AccountKey={storage_account_key};EndpointSuffix=core.windows.net"
    blob_service = BlobServiceClient.from_connection_string(connection_string)
    container_client = blob_service.get_container_client(container_name)
    
    # Build path - list ALL files recursively under this changefeed path
    prefix = f"{format}/{source_catalog}/{source_schema}/{source_table}/{target_table}/"
    
    # List all blobs recursively (no date filtering)
    blobs = list(container_client.list_blobs(name_starts_with=prefix))
    
    # Convert blobs to standard dict format (same as UC Volume)
    # This ensures consistent format across all storage backends
    all_files = [
        {
            'name': b.name,
            'path': b.name,  # Azure blobs use 'name' as the full path
            'size': b.size
        }
        for b in blobs
    ]
    
    # Categorize files (using same filtering logic as cockroachdb.py)
    # Data files: .parquet files, excluding:
    #   - .RESOLVED files (CDC watermarks)
    #   - _metadata/ directory (schema files)
    #   - Files starting with _ (_SUCCESS, _committed_*, etc.)
    data_files = [
        f for f in all_files 
        if f['name'].endswith('.parquet') 
        and '.RESOLVED' not in f['name']
        and '/_metadata/' not in f['name']
        and not f['name'].split('/')[-1].startswith('_')
    ]
    resolved_files = [f for f in all_files if '.RESOLVED' in f['name']]
    
    if verbose:
        print(f"📁 Files in Azure changefeed path:")
        print(f"   Path: {prefix}")
        print(f"   📄 Data files: {len(data_files)}")
        print(f"   🕐 Resolved files: {len(resolved_files)}")
        print(f"   📊 Total: {len(blobs)}")
        
        if data_files:
            print(f"\n   Example data file:")
            print(f"   {data_files[0]['name']}")
    
    return {
        'data_files': data_files,
        'resolved_files': resolved_files,
        'total': len(blobs)
    }


def wait_for_changefeed_files(
    storage_account_name: str,
    storage_account_key: str,
    container_name: str,
    source_catalog: str,
    source_schema: str,
    source_table: str,
    target_table: str,
    max_wait: int = 120,
    check_interval: int = 5,
    stabilization_wait: Optional[int] = None,
    format: str = "parquet",
    wait_for_resolved: bool = True
) -> Dict[str, Any]:
    """
    Wait for changefeed files to appear in Azure with timeout.
    
    This function can operate in two modes:
    1. RESOLVED mode (wait_for_resolved=True): Waits for .RESOLVED file to appear ✅ RECOMMENDED
       - CRITICAL for column family completeness guarantee
       - Returns the RESOLVED filename for coordination
       - No stabilization wait needed (RESOLVED guarantees completeness)
       - Recommended for production multi-CF tables
    
    2. Data file mode (wait_for_resolved=False): Waits for data files with stabilization
       - Legacy mode for backward compatibility
       - Uses stabilization_wait to detect when all files have landed
       - Not recommended for production (use RESOLVED mode instead)
    
    Args:
        storage_account_name: Azure storage account name
        storage_account_key: Azure storage account key
        container_name: Azure container name
        source_catalog: CockroachDB catalog (database)
        source_schema: CockroachDB schema
        source_table: Source table name
        target_table: Target table name
        max_wait: Maximum seconds to wait for files (default: 120)
        check_interval: Seconds between checks (default: 5)
        stabilization_wait: Seconds to wait for file count to stabilize (default: None)
                           - If None: Defaults to 5s in legacy mode, unused in RESOLVED mode
                           - Only used when wait_for_resolved=False
                           - Ignored in RESOLVED mode (not needed)
        format: Changefeed format (default: "parquet")
        wait_for_resolved: If True, wait for RESOLVED file (recommended, default)
                          If False, wait for data files (legacy mode)
    
    Returns:
        dict with:
        - 'success': bool - True if files/RESOLVED found
        - 'resolved_file': str or None - RESOLVED filename if wait_for_resolved=True
        - 'elapsed_time': int - Total seconds waited
        - 'file_count': int - Number of files found
    
    Example (RESOLVED mode - Recommended):
        >>> result = wait_for_changefeed_files(
        ...     storage_account_name="mystorageaccount",
        ...     storage_account_key="<key>",
        ...     container_name="cockroachcdc",
        ...     source_catalog="defaultdb",
        ...     source_schema="public",
        ...     source_table="usertable",
        ...     target_table="usertable",
        ...     max_wait=300,
        ...     wait_for_resolved=True  # ✅ Wait for RESOLVED
        ... )
        >>> if result['success']:
        ...     print(f"RESOLVED file: {result['resolved_file']}")
        ...     # Use for watermark coordination
    
    Example (Legacy mode):
        >>> result = wait_for_changefeed_files(
        ...     storage_account_name="mystorageaccount",
        ...     storage_account_key="<key>",
        ...     container_name="cockroachcdc",
        ...     source_catalog="defaultdb",
        ...     source_schema="public",
        ...     source_table="usertable",
        ...     target_table="usertable",
        ...     wait_for_resolved=False  # Legacy data file mode
        ... )
    """
    if wait_for_resolved:
        # ====================================================================
        # RESOLVED MODE: Wait for .RESOLVED file (RECOMMENDED)
        # ====================================================================
        print(f"⏳ Waiting for RESOLVED file to appear in Azure...")
        print(f"   This ensures all CDC events and column family fragments are complete")
        print(f"   No stabilization wait needed - RESOLVED guarantees completeness")
        
        elapsed = 0
        resolved_file = None
        
        while elapsed < max_wait:
            result = check_azure_files(
                storage_account_name, storage_account_key, container_name,
                source_catalog, source_schema, source_table, target_table,
                verbose=False,
                format=format
            )
            
            resolved_files = result['resolved_files']
            
            if resolved_files:
                # RESOLVED file found!
                resolved_file = resolved_files[-1]['name']  # Get latest RESOLVED file
                data_file_count = len(result['data_files'])
                
                print(f"\n✅ RESOLVED file found after {elapsed} seconds!")
                print(f"   RESOLVED file: {resolved_file}")
                print(f"   Data files: {data_file_count}")
                print(f"   💡 All CDC events up to this RESOLVED timestamp are complete")
                
                return {
                    'success': True,
                    'resolved_file': resolved_file,
                    'elapsed_time': elapsed,
                    'file_count': data_file_count
                }
            
            print(f"   Checking... ({elapsed}s elapsed)", end='\r')
            time.sleep(check_interval)
            elapsed += check_interval
        
        # Timeout
        print(f"\n⚠️  Timeout after {max_wait}s - no RESOLVED file appeared")
        print(f"   This may indicate:")
        print(f"   1. Changefeed has not written RESOLVED file yet (increase max_wait)")
        print(f"   2. Changefeed not configured with 'resolved' option")
        print(f"   3. Path or table name mismatch")
        
        return {
            'success': False,
            'resolved_file': None,
            'elapsed_time': elapsed,
            'file_count': 0
        }
    
    else:
        # ====================================================================
        # LEGACY MODE: Wait for data files with stabilization
        # ====================================================================
        # Set default stabilization_wait for legacy mode
        if stabilization_wait is None:
            stabilization_wait = 5  # Default 5 seconds for legacy mode
        
        print(f"⏳ Waiting for initial snapshot files to appear in Azure...")
        print(f"   (Legacy mode - consider using wait_for_resolved=True)")
        print(f"   Using stabilization wait: {stabilization_wait}s")
        
        elapsed = 0
        files_found = False
        last_file_count = 0
        stable_elapsed = 0
        
        while elapsed < max_wait:
            result = check_azure_files(
                storage_account_name, storage_account_key, container_name,
                source_catalog, source_schema, source_table, target_table,
                verbose=False,
                format=format
            )
            
            current_file_count = len(result['data_files'])
            
            if not files_found and current_file_count > 0:
                # First files detected - switch to stabilization mode
                files_found = True
                last_file_count = current_file_count
                stable_elapsed = 0
                print(f"\n✅ First files appeared after {elapsed} seconds!")
                print(f"   Found {current_file_count} file(s) so far...")
                print(f"   Waiting {stabilization_wait}s for more files (column family fragments)...")
            
            elif files_found:
                # In stabilization mode - check if file count is stable
                if current_file_count > last_file_count:
                    # More files arrived - reset stabilization timer
                    print(f"   📄 File count increased: {last_file_count} → {current_file_count}")
                    last_file_count = current_file_count
                    stable_elapsed = 0
                else:
                    # File count unchanged - increment stabilization timer
                    stable_elapsed += check_interval
                    
                    if stable_elapsed >= stabilization_wait:
                        # Stabilization period complete - all files have landed
                        print(f"\n✅ File count stable at {current_file_count} for {stabilization_wait}s")
                        print(f"   Total wait time: {elapsed + stable_elapsed}s")
                        print(f"   Example: {result['data_files'][0]['name']}")
                        
                        return {
                            'success': True,
                            'resolved_file': None,
                            'elapsed_time': elapsed + stable_elapsed,
                            'file_count': current_file_count
                        }
            
            if not files_found:
                print(f"   Checking... ({elapsed}s elapsed)", end='\r')
            
            time.sleep(check_interval)
            elapsed += check_interval
        
        # Timeout
        if files_found:
            print(f"\n⚠️  Timeout after {max_wait}s (found {last_file_count} files but more may still be generating)")
        else:
            print(f"\n⚠️  Timeout after {max_wait}s - no files appeared")
        
        return {
            'success': files_found,
            'resolved_file': None,
            'elapsed_time': elapsed,
            'file_count': last_file_count
        }


def delete_changefeed_files(
    storage_account_name: str,
    storage_account_key: str,
    container_name: str,
    changefeed_path: str,
    verbose: bool = True
) -> Dict[str, Any]:
    """
    Delete all changefeed files in Azure Blob Storage for a given path.
    
    ⚠️  WARNING: This permanently deletes all CDC data at the specified path!
    
    Use this when:
    - Starting completely fresh
    - Old data from previous runs is causing sync issues
    - Table schema changed (e.g., VARCHAR → INT)
    
    Args:
        storage_account_name: Azure storage account name
        storage_account_key: Azure storage account key
        container_name: Azure container name
        changefeed_path: Path to delete (e.g., "parquet/defaultdb/public/usertable/target_table")
                        Can also use config.cdc_config.path from Config dataclass
        verbose: Print detailed output (default: True)
    
    Returns:
        dict with keys:
            - deleted_count: Number of items deleted
            - failed_count: Number of items that failed to delete
            - data_files_deleted: Number of .parquet data files deleted
            - resolved_files_deleted: Number of .RESOLVED files deleted
            - directories_deleted: Number of directory markers deleted
    
    Example:
        >>> # Using config dataclass
        >>> result = delete_changefeed_files(
        ...     storage_account_name=config.azure_storage.account_name,
        ...     storage_account_key=config.azure_storage.account_key,
        ...     container_name=config.azure_storage.container_name,
        ...     changefeed_path=config.cdc_config.path
        ... )
        >>> print(f"Deleted {result['deleted_count']} items")
        
        >>> # Manual path
        >>> result = delete_changefeed_files(
        ...     storage_account_name="mystorageaccount",
        ...     storage_account_key="<key>",
        ...     container_name="cockroachcdc",
        ...     changefeed_path="parquet/defaultdb/public/usertable/target_table"
        ... )
    """
    # Ensure path ends with / for prefix matching
    if not changefeed_path.endswith('/'):
        changefeed_path = f"{changefeed_path}/"
    
    if verbose:
        print(f"🗑️  Deleting Azure changefeed data...")
        print(f"=" * 80)
        print(f"Container: {container_name}")
        print(f"Path: {changefeed_path}")
        print()
    
    # Connect to Azure
    connection_string = f"DefaultEndpointsProtocol=https;AccountName={storage_account_name};AccountKey={storage_account_key};EndpointSuffix=core.windows.net"
    blob_service = BlobServiceClient.from_connection_string(connection_string)
    container_client = blob_service.get_container_client(container_name)
    
    # List all blobs with this prefix
    if verbose:
        print(f"🔍 Scanning for files...")
    
    blobs = list(container_client.list_blobs(name_starts_with=changefeed_path))
    
    if not blobs:
        if verbose:
            print(f"ℹ️  No files found at: {changefeed_path}")
            print(f"   Files may have already been deleted, or path is incorrect")
        return {
            'deleted_count': 0,
            'failed_count': 0,
            'data_files_deleted': 0,
            'resolved_files_deleted': 0,
            'directories_deleted': 0
        }
    
    # Categorize items
    data_files = [b for b in blobs if b.size > 0 and '.parquet' in b.name and '.RESOLVED' not in b.name]
    resolved_files = [b for b in blobs if '.RESOLVED' in b.name]
    directories = [b for b in blobs if b.size == 0]
    
    if verbose:
        print(f"✅ Found {len(blobs)} items to delete")
        print(f"   📄 Data files: {len(data_files)}")
        print(f"   🕐 Resolved files: {len(resolved_files)}")
        print(f"   📁 Directories: {len(directories)}")
        print()
    
    # Delete all blobs
    if verbose:
        print(f"🔄 Deleting {len(blobs)} items...")
    
    deleted = 0
    failed = 0
    
    for blob in blobs:
        try:
            container_client.delete_blob(blob.name)
            deleted += 1
            if verbose and deleted % 50 == 0:
                print(f"   Deleted {deleted}/{len(blobs)} items...", end='\r')
        except Exception as e:
            # Some errors are expected (e.g., directories already removed, blob not found)
            error_str = str(e)
            if "DirectoryIsNotEmpty" not in error_str and "BlobNotFound" not in error_str:
                failed += 1
                if verbose:
                    print(f"\n   ⚠️  Failed: {blob.name[:60]}... - {e}")
    
    if verbose:
        print(f"✅ Deleted {deleted} items from Azure                    ")
        if failed > 0:
            print(f"   ⚠️  Failed to delete {failed} items")
        print()
        print(f"=" * 80)
        print(f"✅ Cleanup complete!")
    
    return {
        'deleted_count': deleted,
        'failed_count': failed,
        'data_files_deleted': len(data_files),
        'resolved_files_deleted': len(resolved_files),
        'directories_deleted': len(directories)
    }
