"""
CockroachDB CDC Unity Catalog Volume Utilities

This module provides Unity Catalog Volume utilities for CockroachDB CDC changefeeds.

This module maintains the same function signatures as cockroachdb_azure.py to enable
easy addition of other cloud providers (AWS S3, GCP GCS, Cloudflare R2, etc.) in the future.

Design Pattern:
    - Same function signatures across all cloud/storage providers
    - Provider-specific implementation details hidden
    - Easy to swap between providers or add new ones
    
Example:
    # Azure
    from cockroachdb_azure import check_azure_files, wait_for_changefeed_files
    
    # Unity Catalog Volume
    from cockroachdb_uc_volume import check_volume_files, wait_for_changefeed_files
    
    # Future: AWS S3
    from cockroachdb_s3 import check_s3_files, wait_for_changefeed_files
"""

import time
from typing import Dict, Any, List, Optional


def check_volume_files(
    volume_path: str,
    source_catalog: str,
    source_schema: str,
    source_table: str,
    target_table: str,
    spark,
    verbose: bool = True,
    format: str = "parquet"
) -> Dict[str, Any]:
    """
    Check for changefeed files in Unity Catalog Volume.
    
    This function has the same signature pattern as check_azure_files() to enable
    easy addition of other cloud providers in the future.
    
    Args:
        volume_path: Unity Catalog Volume path (e.g., '/Volumes/main/default/cdc')
        source_catalog: CockroachDB catalog (database)
        source_schema: CockroachDB schema
        source_table: Source table name
        target_table: Target table name
        spark: SparkSession (required for Unity Catalog operations)
        verbose: Print detailed output
        format: Changefeed format (default: "parquet")
    
    Returns:
        dict with 'data_files' and 'resolved_files' lists
        
        data_files: List of file info dicts with keys:
            - name: filename
            - path: full path
            - size: file size in bytes
        
        resolved_files: List of RESOLVED file info dicts (same structure)
    
    Example:
        >>> result = check_volume_files(
        ...     volume_path="/Volumes/main/default/cdc",
        ...     source_catalog="defaultdb",
        ...     source_schema="public",
        ...     source_table="usertable",
        ...     target_table="usertable_append_only",
        ...     spark=spark
        ... )
        >>> print(f"Found {len(result['data_files'])} data files")
    """
    # Build path - same structure as Azure blob storage
    # Format: {format}/{source_catalog}/{source_schema}/{source_table}/{target_table}/
    path_prefix = f"{format}/{source_catalog}/{source_schema}/{source_table}/{target_table}/"
    full_path = f"{volume_path.rstrip('/')}/{path_prefix}"
    
    # Use Spark to list files in parallel
    if verbose:
        print(f"   🚀 Using Spark for fast parallel file listing...")
    
    # Use Spark's file listing which is parallelized
    from pyspark.sql.utils import AnalysisException
    
    try:
        if verbose:
            print(f"   ⏳ Spark: Reading directory structure...")
        
        # Try to read the directory structure using Spark
        file_df = spark.read.format("binaryFile").option("recursiveFileLookup", "true").load(full_path)
        
        if verbose:
            print(f"   ⏳ Spark: Collecting file paths...")
        
        file_paths = [row.path for row in file_df.select("path").collect()]
        
        if verbose:
            print(f"   ✅ Spark: Found {len(file_paths)} total paths")
        
        # Convert to file info dicts
        all_files = []
        for path in file_paths:
            filename = path.split('/')[-1]
            
            # Skip metadata directory and underscore files (but NOT .RESOLVED)
            if '/_metadata/' in path or (filename.startswith('_') and '.RESOLVED' not in filename):
                continue
            
            # Include data files AND .RESOLVED files
            if (filename.endswith('.parquet') or filename.endswith('.json') or 
                filename.endswith('.ndjson') or '.RESOLVED' in filename):
                all_files.append({
                    'name': filename,
                    'path': path,
                    'size': 0  # Size not available from binaryFile format
                })
                
    except AnalysisException as e:
        # Directory doesn't exist or is empty - treat as empty directory
        if verbose:
            print(f"   ℹ️  Directory not found or empty: {full_path}")
        all_files = []
    except Exception as e:
        # Other errors - fail with exception
        raise RuntimeError(
            f"Failed to list files in Unity Catalog Volume path: {full_path}\n"
            f"Error: {e}"
        ) from e
    
    # Categorize files (same logic as Azure module)
    # Data files: .parquet/.json files, excluding:
    #   - .RESOLVED files (CDC watermarks)
    #   - _metadata/ directory (schema files)
    #   - Files starting with _ (_SUCCESS, _committed_*, etc.)
    data_files = [
        f for f in all_files 
        if (f['name'].endswith('.parquet') or f['name'].endswith('.json') or f['name'].endswith('.ndjson'))
        and '.RESOLVED' not in f['name']
        and '/_metadata/' not in f['path']
        and not f['name'].startswith('_')
    ]
    
    resolved_files = [f for f in all_files if '.RESOLVED' in f['name']]
    
    if verbose:
        print(f"📁 Files in Unity Catalog Volume:")
        print(f"   Path: {full_path}")
        print(f"   📄 Data files: {len(data_files)}")
        print(f"   🕐 Resolved files: {len(resolved_files)}")
        print(f"   📊 Total: {len(all_files)}")
        
        if data_files:
            print(f"\n   Example data file:")
            print(f"   {data_files[0]['name']}")
    
    return {
        'data_files': data_files,
        'resolved_files': resolved_files,
        'total': len(all_files)
    }


def wait_for_changefeed_files(
    volume_path: str,
    source_catalog: str,
    source_schema: str,
    source_table: str,
    target_table: str,
    spark,
    max_wait: int = 120,
    check_interval: int = 5,
    stabilization_wait: Optional[int] = None,
    format: str = "parquet",
    wait_for_resolved: bool = True
) -> Dict[str, Any]:
    """
    Wait for changefeed files to appear in Unity Catalog Volume with timeout.
    
    This function has the same signature pattern as the Azure version to enable
    easy addition of other cloud providers in the future.
    
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
        volume_path: Unity Catalog Volume path (e.g., '/Volumes/main/default/cdc')
        source_catalog: CockroachDB catalog (database)
        source_schema: CockroachDB schema
        source_table: Source table name
        target_table: Target table name
        spark: SparkSession (required for Unity Catalog operations)
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
        ...     volume_path="/Volumes/main/default/cdc",
        ...     source_catalog="defaultdb",
        ...     source_schema="public",
        ...     source_table="usertable",
        ...     target_table="usertable",
        ...     spark=spark,
        ...     max_wait=300,
        ...     wait_for_resolved=True  # ✅ Wait for RESOLVED
        ... )
        >>> if result['success']:
        ...     print(f"RESOLVED file: {result['resolved_file']}")
        ...     # Use for watermark coordination
    """
    if wait_for_resolved:
        # ====================================================================
        # RESOLVED MODE: Wait for .RESOLVED file (RECOMMENDED)
        # ====================================================================
        print(f"⏳ Waiting for RESOLVED file to appear in Unity Catalog Volume...")
        print(f"   This ensures all CDC events and column family fragments are complete")
        print(f"   No stabilization wait needed - RESOLVED guarantees completeness")
        
        elapsed = 0
        resolved_file = None
        
        while elapsed < max_wait:
            result = check_volume_files(
                volume_path, source_catalog, source_schema, source_table, target_table,
                spark, verbose=False, format=format
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
        
        print(f"⏳ Waiting for initial snapshot files to appear in Unity Catalog Volume...")
        print(f"   (Legacy mode - consider using wait_for_resolved=True)")
        print(f"   Using stabilization wait: {stabilization_wait}s")
        
        elapsed = 0
        files_found = False
        last_file_count = 0
        stable_elapsed = 0
        
        while elapsed < max_wait:
            result = check_volume_files(
                volume_path, source_catalog, source_schema, source_table, target_table,
                spark, verbose=False, format=format
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
        print(f"\n⚠️  Timeout after {max_wait}s - no files appeared")
        print(f"   Check:")
        print(f"   1. Changefeed is running")
        print(f"   2. Files are being written to volume")
        print(f"   3. Path is correct: {volume_path}/{format}/{source_catalog}/{source_schema}/{source_table}/{target_table}/")
        
        return {
            'success': False,
            'resolved_file': None,
            'elapsed_time': elapsed,
            'file_count': 0
        }


# ============================================================================
# Future Extension Example (Template for AWS S3, GCP GCS, Cloudflare R2, etc.)
# ============================================================================
"""
To add a new cloud provider, create cockroachdb_<provider>.py with these functions:

def check_<provider>_files(
    <provider_specific_params>,  # e.g., bucket_name, access_key for S3
    source_catalog: str,
    source_schema: str,
    source_table: str,
    target_table: str,
    verbose: bool = True,
    format: str = "parquet"
) -> Dict[str, Any]:
    # Provider-specific implementation
    # Returns same structure: {'data_files': [...], 'resolved_files': [...], 'total': int}
    pass

def wait_for_changefeed_files(
    <provider_specific_params>,
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
    # Provider-specific implementation
    # Returns same structure: {'success': bool, 'resolved_file': str, 'elapsed_time': int, 'file_count': int}
    pass

Examples:
- cockroachdb_s3.py (AWS S3)
- cockroachdb_gcs.py (Google Cloud Storage)
- cockroachdb_r2.py (Cloudflare R2)
- cockroachdb_minio.py (MinIO)
"""
