"""
CockroachDB CDC Auto Loader Functions

These are the original 4 CDC ingestion functions that were previously defined 
in the notebook but were later removed. This file restores them from the backup
(cockroachdb-cdc-tutorial-2-after-dup-removal.ipynb).

History:
- These functions existed in the notebook's Cell 6
- They were later deleted/removed
- This file recreates them based on the backup file

Note: merge_column_family_fragments() has been updated with the latest version
from cockroachdb.py that includes the deduplicate_to_latest_state parameter
for robust NULL handling in column families.
"""

from typing import List, Optional
from pyspark.sql import functions as F
from datetime import datetime


# ============================================================================
# HELPER FUNCTIONS - ALL MODES
# ============================================================================

def _build_paths(config, mode_suffix=""):
    """
    Build source, checkpoint, and target paths from config.
    
    Supports both Azure storage and UC external volume based on data_source configuration.
    
    Args:
        config: Config dataclass from cockroachdb_config.py
        mode_suffix: Optional suffix for checkpoint path (e.g., "_merge", "_merge_cf")
    
    Returns:
        tuple: (source_path, checkpoint_path, target_table_fqn)
    
    Raises:
        ValueError: If data source is not properly configured
    """
    # Import here to avoid circular dependency
    from cockroachdb_config import get_storage_path
    
    # Get source path based on data source
    source_path = get_storage_path(config)
    
    checkpoint_path = f"/checkpoints/{config.tables.destination_schema}_{config.tables.destination_table_name}{mode_suffix}"
    target_table_fqn = f"{config.tables.destination_catalog}.{config.tables.destination_schema}.{config.tables.destination_table_name}"
    return source_path, checkpoint_path, target_table_fqn


def _setup_autoloader(spark, source_path, checkpoint_path, source_table):
    """
    Set up Spark Auto Loader for CDC files.
    
    Automatically detects storage type (Azure vs UC Volume) and applies appropriate settings:
    - UC Volumes: Use directory listing (notifications not supported)
    - Azure: Can use notifications or directory listing
    
    Args:
        spark: SparkSession
        source_path: Storage path to CDC files (Azure abfss:// or UC Volume /Volumes/)
        checkpoint_path: Checkpoint location for schema tracking
        source_table: Source table name for filtering files
    
    Returns:
        DataFrame: Streaming DataFrame with raw CDC data
    """
    # Detect if we're using UC Volume (path starts with /Volumes/)
    is_uc_volume = source_path.startswith("/Volumes/")
    
    reader = (spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option("cloudFiles.schemaLocation", f"{checkpoint_path}/schema")
        .option("pathGlobFilter", f"*{source_table}*.parquet")
        .option("recursiveFileLookup", "true")
    )
    
    # For UC Volumes, explicitly disable notifications (not supported)
    # and use directory listing instead
    if is_uc_volume:
        reader = reader.option("cloudFiles.useNotifications", "false")
    
    return reader.load(source_path)


def _transform_cdc_columns(raw_df, resolved_watermark: Optional[int] = None):
    """
    Transform CockroachDB CDC columns to standard format.
    
    Converts:
    - __crdb__updated (nanoseconds) → _cdc_timestamp (timestamp)
    - __crdb__event_type ('d'/'c'/'r') → _cdc_operation ('DELETE'/'UPSERT')
    
    Optionally filters by RESOLVED timestamp watermark to guarantee completeness.
    
    Args:
        raw_df: Raw DataFrame from Auto Loader
        resolved_watermark: Optional RESOLVED timestamp in nanoseconds.
                           If provided, only events with __crdb__updated <= watermark are kept.
                           This GUARANTEES all column family fragments have arrived.
    
    Returns:
        DataFrame: Transformed DataFrame with standard CDC columns
    """
    # Helper function to extract wall time from CockroachDB HLC timestamp
    # __crdb__updated format: "1770067697320026017.0000000002" (wall_time.logical)
    # We extract just the wall_time part (before decimal) for filtering and conversion
    def extract_wall_time(col):
        """Extract wall time nanoseconds from CockroachDB HLC timestamp string."""
        return F.split(F.col(col), "\\.")[0].cast("bigint")
    
    df = raw_df.select(
        "*",
        # Convert __crdb__updated (nanoseconds) to timestamp
        # Extract wall time from HLC format (before decimal point)
        F.from_unixtime(
            extract_wall_time("__crdb__updated") / 1000000000
        ).cast("timestamp").alias("_cdc_timestamp"),
        # Map event type
        F.when(F.col("__crdb__event_type") == "d", "DELETE")
         .otherwise("UPSERT")
         .alias("_cdc_operation")
    ).drop("__crdb__event_type", "__crdb__updated")
    
    # Apply RESOLVED watermark filtering if provided
    if resolved_watermark is not None:
        print(f"   🔒 Applying RESOLVED watermark filter: timestamp ≤ {resolved_watermark}")
        # Note: raw_df still has __crdb__updated before drop, so filter on original df
        # Extract wall time and compare with watermark
        df = raw_df.filter(extract_wall_time("__crdb__updated") <= resolved_watermark).select(
            "*",
            F.from_unixtime(
                extract_wall_time("__crdb__updated") / 1000000000
            ).cast("timestamp").alias("_cdc_timestamp"),
            F.when(F.col("__crdb__event_type") == "d", "DELETE")
             .otherwise("UPSERT")
             .alias("_cdc_operation")
        ).drop("__crdb__event_type", "__crdb__updated")
        print(f"   ✅ RESOLVED watermark applied - only complete data will be processed")
    
    return df


def _print_ingestion_header(config, mode, column_family_mode, source_path, target_table_fqn):
    """
    Print standard ingestion header with configuration details.
    
    Args:
        config: Config dataclass
        mode: CDC mode ('append_only' or 'update_delete')
        column_family_mode: Column family mode ('single_cf' or 'multi_cf')
        source_path: Azure blob storage path
        target_table_fqn: Fully qualified target table name
    """
    mode_descriptions = {
        ("append_only", "single_cf"): "APPEND-ONLY (Single Column Family)",
        ("append_only", "multi_cf"): "APPEND-ONLY (Multi Column Family)",
        ("update_delete", "single_cf"): "MERGE (Apply UPDATE/DELETE)",
        ("update_delete", "multi_cf"): "MERGE with Column Families"
    }
    
    mode_desc = mode_descriptions.get((mode, column_family_mode), "Unknown Mode")
    
    print("📖 Ingesting CDC events")
    print("=" * 80)
    print(f"Mode: {mode_desc}")
    print(f"Source: {config.tables.source_catalog}.{config.tables.source_schema}.{config.tables.source_table_name} (CockroachDB)")
    print(f"Target: {target_table_fqn} (Databricks Delta)")
    print(f"Source path: {source_path}/ (all dates, recursively)")
    print(f"File filter: *{config.tables.source_table_name}*.parquet")
    print(f"   ✅ Includes: Data files")
    print(f"   ❌ Excludes: .RESOLVED, _metadata/, _SUCCESS, etc.")
    print()


def _get_resolved_watermark(config, source_table: str, spark=None) -> Optional[int]:
    """
    Get the latest RESOLVED timestamp watermark from CockroachDB CDC .RESOLVED files.
    
    RESOLVED files guarantee that all CDC events with timestamp ≤ RESOLVED have been written.
    This is CRITICAL for column family completeness - ensures all fragments have arrived.
    
    Supports both Azure Blob Storage and Unity Catalog external volumes.
    
    How it works:
    1. Reads all .RESOLVED files for the source table from storage (Azure or UC Volume)
    2. Extracts the resolved timestamp from the filename (nanoseconds since epoch)
    3. Returns the MAXIMUM resolved timestamp (latest safe watermark)
    
    RESOLVED filename format: <table>.RESOLVED.<nanos>
    Example: ycsb.RESOLVED.1704067200000000000
    
    Args:
        config: Configuration object with storage credentials
        source_table: Source table name (e.g., "ycsb")
        spark: SparkSession (required for UC Volume, optional for Azure)
    
    Returns:
        int: Latest RESOLVED timestamp in nanoseconds, or None if no RESOLVED files found
    
    Example:
        >>> watermark = _get_resolved_watermark(config, "ycsb", spark)
        >>> if watermark:
        >>>     print(f"Safe to process events up to {watermark} nanos")
        >>>     df = df.filter(F.col("__crdb__updated") <= watermark)
    """
    try:
        # Check data source and use appropriate module
        if config.data_source == "azure_storage":
            # Import cockroachdb_azure (works in notebooks and when package is installed)
            import cockroachdb_azure
            
            # Use Azure SDK to list files (works in all environments)
            print(f"   🔍 Scanning for .RESOLVED files in Azure Blob Storage...")
            
            result = cockroachdb_azure.check_azure_files(
                storage_account_name=config.azure_storage.account_name,
                storage_account_key=config.azure_storage.account_key,
                container_name=config.azure_storage.container_name,
                source_catalog=config.tables.source_catalog,
                source_schema=config.tables.source_schema,
                source_table=source_table,
                target_table=config.tables.destination_table_name,
                verbose=False,
                format=config.cdc_config.format
            )
        
        elif config.data_source == "uc_external_volume":
            # Import cockroachdb_uc_volume
            import cockroachdb_uc_volume
            from cockroachdb_config import get_volume_path
            
            # Use UC Volume functions
            print(f"   🔍 Scanning for .RESOLVED files in Unity Catalog Volume...")
            
            if not spark:
                print(f"   ⚠️  spark is required for UC Volume access")
                print(f"      Proceeding without RESOLVED watermarking")
                return None
            
            volume_path = get_volume_path(config)
            print(f"   📂 Volume path: {volume_path}")
            print(f"   ⏳ Listing files (this may take 5-10 seconds)...")
            
            import time
            start_time = time.time()
            
            result = cockroachdb_uc_volume.check_volume_files(
                volume_path=volume_path,
                source_catalog=config.tables.source_catalog,
                source_schema=config.tables.source_schema,
                source_table=source_table,
                target_table=config.tables.destination_table_name,
                spark=spark,
                verbose=False,
                format=config.cdc_config.format
            )
            
            elapsed = time.time() - start_time
            print(f"   ✅ File listing completed in {elapsed:.1f}s")
        
        else:
            print(f"   ⚠️  Unknown data source: {config.data_source}")
            return None
        
        resolved_files = result['resolved_files']
        
        if not resolved_files:
            print(f"   ⚠️  No .RESOLVED files found for table '{source_table}'")
            print(f"      Proceeding without RESOLVED watermarking (all data will be processed)")
            return None
        
        print(f"   📁 Found {len(resolved_files)} .RESOLVED file(s)")
        if resolved_files:
            print(f"   📄 Example RESOLVED file: {resolved_files[0]['name']}")
        
        # Extract timestamps from filenames
        # CockroachDB RESOLVED file formats (observed patterns):
        # - Format 1: <timestamp_nanos>.RESOLVED (e.g., 1738468800000000000.RESOLVED)
        # - Format 2: <timestamp>-<jobid>-...-<table>.RESOLVED (CDC file naming pattern)
        # - Format 3: path/to/<timestamp>.RESOLVED.parquet
        timestamps = []
        for file in resolved_files:
            try:
                # Get just the filename (not the full path)
                # All storage backends now return dicts with ['name'] key
                full_path = file['name']
                filename = full_path.split('/')[-1]  # Get last part after /
                
                # Remove .parquet extension if present
                if filename.endswith('.parquet'):
                    filename = filename[:-8]  # Remove '.parquet'
                
                # The timestamp is typically the FIRST part (before first dash or dot)
                # Split by dash first (for CDC file naming pattern)
                if '-' in filename:
                    first_part = filename.split('-')[0]
                else:
                    # Split by dot (for simple timestamp.RESOLVED format)
                    first_part = filename.split('.')[0]
                
                # Parse timestamp based on format
                # CockroachDB uses HLC (Hybrid Logical Clock) timestamps in RESOLVED filenames
                if first_part.isdigit():
                    timestamp_str = first_part
                    
                    # CockroachDB HLC format (33 digits): YYYYMMDDHHMMSS + NNNNNNNNN + LLLLLLLLLL
                    # where: 14 digits datetime + 9 digits nanos + 10 digits logical
                    if len(timestamp_str) == 33:
                        # Parse CockroachDB HLC format
                        from datetime import datetime as dt
                        
                        # Extract components
                        datetime_part = timestamp_str[:14]  # YYYYMMDDHHMMSS
                        nanos_part = timestamp_str[14:23]    # 9 digits
                        logical_part = timestamp_str[23:33]  # 10 digits
                        
                        # Convert datetime to Unix timestamp
                        year = int(datetime_part[0:4])
                        month = int(datetime_part[4:6])
                        day = int(datetime_part[6:8])
                        hour = int(datetime_part[8:10])
                        minute = int(datetime_part[10:12])
                        second = int(datetime_part[12:14])
                        
                        dt_obj = dt(year, month, day, hour, minute, second)
                        unix_seconds = int(dt_obj.timestamp())
                        
                        # Convert to nanoseconds and add nanosecond component
                        timestamp_nanos = (unix_seconds * 1_000_000_000) + int(nanos_part)
                        timestamps.append(timestamp_nanos)
                        
                    # Standard Unix nanosecond timestamp (19 digits)
                    elif 18 <= len(timestamp_str) <= 19:
                        timestamp_nanos = int(timestamp_str)
                        if timestamp_nanos < 10_000_000_000_000_000_000:
                            timestamps.append(timestamp_nanos)
                        else:
                            print(f"   ⚠️  Timestamp out of valid range: {timestamp_str}")
                    else:
                        print(f"   ⚠️  Unexpected timestamp length ({len(timestamp_str)} digits): {timestamp_str}")
                else:
                    print(f"   ⚠️  Could not extract timestamp from: {filename} (first_part={first_part})")
            except (IndexError, ValueError) as e:
                print(f"   ⚠️  Skipping malformed RESOLVED file: {full_path} ({e})")
                continue
        
        if not timestamps:
            print(f"   ⚠️  No valid .RESOLVED timestamps found")
            print(f"      Check RESOLVED file naming format")
            return None
        
        # Get the MAXIMUM (latest) RESOLVED timestamp
        max_resolved = max(timestamps)
        
        # Convert to human-readable datetime for logging
        try:
            max_resolved_seconds = max_resolved / 1_000_000_000
            max_resolved_dt = datetime.fromtimestamp(max_resolved_seconds)
            datetime_str = max_resolved_dt.strftime('%Y-%m-%d %H:%M:%S')
        except (ValueError, OSError, OverflowError) as e:
            # Timestamp out of range for datetime conversion
            print(f"   ⚠️  Could not convert timestamp to datetime: {e}")
            datetime_str = f"<timestamp: {max_resolved} nanos>"
        
        print(f"   ✅ Found {len(resolved_files)} .RESOLVED file(s)")
        print(f"   ✅ Latest RESOLVED watermark: {max_resolved} nanos")
        print(f"      ({datetime_str} UTC)")
        print(f"   💡 Only events with timestamp ≤ {max_resolved} will be processed")
        print(f"      This GUARANTEES all column family fragments have arrived")
        print()
        
        return max_resolved
        
    except Exception as e:
        print(f"   ⚠️  Error reading .RESOLVED files: {e}")
        print(f"      Proceeding without RESOLVED watermarking")
        return None


def _resolve_watermark(
    config,
    source_table: str,
    resolved_watermark: Optional[int],
    is_multi_cf: bool = False,
    spark=None
) -> Optional[int]:
    """
    Resolve the RESOLVED watermark to use for CDC ingestion.
    
    This helper consolidates watermark resolution logic across all ingestion functions.
    Supports both Azure Blob Storage and Unity Catalog external volumes.
    
    Logic:
    1. If watermark disabled in config: Return None (no watermarking)
    2. If watermark provided by caller: Use it (multi-table coordination)
    3. Otherwise: Compute from .RESOLVED files (using Azure SDK or Spark listing)
    
    Args:
        config: Config dataclass with storage credentials
        source_table: Source table name
        resolved_watermark: Optional watermark provided by caller
        is_multi_cf: True if multi-CF mode (affects warning messages)
        spark: SparkSession (required for UC Volume, optional for Azure)
    
    Returns:
        int: RESOLVED timestamp in nanoseconds, or None if disabled/unavailable
    """
    # Check if RESOLVED watermarking is enabled in config
    if not config.cdc_config.use_resolved_watermark:
        print("⚠️  RESOLVED Watermarking: DISABLED")
        if is_multi_cf:
            print("   WARNING: Column family fragments may be incomplete!")
        print("   Recommendation: Set config.cdc_config.use_resolved_watermark = True")
        print()
        return None
    
    # RESOLVED watermarking is enabled
    if resolved_watermark is not None:
        # Watermark provided by caller (multi-table coordination)
        print(f"🔒 RESOLVED Watermarking: ENABLED (provided by caller)")
        print(f"   Using watermark: {resolved_watermark} nanos")
        if is_multi_cf:
            print(f"   This GUARANTEES all column family fragments are complete before processing")
        print(f"   💡 Shared watermark ensures consistency across multiple tables")
        print()
        return resolved_watermark
    
    # Compute watermark from .RESOLVED files
    if is_multi_cf:
        print("🔒 RESOLVED Watermarking: ENABLED")
        print("   This GUARANTEES all column family fragments are complete before processing")
    else:
        print("🔒 RESOLVED Watermarking: ENABLED (optional for single-CF tables)")
    
    computed_watermark = _get_resolved_watermark(config, source_table, spark)
    
    if computed_watermark and is_multi_cf:
        print("   ✅ RESOLVED watermark will be applied to CDC events")
        print()
    elif not computed_watermark and is_multi_cf:
        print("   ⚠️  No RESOLVED watermark found - proceeding with all data")
        print()
    
    return computed_watermark


# ============================================================================
# HELPER FUNCTIONS - MULTI-STAGE PROCESSING
# ============================================================================

def _stream_to_staging(df, checkpoint_path, staging_table_fqn):
    """
    Stream CDC events to staging table (multi-stage processing).
    
    Used by functions that need intermediate staging:
    - Merge functions: Need staging for deduplication
    - Multi-CF functions: Need staging for column family merging
    
    Args:
        df: Transformed DataFrame (with _cdc_timestamp, _cdc_operation)
        checkpoint_path: Checkpoint location base path
        staging_table_fqn: Fully qualified staging table name
    
    Returns:
        StreamingQuery object
    """
    query = (df.writeStream
        .format("delta")
        .option("checkpointLocation", f"{checkpoint_path}/data")
        .option("mergeSchema", "true")
        .trigger(availableNow=True)
        .toTable(staging_table_fqn)
    )
    
    print("⏳ Streaming CDC events to staging table...")
    query.awaitTermination()
    print("✅ Stream completed\n")
    
    return query


# ============================================================================
# HELPER FUNCTIONS - UPDATE/DELETE MODE
# ============================================================================

def _merge_staging_to_target(spark, staging_table_fqn, target_table_fqn, primary_key_columns, staging_df_raw=None):
    """
    Process staging table to target with MERGE logic (UPDATE/DELETE mode).
    
    This is "STAGE 2" of the two-stage MERGE pipeline. It performs:
    1. Deduplication: Keep latest event per primary key
    2. If new table: Handle DELETE events and create table
    3. If existing table: Ensure _cdc_operation column exists and execute MERGE
    
    Args:
        spark: SparkSession
        staging_table_fqn: Fully qualified staging table name
        target_table_fqn: Fully qualified target table name
        primary_key_columns: List of primary key column names
        staging_df_raw: Optional pre-processed DataFrame (for multi-CF mode with merged fragments)
                       If None, will read from staging_table_fqn
    
    Returns:
        dict: {
            "staging_count_raw": int,
            "staging_count": int,
            "merged_count": int,
            "duplicates_removed": int
        }
    """
    from pyspark.sql import functions as F, Window
    from delta.tables import DeltaTable
    
    print("🔷 STAGE 2: Applying MERGE logic (batch operation)")
    print(f"   Source: {staging_table_fqn}")
    print(f"   Target: {target_table_fqn}")
    print()
    
    # ============================================================================
    # STEP 1: Read staging table (or use provided DataFrame)
    # ============================================================================
    if staging_df_raw is None:
        staging_df_raw = spark.read.table(staging_table_fqn)
    
    staging_count_raw = staging_df_raw.count()
    print(f"   📊 Raw staging events: {staging_count_raw}")
    
    if staging_count_raw == 0:
        print("   ℹ️  No new events to process")
        return {
            "staging_count_raw": 0,
            "staging_count": 0,
            "merged_count": 0,
            "duplicates_removed": 0
        }
    
    # ============================================================================
    # STEP 2: Deduplication
    # ============================================================================
    print(f"   🔄 Deduplicating by primary keys: {primary_key_columns}...")
    window_spec = Window.partitionBy(*primary_key_columns).orderBy(F.col("_cdc_timestamp").desc())
    staging_df = (staging_df_raw
        .withColumn("_row_num", F.row_number().over(window_spec))
        .filter(F.col("_row_num") == 1)
        .drop("_row_num")
    )
    
    staging_count = staging_df.count()
    duplicates_removed = staging_count_raw - staging_count
    print(f"   ✅ Deduplicated: {staging_count} unique events ({duplicates_removed} duplicates removed)")
    
    if staging_count == 0:
        print("   ℹ️  All events were duplicates")
        return {
            "staging_count_raw": staging_count_raw,
            "staging_count": 0,
            "merged_count": 0,
            "duplicates_removed": duplicates_removed
        }
    
    # ============================================================================
    # STEP 3: Create target table OR Execute MERGE
    # ============================================================================
    if not spark.catalog.tableExists(target_table_fqn):
        # ========================================================================
        # Path A: New table - Handle DELETE events
        # ========================================================================
        print(f"   📝 Creating new target table: {target_table_fqn}")
        
        # 1. Get keys that have DELETE events
        delete_keys = staging_df.filter(F.col("_cdc_operation") == "DELETE") \
            .select(*primary_key_columns) \
            .distinct()
        delete_count = delete_keys.count()
        
        # 2. Get all non-DELETE rows
        active_rows = staging_df.filter(F.col("_cdc_operation") != "DELETE")
        active_count = active_rows.count()
        
        # 3. Exclude rows with keys that are deleted (left anti join)
        rows_after_delete = active_rows.join(
            delete_keys,
            on=primary_key_columns,
            how="left_anti"
        )
        after_delete_count = rows_after_delete.count()
        
        final_rows = rows_after_delete
        final_count = after_delete_count
        
        if delete_count > 0:
            print(f"   ℹ️  Found {delete_count} DELETE events")
            print(f"   ℹ️  Active rows before DELETE: {active_count}")
            print(f"   ℹ️  Active rows after DELETE: {after_delete_count}")
            print(f"   ℹ️  Rows removed by DELETE: {active_count - after_delete_count}")
        
        # Keep ALL columns including _cdc_operation for monitoring
        final_rows.write.format("delta").saveAsTable(target_table_fqn)
        merged_count = final_count
        print(f"   ✅ Created table with {merged_count} initial rows")
        print(f"      Schema includes _cdc_operation for observability\n")
        
    else:
        # ========================================================================
        # Path B: Existing table - Execute MERGE
        # ========================================================================
        delta_table = DeltaTable.forName(spark, target_table_fqn)
        
        # Check if _cdc_operation exists in target
        target_columns = set(spark.read.table(target_table_fqn).columns)
        if "_cdc_operation" not in target_columns:
            print(f"   ⚠️  Target table missing _cdc_operation column (old schema)")
            print(f"   🔧 Adding _cdc_operation column for observability...")
            spark.sql(f"""
                ALTER TABLE {target_table_fqn} 
                ADD COLUMN _cdc_operation STRING
            """)
            print(f"   ✅ Column added\n")
        
        # Build and execute MERGE
        join_condition = " AND ".join([f"target.{col} = source.{col}" for col in primary_key_columns])
        data_columns = [col for col in staging_df.columns]
        update_set = {col: f"source.{col}" for col in data_columns}
        insert_values = {col: f"source.{col}" for col in data_columns}
        
        print(f"   🔄 Executing MERGE...")
        print(f"      Join: {join_condition}")
        print(f"      ℹ️  _cdc_operation will be preserved for monitoring")
        
        (delta_table.alias("target").merge(
            staging_df.alias("source"),
            join_condition
        )
        .whenMatchedUpdate(
            condition="source._cdc_operation = 'UPSERT' AND source._cdc_timestamp > target._cdc_timestamp",
            set=update_set
        )
        .whenMatchedDelete(
            condition="source._cdc_operation = 'DELETE'"
        )
        .whenNotMatchedInsert(
            condition="source._cdc_operation = 'UPSERT'",
            values=insert_values
        )
        .execute())
        
        merged_count = staging_count
        print(f"   ✅ MERGE complete: processed {merged_count} events\n")
    
    return {
        "staging_count_raw": staging_count_raw,
        "staging_count": staging_count,
        "merged_count": merged_count,
        "duplicates_removed": duplicates_removed
    }


# ============================================================================
# MAIN INGESTION FUNCTIONS
# ============================================================================

def ingest_cdc_append_only_single_family(
    config,
    spark,
    resolved_watermark: Optional[int] = None
):
    """
    Ingest CDC events in APPEND-ONLY mode for single column family tables.
    
    This function:
    - Reads Parquet CDC files from storage (Azure or UC Volume) using Auto Loader
    - Filters out .RESOLVED files and metadata
    - Transforms CockroachDB CDC columns (__crdb__*) to standard format
    - Writes all events (INSERT/UPDATE/DELETE) as rows to Delta table
    - Does NOT apply deletes or deduplicate updates (append_only)
    
    Use this for:
    - Audit logs and full history tracking
    - Tables WITHOUT column families (split_column_families=false)
    - Simple CDC pipelines without MERGE logic
    
    Args:
        config: Config dataclass from cockroachdb_config.py
        spark: SparkSession
        resolved_watermark: Optional RESOLVED timestamp in nanoseconds.
                           If provided, uses this watermark (for multi-table coordination).
                           If None, will compute from .RESOLVED files if enabled.
    
    Returns:
        StreamingQuery object
    """
    # Build paths using helper
    source_path, checkpoint_path, target_table_fqn = _build_paths(config)
    
    # Print ingestion header
    _print_ingestion_header(config, "append_only", "single_cf", source_path, target_table_fqn)
    
    # Resolve RESOLVED watermark (use provided or compute)
    resolved_watermark = _resolve_watermark(
        config=config,
        source_table=config.tables.source_table_name,
        resolved_watermark=resolved_watermark,
        is_multi_cf=False,  # Single-CF mode
        spark=spark
    )
    
    # Read with Auto Loader
    raw_df = _setup_autoloader(spark, source_path, checkpoint_path, config.tables.source_table_name)
    
    print("✅ Schema inferred from data files")
    print("   (Filtering matches cockroachdb.py production code)")
    print()
    
    # Transform: CockroachDB CDC → Standard CDC format
    df = _transform_cdc_columns(raw_df, resolved_watermark=resolved_watermark)
    
    # Write to Delta table (append_only)
    query = (df.writeStream
        .format("delta")
        .option("checkpointLocation", f"{checkpoint_path}/data")
        .option("mergeSchema", "true")
        .trigger(availableNow=True)
        .toTable(target_table_fqn)
    )
    
    print("⏳ Processing CDC events...")
    return query


def ingest_cdc_with_merge_single_family(
    config,
    spark,
    resolved_watermark: Optional[int] = None
):
    """
    Ingest CDC events with MERGE logic for single column family tables.
    
    This function:
    - Reads Parquet CDC files from storage (Azure or UC Volume) using Auto Loader
    - Filters out .RESOLVED files and metadata
    - Transforms CockroachDB CDC columns (__crdb__*) to standard format
    - Deduplicates events within each microbatch (handles column family fragments)
    - Applies MERGE logic to target Delta table:
      * UPDATE: When key exists and timestamp is newer
      * DELETE: When key exists and operation is DELETE
      * INSERT: When key doesn't exist and operation is UPSERT
    - Preserves _cdc_operation column for monitoring and observability
    
    Use this for:
    - Applications needing current state (not history)
    - Tables WITHOUT column families (split_column_families=false)
    - Production CDC pipelines with UPDATE/DELETE support
    - Lower storage requirements (only latest state)
    
    Target table will contain:
    - All data columns from source
    - _cdc_operation: "UPSERT" (shows last operation on each row)
    - _cdc_timestamp: Timestamp of last CDC event
    
    Args:
        config: Config dataclass from cockroachdb_config.py
        spark: SparkSession
        resolved_watermark: Optional RESOLVED timestamp in nanoseconds.
                           If provided, uses this watermark (for multi-table coordination).
                           If None, will compute from .RESOLVED files if enabled.
    
    Returns:
        Dict with query, staging_table, target_table, raw_count, deduped_count, merged
    """
    from pyspark.sql import functions as F, Window
    from delta.tables import DeltaTable
    
    # Build paths using helper (with "_merge" suffix for checkpoint)
    source_path, checkpoint_path, target_table_fqn = _build_paths(config, mode_suffix="_merge")
    
    # Get primary key columns from config
    primary_key_columns = config.cdc_config.primary_key_columns
    
    # Print ingestion header
    _print_ingestion_header(config, "update_delete", "single_cf", source_path, target_table_fqn)
    print(f"Primary keys: {primary_key_columns}")
    print()
    
    # Resolve RESOLVED watermark (use provided or compute)
    resolved_watermark = _resolve_watermark(
        config=config,
        source_table=config.tables.source_table_name,
        resolved_watermark=resolved_watermark,
        is_multi_cf=False,  # Single-CF mode
        spark=spark
    )
    
    # Read with Auto Loader
    raw_df = _setup_autoloader(spark, source_path, checkpoint_path, config.tables.source_table_name)
    
    print("✅ Schema inferred from data files")
    print("   (Filtering matches cockroachdb.py production code)")
    print()
    
    # Transform: CockroachDB CDC → Standard CDC format
    transformed_df = _transform_cdc_columns(raw_df, resolved_watermark=resolved_watermark)
    
    print("✅ CDC transformations applied (streaming compatible)")
    print("   ℹ️  Deduplication will happen in Stage 2 (batch mode)")
    print()
    
    # ========================================================================
    # STAGE 1: Stream to Staging Table (Serverless Compatible)
    # ========================================================================
    staging_table_fqn = f"{target_table_fqn}_staging"
    
    print("🔷 STAGE 1: Streaming to staging table (no Python UDFs)")
    print(f"   Staging: {staging_table_fqn}")
    print()
    
    # Stream to staging using helper
    query = _stream_to_staging(transformed_df, checkpoint_path, staging_table_fqn)
    
    # ========================================================================
    # STAGE 2: Batch MERGE from Staging to Target
    # ========================================================================
    # Merge staging to target using helper (combines deduplication, DELETE handling, and MERGE)
    results = _merge_staging_to_target(spark, staging_table_fqn, target_table_fqn, primary_key_columns)
    
    staging_count_raw = results["staging_count_raw"]
    staging_count = results["staging_count"]
    merged_count = results["merged_count"]
    duplicates_removed = results["duplicates_removed"]
    
    print("=" * 80)
    print("✅ CDC INGESTION COMPLETE (TWO-STAGE MERGE)")
    print("=" * 80)
    print(f"📊 Raw events: {staging_count_raw}")
    print(f"📊 After deduplication: {staging_count} unique events")
    print(f"📊 Staging table: {staging_table_fqn}")
    print(f"📊 Target table:  {target_table_fqn}")
    print()
    print("📋 Target table includes:")
    print("   - All data columns from source")
    print("   - _cdc_operation: UPSERT (for monitoring)")
    print("   - _cdc_timestamp: Last CDC event timestamp")
    print()
    print("💡 TIP: Staging table can be dropped after successful MERGE:")
    print(f"   spark.sql('DROP TABLE IF EXISTS {staging_table_fqn}')")
    
    return {
        "query": query,
        "staging_table": staging_table_fqn,
        "target_table": target_table_fqn,
        "raw_count": staging_count_raw,
        "deduped_count": staging_count,
        "merged": merged_count
    }


def merge_column_family_fragments(
    df,
    primary_key_columns: List[str],
    metadata_columns: List[str] = None,
    debug: bool = False,
    is_streaming: bool = None,
    deduplicate_to_latest_state: bool = False
):
    """
    Merge column family fragments into complete rows.
    
    When split_column_families=true, CockroachDB writes one Parquet file per column family,
    resulting in multiple fragment records per logical row. This function merges these
    fragments by grouping on primary key and taking the first non-null value for each column.
    
    **Streaming vs Batch Mode:**
    - **Streaming DataFrames** (from Autoloader): Always applies merge (can't detect beforehand)
    - **Batch DataFrames** (from spark.read): Auto-detects fragmentation, skips if not needed
    - Set `is_streaming=True` to force streaming mode (skips detection)
    - Set `is_streaming=False` to force batch mode (enables detection)
    
    **Deduplication Mode (NEW):**
    - `deduplicate_to_latest_state=False` (default): Merges fragments within same event, preserves all events
    - `deduplicate_to_latest_state=True`: Coalesces columns across time + deduplicates to latest state
      * Use when you have multiple UPDATE events for the same key
      * Preserves old values for columns not touched by newer events
      * Example: Event1 has field3=3, Event2 updates field0 but leaves field3=NULL
        → Result keeps field3=3 from Event1 (not NULL from Event2)
    
    Args:
        df: Spark DataFrame with potential column family fragments
        primary_key_columns: List of primary key column names (e.g., ['ycsb_key'])
        metadata_columns: Optional list of metadata columns to preserve
                         (default: __crdb__*, _cdc_*, _source_*, _rescued_data)
        debug: Enable debug output showing merge statistics
        is_streaming: Optional boolean to force streaming/batch mode
                     (default: auto-detect based on df.isStreaming)
        deduplicate_to_latest_state: If True, coalesce columns across time and deduplicate to latest row
                                    (default: False - preserves all CDC events)
        
    Returns:
        Merged Spark DataFrame with complete rows
        
    Example (Standard Mode - Preserves All Events):
        ```python
        from cockroachdb import merge_column_family_fragments
        
        # Read batch data
        df_raw = spark.read.parquet("dbfs:/Volumes/catalog/schema/volume")
        
        # Merge - auto-detects fragmentation, preserves all CDC events
        df_merged = merge_column_family_fragments(
            df_raw,
            primary_key_columns=['ycsb_key'],
            debug=True
        )
        ```
        
    Example (Streaming):
        ```python
        # Read streaming data (Autoloader)
        df_raw = spark.readStream.format("cloudFiles").load(...)
        
        # Add transformations
        df_transformed = df_raw.withColumn(...)
        
        # Merge - always applies (can't detect on streaming)
        df_merged = merge_column_family_fragments(
            df_transformed,
            primary_key_columns=['ycsb_key'],
            debug=True
        )
        
        # Write to Delta
        df_merged.writeStream.toTable(...)
        ```
        
    Example (Deduplication Mode - Latest State with Value Preservation):
        ```python
        # For staging → target MERGE scenarios where you want latest state
        # and need to preserve old column values when newer events don't touch them
        
        # Read staging data (may have multiple UPDATE events per key)
        df_staging = spark.read.table("staging_table")
        
        # Merge + deduplicate to latest state (preserves old values)
        df_latest = merge_column_family_fragments(
            df_staging,
            primary_key_columns=['ycsb_key'],
            deduplicate_to_latest_state=True,  # ← NEW MODE
            debug=True
        )
        
        # Result: Latest row per key with all column values preserved
        # - field0 from latest event where field0 was updated
        # - field3 from earlier event (if latest event didn't touch field3)
        ```
        
    **Technical Details:**
    
    *Standard Mode (default):*
    - Uses `first(col, ignorenulls=True)` to combine NULL values from different fragments
    - Each fragment has the PK + data for ONE column family (other columns are NULL)
    - Groups by PK + timestamp + operation to preserve ALL CDC events
    - For non-split tables, this is a harmless no-op (groupBy preserves all data)
    
    *Deduplication Mode (deduplicate_to_latest_state=True):*
    - Uses `last(col, ignorenulls=True)` over window to coalesce columns across time
    - Then deduplicates to keep only the latest row per PK
    - Preserves old values for columns not touched by newer UPDATE events
    - Essential for handling CockroachDB column families with partial updates
    
    **Performance:**
    - Requires a shuffle operation (groupBy)
    - For large datasets, consider repartitioning by PK first
    - Adaptive Query Execution (AQE) helps optimize automatically
    """
    from pyspark.sql import functions as F
    
    # Auto-detect streaming mode if not explicitly set
    if is_streaming is None:
        is_streaming = df.isStreaming
    
    # Default metadata columns to preserve
    if metadata_columns is None:
        metadata_columns = [
            '__crdb__event_type', '__crdb__updated', '_rescued_data',
            '_cdc_operation', '_cdc_timestamp', '_cdc_updated', '_source_file', '_processing_time',
            '_metadata',  # Unity Catalog metadata
            # JSON envelope columns (should not be merged as data columns)
            'after', 'before', 'key', 'updated',
            # Debug columns
            '_after_json', '_before_json', '_debug_after_first_10', '_debug_before_first_10'
        ]
    
    # Get all columns from DataFrame
    all_columns = df.columns
    
    # Identify data columns (everything except PK and metadata)
    data_columns = [
        col for col in all_columns 
        if col not in primary_key_columns and col not in metadata_columns
        and not col.startswith('__crdb__')
        and not col.startswith('_cdc_')
        and not col.startswith('_source_')
        and not col.startswith('_rescued_')
    ]
    
    if debug:
        mode_str = "Streaming" if is_streaming else "Batch"
        print(f"\n🔍 Column Family Merge ({mode_str} Mode)")
        print(f"   Primary key columns: {primary_key_columns}")
        print(f"   Metadata columns: {len(metadata_columns)} columns")
        print(f"   Data columns: {len(data_columns)} columns")
        if len(data_columns) <= 10:
            print(f"     {data_columns}")
        else:
            print(f"     {data_columns[:5]}... (showing first 5)")
        
        # DIAGNOSTIC: If no data columns, show what we have
        if len(data_columns) == 0:
            print(f"\n⚠️  WARNING: No data columns found!")
            print(f"   All columns in DataFrame: {all_columns}")
            print(f"   Metadata columns list: {metadata_columns}")
    
    # For batch mode: Check if merge is needed
    # For streaming mode: Always merge (can't count streaming DataFrames)
    if not is_streaming:
        try:
            # Determine timestamp column for fragmentation detection
            timestamp_col_for_check = None
            if '_cdc_timestamp' in all_columns:
                timestamp_col_for_check = '_cdc_timestamp'
            elif '_cdc_updated' in all_columns:
                timestamp_col_for_check = '_cdc_updated'
            elif '__crdb__updated' in all_columns:
                timestamp_col_for_check = '__crdb__updated'
            elif 'updated' in all_columns:
                timestamp_col_for_check = 'updated'
            
            # Try to detect fragmentation
            total_rows = df.count()
            
            if timestamp_col_for_check and '_cdc_operation' in all_columns:
                # Check for fragmentation at (PK + timestamp + operation) level
                # This preserves all distinct CDC events (same key can have UPDATE + DELETE at same timestamp)
                unique_events = df.select(primary_key_columns + [timestamp_col_for_check, '_cdc_operation']).distinct().count()
            elif timestamp_col_for_check:
                # Fallback: check at (PK + timestamp) level
                unique_events = df.select(primary_key_columns + [timestamp_col_for_check]).distinct().count()
            elif '_cdc_operation' in all_columns:
                # Fallback: check at (PK + operation) level
                unique_events = df.select(primary_key_columns + ['_cdc_operation']).distinct().count()
            else:
                # Fallback: check at PK level only
                unique_events = df.select(primary_key_columns).distinct().count()
            
            if debug:
                print(f"\n📊 Fragmentation Detection:")
                print(f"   Total rows: {total_rows:,}")
                if timestamp_col_for_check and '_cdc_operation' in all_columns:
                    print(f"   Unique events (PK + timestamp + operation): {unique_events:,}")
                elif timestamp_col_for_check:
                    print(f"   Unique events (PK + timestamp): {unique_events:,}")
                elif '_cdc_operation' in all_columns:
                    print(f"   Unique events (PK + operation): {unique_events:,}")
                else:
                    print(f"   Unique keys (PK only): {unique_events:,}")
                print(f"   Duplication ratio: {total_rows / unique_events if unique_events > 0 else 0:.1f}x")
            
            # If no duplicates, return original DataFrame
            if total_rows == unique_events:
                if debug:
                    print(f"\n✅ No column family fragmentation detected")
                    print(f"   Returning original DataFrame unchanged")
                return df
            
            if debug:
                print(f"\n🔧 Column family fragmentation detected!")
                print(f"   Merging {total_rows:,} fragments into {unique_events:,} distinct CDC events...")
        except Exception as e:
            # If detection fails (e.g., actually streaming), proceed with merge
            if debug:
                print(f"\n⚠️  Detection failed (treating as streaming): {e}")
                print(f"   Proceeding with merge...")
    else:
        if debug:
            print(f"\n🔧 Streaming mode: Applying merge")
            print(f"   (Cannot detect fragmentation in streaming DataFrames)")
            print(f"   - If column families exist: fragments will be merged")
            print(f"   - If no column families: merge is harmless no-op")
    
    # ============================================================================
    # DEDUPLICATION MODE: Coalesce columns across time + deduplicate to latest state
    # ============================================================================
    if deduplicate_to_latest_state:
        from pyspark.sql.window import Window
        
        if debug:
            print(f"\n🔄 Applying cross-time coalescing + deduplication...")
            print(f"   (Preserves latest non-NULL value per column across all events)")
        
        # Determine timestamp column for ordering
        timestamp_col_for_coalesce = None
        if '_cdc_timestamp' in all_columns:
            timestamp_col_for_coalesce = '_cdc_timestamp'
        elif '_cdc_updated' in all_columns:
            timestamp_col_for_coalesce = '_cdc_updated'
        elif '__crdb__updated' in all_columns:
            timestamp_col_for_coalesce = '__crdb__updated'
        elif 'updated' in all_columns:
            timestamp_col_for_coalesce = 'updated'
        
        if not timestamp_col_for_coalesce:
            if debug:
                print(f"   ⚠️  No timestamp column found - cannot coalesce across time")
                print(f"   Falling back to standard merge mode")
        else:
            # Step 1: Coalesce columns across time (keep latest non-NULL value per column)
            if debug:
                print(f"   Step 1: Coalescing columns by primary keys: {primary_key_columns}...")
                print(f"           Using last_value(col, ignorenulls=True) per column")
            
            # Window spec: partition by PK, order by timestamp, look at all rows
            window_spec_coalesce = (Window.partitionBy(*primary_key_columns)
                .orderBy(F.col(timestamp_col_for_coalesce))
                .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing))
            
            # For each data column, coalesce to latest non-NULL value
            for col in data_columns:
                df = df.withColumn(
                    col,
                    F.last(F.col(col), ignorenulls=True).over(window_spec_coalesce)
                )
            
            # Also coalesce _cdc_operation to the LATEST value (for DELETE handling)
            if '_cdc_operation' in all_columns:
                df = df.withColumn(
                    "_cdc_operation",
                    F.last(F.col("_cdc_operation"), ignorenulls=True).over(window_spec_coalesce)
                )
            
            if debug:
                print(f"   ✅ Columns coalesced (latest non-NULL value per column)")
            
            # Step 2: Deduplicate by primary key (keep LATEST row, which now has ALL coalesced columns)
            if debug:
                print(f"   Step 2: Deduplicating by primary keys: {primary_key_columns}...")
            
            window_spec_dedup = Window.partitionBy(*primary_key_columns).orderBy(F.col(timestamp_col_for_coalesce).desc())
            df_merged = (df
                .withColumn("_row_num", F.row_number().over(window_spec_dedup))
                .filter(F.col("_row_num") == 1)
                .drop("_row_num")
            )
            
            if debug:
                print(f"   ✅ Deduplication complete!")
                print(f"      Result: Latest state per primary key with all column values preserved")
            
            return df_merged
    
    # ============================================================================
    # STANDARD MODE: Merge fragments within events, preserve all CDC events
    # ============================================================================
    
    # Build aggregation expressions for merging column family fragments
    # CRITICAL: For CDC data, we must preserve ALL events for a key (SNAPSHOT, UPDATE, DELETE)
    # So we group by BOTH primary_key AND timestamp to:
    #   1. Merge fragments WITHIN the same CDC event (same PK + same timestamp)
    #   2. Preserve ALL CDC events for a key (different timestamps)
    agg_exprs = []
    
    # Determine which timestamp column to use for grouping
    timestamp_col = None
    if '_cdc_timestamp' in all_columns:
        timestamp_col = '_cdc_timestamp'
    elif '_cdc_updated' in all_columns:
        timestamp_col = '_cdc_updated'
    elif '__crdb__updated' in all_columns:
        timestamp_col = '__crdb__updated'
    elif 'updated' in all_columns:
        timestamp_col = 'updated'
    
    # Determine grouping columns
    if timestamp_col and '_cdc_operation' in all_columns:
        # Group by PK + timestamp + operation to preserve all distinct CDC events
        # This is critical: same key can have UPDATE and DELETE at same timestamp!
        group_by_cols = primary_key_columns + [timestamp_col, '_cdc_operation']
        
        # For aggregation, use first() with ignorenulls to combine NULL values from fragments
        # (Each fragment has data for ONE column family, other families are NULL)
        for col in data_columns:
            agg_exprs.append(F.first(col, ignorenulls=True).alias(col))
        
        # Metadata columns: also use first()
        # Don't aggregate columns that are already in the grouping key
        for col in metadata_columns:
            if col in all_columns and col not in group_by_cols:
                agg_exprs.append(F.first(col, ignorenulls=True).alias(col))
    elif timestamp_col:
        # Fallback: Group by PK + timestamp only
        group_by_cols = primary_key_columns + [timestamp_col]
        
        # For aggregation, use first() with ignorenulls to combine NULL values from fragments
        # (Each fragment has data for ONE column family, other families are NULL)
        for col in data_columns:
            agg_exprs.append(F.first(col, ignorenulls=True).alias(col))
        
        # Metadata columns: also use first()
        # Don't aggregate columns that are already in the grouping key
        for col in metadata_columns:
            if col in all_columns and col not in group_by_cols:
                agg_exprs.append(F.first(col, ignorenulls=True).alias(col))
    else:
        # No timestamp column - group by PK + operation if available (best effort)
        if '_cdc_operation' in all_columns:
            group_by_cols = primary_key_columns + ['_cdc_operation']
        else:
            group_by_cols = primary_key_columns
        
        for col in data_columns:
            agg_exprs.append(F.first(col, ignorenulls=True).alias(col))
        
        for col in metadata_columns:
            if col in all_columns and col not in group_by_cols:
                agg_exprs.append(F.first(col, ignorenulls=True).alias(col))
    
    # Group by primary key + timestamp + operation and aggregate
    if not agg_exprs:
        # No columns to aggregate means all data is in grouping columns
        # This shouldn't happen in normal CDC scenarios, but handle it gracefully
        if debug:
            print(f"\n⚠️  No additional columns to aggregate beyond grouping key")
            print(f"   Using distinct on grouping columns: {group_by_cols}")
        df_merged = df.select(*group_by_cols).distinct()
    else:
        df_merged = df.groupBy(*group_by_cols).agg(*agg_exprs)
    
    if debug:
        print(f"\n✅ Merge transformation applied!")
        if is_streaming:
            print(f"   Streaming DataFrame merged")
            print(f"   (Actual counts will be visible after writeStream completes)")
        else:
            print(f"   Batch DataFrame merged")
    
    return df_merged


def ingest_cdc_append_only_multi_family(
    config,
    spark,
    resolved_watermark: Optional[int] = None
):
    """
    Ingest CDC events in APPEND-ONLY mode with COLUMN FAMILY support.
    
    **Two-Stage Approach (Serverless Compatible)**:
    - Stage 1: Stream raw CDC events to staging table (no aggregations)
    - Stage 2: Batch merge column family fragments to target table
    
    This function:
    - Reads Parquet CDC files from storage (Azure or UC Volume) using Auto Loader
    - Filters out .RESOLVED files and metadata
    - Transforms CockroachDB CDC columns (__crdb__*) to standard format
    - MERGES column family fragments (split_column_families=true) in batch mode
    - Writes all events (INSERT/UPDATE/DELETE) as rows to Delta table
    - Does NOT apply deletes or deduplicate updates (append_only)
    
    Use this for:
    - Audit logs with column family tables
    - Tables WITH column families (split_column_families=true)
    - Full history tracking with wide tables
    
    Args:
        config: Config dataclass from cockroachdb_config.py
        spark: SparkSession
        resolved_watermark: Optional RESOLVED timestamp in nanoseconds.
                           If provided, uses this watermark (for multi-table coordination).
                           If None, will compute from .RESOLVED files if enabled.
                           CRITICAL for multi-CF: Ensures all fragments are complete.
    
    Returns:
        StreamingQuery object
    """
    # Build paths using helper
    source_path, checkpoint_path, target_table_fqn = _build_paths(config)
    
    # Get primary key columns from config
    primary_key_columns = config.cdc_config.primary_key_columns
    
    # Print ingestion header
    _print_ingestion_header(config, "append_only", "multi_cf", source_path, target_table_fqn)
    print(f"Primary keys: {primary_key_columns}")
    print()
    
    # Resolve RESOLVED watermark (use provided or compute) - CRITICAL for column family completeness
    resolved_watermark = _resolve_watermark(
        config=config,
        source_table=config.tables.source_table_name,
        resolved_watermark=resolved_watermark,
        is_multi_cf=True,  # Multi-CF mode - CRITICAL for completeness
        spark=spark
    )
    
    # Read with Auto Loader
    raw_df = _setup_autoloader(spark, source_path, checkpoint_path, config.tables.source_table_name)
    
    print("✅ Schema inferred from data files")
    print()
    
    # Transform: CockroachDB CDC → Standard CDC format
    # Apply RESOLVED watermark if available (guarantees completeness)
    transformed_df = _transform_cdc_columns(raw_df, resolved_watermark=resolved_watermark)
    
    print("✅ CDC transformations applied (streaming compatible)")
    print("   ℹ️  Column family merge will happen in Stage 2 (batch mode)")
    print()
    
    # ========================================================================
    # STAGE 1: Stream to Staging Table (Serverless Compatible)
    # ========================================================================
    staging_table_fqn = f"{target_table_fqn}_staging_cf"
    
    print("🔷 STAGE 1: Streaming to staging table (no aggregations)")
    print(f"   Staging: {staging_table_fqn}")
    print()
    
    # Stream to staging using helper
    query = _stream_to_staging(transformed_df, checkpoint_path, staging_table_fqn)
    
    # ========================================================================
    # STAGE 2: Merge Column Families in Batch Mode
    # ========================================================================
    print("🔷 STAGE 2: Merging column family fragments (batch mode)")
    print(f"   Reading from staging: {staging_table_fqn}")
    print(f"   Writing to target: {target_table_fqn}")
    print()
    
    # Read staging table in batch mode
    staging_df = spark.table(staging_table_fqn)
    
    # Merge column family fragments (batch mode - no streaming limitations!)
    print("🔧 Merging column family fragments...")
    print(f"   Grouping by: {primary_key_columns} + _cdc_timestamp + _cdc_operation")
    print(f"   Using first(col, ignorenulls=True) to coalesce fragments")
    merged_df = merge_column_family_fragments(staging_df, primary_key_columns)
    print("✅ Column family fragments merged")
    print()
    
    # Write to final target table (batch mode, append_only)
    print(f"💾 Writing merged events to {target_table_fqn}...")
    merged_df.write.format("delta").mode("append").saveAsTable(target_table_fqn)
    print("✅ Append-only write complete")
    print()
    
    # Clean up staging table
    spark.sql(f"DROP TABLE IF EXISTS {staging_table_fqn}")
    print(f"🧹 Staging table dropped: {staging_table_fqn}")
    print()
    
    return query


def ingest_cdc_with_merge_multi_family(
    config,
    spark,
    resolved_watermark: Optional[int] = None
):
    """
    Ingest CDC events with MERGE logic and COLUMN FAMILY support.
    
    **Two-Stage Approach (Serverless Compatible)**:
    - Stage 1: Stream raw CDC events to staging table (no aggregations)
    - Stage 2: Batch merge column families + deduplicate + MERGE to target
    
    This function:
    - Reads Parquet CDC files from storage (Azure or UC Volume) using Auto Loader
    - Filters out .RESOLVED files and metadata
    - Transforms CockroachDB CDC columns (__crdb__*) to standard format
    - MERGES column family fragments (split_column_families=true) in batch mode
    - Streams to staging table (Serverless-compatible)
    - Deduplicates by primary key in batch mode
    - Applies MERGE logic to target Delta table
    
    Use this for:
    - Current state replication with column families
    - Tables WITH column families (split_column_families=true)
    - Production CDC with UPDATE/DELETE support
    
    Target table will contain:
    - All data columns from source
    - _cdc_operation: "UPSERT" (shows last operation)
    - _cdc_timestamp: Timestamp of last CDC event
    
    Args:
        config: Config dataclass from cockroachdb_config.py
        spark: SparkSession
        resolved_watermark: Optional RESOLVED timestamp in nanoseconds.
                           If provided, uses this watermark (for multi-table coordination).
                           If None, will compute from .RESOLVED files if enabled.
                           CRITICAL for multi-CF: Ensures all fragments are complete.
    
    Returns:
        Dict with query, staging_table, target_table, raw_count, deduped_count, merged
    """
    from pyspark.sql import functions as F, Window
    from delta.tables import DeltaTable
    
    # Build paths using helper (with "_merge_cf" suffix for checkpoint)
    source_path, checkpoint_path, target_table_fqn = _build_paths(config, mode_suffix="_merge_cf")
    
    # Get primary key columns from config
    primary_key_columns = config.cdc_config.primary_key_columns
    
    # Print ingestion header
    _print_ingestion_header(config, "update_delete", "multi_cf", source_path, target_table_fqn)
    print(f"Primary keys: {primary_key_columns}")
    print()
    
    # Resolve RESOLVED watermark (use provided or compute) - CRITICAL for column family completeness
    resolved_watermark = _resolve_watermark(
        config=config,
        source_table=config.tables.source_table_name,
        resolved_watermark=resolved_watermark,
        is_multi_cf=True,  # Multi-CF mode - CRITICAL for completeness
        spark=spark
    )
    
    # Read with Auto Loader
    raw_df = _setup_autoloader(spark, source_path, checkpoint_path, config.tables.source_table_name)
    
    print("✅ Schema inferred from data files")
    print()
    
    # Transform: CockroachDB CDC → Standard CDC format
    # Apply RESOLVED watermark if available (guarantees completeness)
    transformed_df = _transform_cdc_columns(raw_df, resolved_watermark=resolved_watermark)
    
    print("✅ CDC transformations applied (streaming compatible)")
    print("   ℹ️  Column family merge will happen in Stage 2 (batch mode)")
    print()
    
    # ========================================================================
    # STAGE 1: Stream to Staging Table (Serverless Compatible)
    # ========================================================================
    staging_table_fqn = f"{target_table_fqn}_staging_cf"
    
    print("🔷 STAGE 1: Streaming to staging table (no aggregations)")
    print(f"   Staging: {staging_table_fqn}")
    print()
    
    # Stream to staging using helper
    query = _stream_to_staging(transformed_df, checkpoint_path, staging_table_fqn)
    
    # ========================================================================
    # STAGE 2: Batch processing - CF merging + MERGE to target
    # ========================================================================
    
    # Read staging table for CF merging
    staging_df_raw = spark.read.table(staging_table_fqn)
    staging_count_raw = staging_df_raw.count()
    
    if staging_count_raw == 0:
        print("   ℹ️  No new events to process")
        return {"query": query, "staging_table": staging_table_fqn, "merged": 0}
    
    # Merge column family fragments (batch mode)
    # For SCD Type 1 (update_delete mode): Keep LATEST STATE exactly as-is (including NULLs)
    print(f"   🔧 Merging column family fragments (SCD Type 1 mode)...")
    print(f"      Step 1: Merge fragments within same CDC event")
    print(f"      Step 2: Deduplicate to keep latest state per key")
    print()
    
    # Step 1: Merge fragments within same timestamp (standard mode)
    staging_df_merged = merge_column_family_fragments(
        staging_df_raw, 
        primary_key_columns,
        deduplicate_to_latest_state=False,  # Standard merge, no cross-time coalescing
        debug=True  # Show merge statistics
    )
    
    fragments_merged = staging_count_raw - staging_df_merged.count()
    print(f"   ✅ Fragments merged: {fragments_merged}")
    print()
    
    # Step 2: Deduplication + MERGE using helper (pass merged DF)
    results = _merge_staging_to_target(
        spark, 
        staging_table_fqn, 
        target_table_fqn, 
        primary_key_columns,
        staging_df_raw=staging_df_merged  # Pass CF-merged DataFrame
    )
    
    staging_count = results["staging_count"]
    merged_count = results["merged_count"]
    duplicates_removed = results["duplicates_removed"]
    
    print("=" * 80)
    print("✅ CDC INGESTION COMPLETE (MERGE + COLUMN FAMILIES)")
    print("=" * 80)
    print(f"📊 Raw events: {staging_count_raw}")
    print(f"📊 After deduplication: {staging_count} unique events")
    print(f"📊 Staging table: {staging_table_fqn}")
    print(f"📊 Target table:  {target_table_fqn}")
    print()
    print("💡 TIP: Staging table can be dropped after successful MERGE:")
    print(f"   spark.sql('DROP TABLE IF EXISTS {staging_table_fqn}')")
    
    return {
        "query": query,
        "staging_table": staging_table_fqn,
        "target_table": target_table_fqn,
        "raw_count": staging_count_raw,
        "deduped_count": staging_count,
        "merged": merged_count
    }
