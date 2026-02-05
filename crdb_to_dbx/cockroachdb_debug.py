"""
CockroachDB CDC Debugging Utilities

This module provides debugging functions for investigating CDC pipeline issues,
particularly for multi-column family scenarios.

Available Functions:
    Core Diagnostic Functions:
        • run_full_diagnosis() - Comprehensive diagnosis of all sync issues
        • run_full_diagnosis_from_config() - Full diagnosis using notebook config dict
        • diagnose_column_family_sync() - Column family specific diagnosis
        • find_mismatched_rows() - Row-by-row comparison between source and target
    
    Quick Debug Functions:
        • quick_check_missing_keys() - Lightweight check for specific keys
        • inspect_target_table() - Comprehensive target table analysis
        • detailed_missing_keys_investigation() - Detailed key investigation
    
    Analysis Functions:
        • analyze_cdc_events_by_column_family() - Analyze CDC event distribution
        • check_merge_completeness() - Check staging table merge status
        • check_staging_and_storage_for_keys() - Check specific keys in staging/storage
        • inspect_raw_cdc_files() - Inspect raw CDC files for specific keys
    
    Helper Functions:
        • get_column_families() - Get column family assignments
        • compare_row_by_row() - Compare specific rows between source and target

Usage in notebook:
    from cockroachdb_debug import quick_check_missing_keys, inspect_target_table
    
    # Quick check for missing keys (primary_key_columns from config)
    quick_check_missing_keys(conn, spark, source_table, target_catalog,
                             target_schema, target_table, primary_key_columns, [17, 18, 19])
    
    # Comprehensive target analysis (primary_key_columns from config)
    inspect_target_table(spark, target_catalog, target_schema, target_table, primary_key_columns)
    
    # Full diagnosis
    run_full_diagnosis(conn, spark, source_table, target_df, staging_table,
                       storage_path, primary_keys, mismatched_columns, storage_type)
"""

import pg8000.native
from pyspark.sql import functions as F
from pyspark.sql import DataFrame
from typing import List, Dict, Any, Optional


class CDCValidationError(Exception):
    """
    Exception raised when CDC validation fails (source vs target mismatch).
    
    Attributes:
        message: Description of the validation failure
        mismatched_columns: List of column names that failed validation
        source_count: Row count in source table
        target_count: Row count in target table
        details: Additional details about the failure
    """
    def __init__(
        self,
        message: str,
        mismatched_columns: List[str] = None,
        source_count: int = None,
        target_count: int = None,
        details: Dict[str, Any] = None
    ):
        self.message = message
        self.mismatched_columns = mismatched_columns or []
        self.source_count = source_count
        self.target_count = target_count
        self.details = details or {}
        
        # Build detailed error message
        error_parts = [message]
        if mismatched_columns:
            error_parts.append(f"\nMismatched columns ({len(mismatched_columns)}): {', '.join(mismatched_columns)}")
        if source_count is not None and target_count is not None:
            error_parts.append(f"\nRow counts - Source: {source_count:,}, Target: {target_count:,}")
        if details:
            error_parts.append(f"\nDetails: {details}")
        
        super().__init__('\n'.join(error_parts))


def get_column_families(conn, table_name: str) -> Dict[str, List[str]]:
    """
    Get column family assignments for a CockroachDB table.
    
    Args:
        conn: pg8000 connection
        table_name: Table name (e.g., 'usertable_update_delete_multi_cf')
    
    Returns:
        Dict mapping column family names to lists of columns
    """
    # Clear any aborted transaction state
    try:
        conn.run("ROLLBACK")
    except:
        pass
    
    schema, table = 'public', table_name
    if '.' in table_name:
        parts = table_name.split('.')
        if len(parts) == 2:
            schema, table = parts
        elif len(parts) == 3:
            schema, table = parts[1], parts[2]
    
    # Simplified approach: Just return all columns in a 'default' family
    # Column family information is complex to query in CockroachDB and not critical for diagnosis
    query = f"""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = '{schema}'
          AND table_name = '{table}'
        ORDER BY ordinal_position;
    """
    
    try:
        rows = conn.run(query)
        conn.commit()  # Close read transaction
        # Return all columns as part of 'default' family for simplicity
        # In reality, column families would need SHOW CREATE TABLE parsing
        return {'default': [row[0] for row in rows]}
    except Exception as e:
        print(f"  ⚠️  Could not retrieve columns: {e}")
        return {}


def find_mismatched_rows(
    conn,
    spark,
    source_table: str,
    target_df,
    primary_keys: List[str],
    columns_to_check: List[str],
    is_append_only: bool = False
) -> bool:
    """
    Find exact rows and columns that have mismatches between source and target.
    
    Args:
        conn: CockroachDB connection
        spark: Spark session
        source_table: Fully qualified source table name
        target_df: Target Spark DataFrame
        primary_keys: List of primary key column names
        columns_to_check: List of column names to check for mismatches
        is_append_only: If True, deduplicates target to latest state before comparison
    
    Returns:
        Tuple of (has_mismatch: bool, target_extra_keys_count: int, target_extra_keys_list: list).
        target_extra_keys_count/list are set when target has rows that were deleted in source (update_delete mode only).
    """
    from pyspark.sql import functions as F
    
    missing_source_count = 0  # keys in target but not in source (target-extra rows)
    
    print("=" * 80)
    print("DETAILED MISMATCH ANALYSIS (Row-by-Row)")
    print("=" * 80)
    
    if is_append_only:
        print("\n📋 Mode: APPEND_ONLY")
        print("   Deduplicating target to latest state per key before comparison...")
    
    try:
        conn.run("ROLLBACK")
    except:
        pass
    
    # Get all source data
    print(f"\n📥 Fetching all rows from source: {source_table}")
    
    # Get column names from target (exclude metadata columns)
    # Exclude: _rescued_*, _file_*, _cdc_*, __crdb__*
    target_columns = [col for col in target_df.columns 
                      if not col.startswith('_rescued') 
                      and not col.startswith('_file')
                      and not col.startswith('_cdc')
                      and not col.startswith('__crdb__')]
    
    print(f"   Comparing {len(target_columns)} business columns (excluding metadata)")
    print(f"   Columns: {', '.join(target_columns)}")
    
    # Select only those columns from source
    column_list = ', '.join(target_columns)
    source_rows = conn.run(f"SELECT {column_list} FROM {source_table} ORDER BY {', '.join(primary_keys)}")
    conn.commit()  # Close read transaction
    
    # Get column names from first row (if any)
    if not source_rows:
        print(f"   ⚠️  Source table is empty!")
        return False, 0, []
    
    # Build schema from target for just the columns we selected
    from pyspark.sql.types import StructType
    filtered_schema = StructType([field for field in target_df.schema.fields 
                                   if field.name in target_columns])
    
    # Convert to Spark DataFrame
    source_df = spark.createDataFrame(source_rows, schema=filtered_schema)
    
    # Filter target to same columns
    target_df_filtered = target_df.select(*target_columns)
    
    # For append_only mode, deduplicate to latest state per key (requires __crdb__updated)
    if is_append_only:
        if '__crdb__updated' not in target_df.columns:
            raise ValueError(
                "Target table is missing __crdb__updated. Append-only comparison requires the connector "
                "timestamp column for deduplication. Ensure the target was written by the connector."
            )
        from cockroachdb_ycsb import deduplicate_to_latest
        target_df_latest = deduplicate_to_latest(target_df, primary_keys, verbose=True)
        target_df_filtered = target_df_latest.select(*target_columns)
    
    source_count = source_df.count()
    target_count = target_df_filtered.count()
    
    print(f"   Source rows: {source_count:,}")
    print(f"   Target rows: {target_count:,}")
    
    # Track if there's actual data loss
    source_keys_missing = False
    
    if source_count != target_count:
        print(f"\n⚠️  Row count mismatch!")
        
        # Find missing keys
        source_keys = source_df.select(*primary_keys)
        target_keys = target_df_filtered.select(*primary_keys)
        
        missing_in_target = source_keys.subtract(target_keys)
        missing_in_source = target_keys.subtract(source_keys)
        
        missing_target_count = missing_in_target.count()
        missing_source_count = missing_in_source.count()
        
        if missing_target_count > 0:
            source_keys_missing = True  # DATA LOSS: Source keys not in target
            print(f"\n   ❌ {missing_target_count} keys in source but NOT in target:")
            missing_in_target.show(10, truncate=False)
            
            if is_append_only:
                print(f"\n   🚨 CRITICAL: In append_only mode, ALL source keys must exist in target!")
                print(f"   This indicates data loss - changefeed failed to capture these rows.")
        
        if missing_source_count > 0:
            print(f"\n   ⚠️  {missing_source_count} keys in target but NOT in source:")
            missing_in_source.show(10, truncate=False)
            
            if is_append_only:
                print(f"\n   ℹ️  Note: In append_only mode, it's EXPECTED for target to have extra keys.")
                print(f"   These are likely deleted rows retained in the append-only log.")
            else:
                print(f"\n   ❌ In update_delete mode, target must match source exactly.")
                print(f"   These rows were DELETED in source but still exist in target (MERGE did not apply DELETEs).")
    
    # Check each column for value mismatches
    print(f"\n📊 Checking {len(columns_to_check)} columns for value mismatches...")
    
    any_mismatch = False
    for col in columns_to_check:
        if col not in source_df.columns or col not in target_df_filtered.columns:
            print(f"   ⚠️  {col}: Column not found in both DataFrames")
            continue
        
        # Join on primary keys and compare values
        pk_cols = primary_keys
        
        comparison = source_df.select(*pk_cols, F.col(col).alias("source_val")) \
            .join(
                target_df_filtered.select(*pk_cols, F.col(col).alias("target_val")),
                on=pk_cols,
                how="inner"
            ) \
            .filter(
                (F.col("source_val").isNotNull() & F.col("target_val").isNull()) |
                (F.col("source_val").isNull() & F.col("target_val").isNotNull()) |
                ((F.col("source_val").isNotNull() & F.col("target_val").isNotNull()) & 
                 (F.col("source_val") != F.col("target_val")))
            )
        
        mismatch_count = comparison.count()
        
        if mismatch_count > 0:
            any_mismatch = True
            print(f"\n   ❌ {col}: {mismatch_count} rows with different values")
            print(f"      Showing first 10 mismatched rows:")
            comparison.orderBy(*pk_cols).show(10, truncate=False)
        else:
            print(f"   ✅ {col}: All values match")
    
    # In update_delete mode, target having extra keys (deleted in source) is a sync failure
    target_extra_keys_sync_failure = (not is_append_only and missing_source_count > 0)
    has_mismatch = any_mismatch or source_keys_missing or target_extra_keys_sync_failure
    
    # Collect list of keys in target but not in source (for DELETE investigation)
    target_extra_keys_list = []
    if missing_source_count > 0:
        target_extra_keys_list = [row[primary_keys[0]] for row in missing_in_source.collect()]
    
    if not any_mismatch and not source_keys_missing and not target_extra_keys_sync_failure:
        print(f"\n✅ All {len(columns_to_check)} columns match perfectly across all rows!")
        print(f"✅ All source keys exist in target!")
        if not is_append_only:
            print(f"✅ Target has no extra rows (DELETEs applied).")
        return False, 0, []  # No mismatches found
    elif source_keys_missing:
        print(f"\n❌ CRITICAL: Source keys are missing from target!")
        if any_mismatch:
            print(f"❌ ALSO: Found value mismatches in columns")
        return True, missing_source_count if not is_append_only else 0, []
    elif target_extra_keys_sync_failure:
        print(f"\n❌ SYNC FAILURE: Target has {missing_source_count} rows that were DELETED in source.")
        if any_mismatch:
            print(f"❌ ALSO: Found value mismatches in columns")
        return True, missing_source_count, target_extra_keys_list
    else:
        print(f"\n⚠️  Found value mismatches - see details above")
        return True, 0, []


def compare_row_by_row(
    conn,
    source_table: str,
    target_df: DataFrame,
    primary_keys: List[str],
    columns_to_check: List[str],
    limit: int = 10,
    cdc_mode: str = "append_only"
) -> None:
    """
    Compare specific rows between source and target to identify discrepancies.
    Checks SOURCE → TARGET (verifying all source rows made it to target).
    
    Args:
        conn: CockroachDB connection
        source_table: Source table name
        target_df: Target Spark DataFrame
        primary_keys: List of primary key columns
        columns_to_check: Columns to compare (e.g., ['field3', 'field4', ...])
        limit: Number of rows to compare
        cdc_mode: CDC mode ('append_only' or 'update_delete')
    """
    # Clear any aborted transaction state
    try:
        conn.run("ROLLBACK")
    except:
        pass
    
    if cdc_mode == "append_only" and "__crdb__updated" not in target_df.columns:
        raise ValueError(
            "Target table is missing __crdb__updated. Append-only row comparison requires it to select the latest row per key."
        )
    print("=" * 80)
    print("ROW-BY-ROW COMPARISON (Source → Target)")
    print("=" * 80)
    if cdc_mode == "append_only":
        print("\n📝 Mode: APPEND_ONLY")
        print("   Comparing source against LATEST target row (by __crdb__updated)")
    else:
        print("\n📝 Mode: UPDATE_DELETE")
        print("   Comparing source against target (should be 1:1 match)")
    print()
    
    # Get sample keys from SOURCE (not target)
    pk_list = ", ".join(primary_keys)
    source_keys_query = f"SELECT {pk_list} FROM {source_table} LIMIT {limit}"
    source_keys_result = conn.run(source_keys_query)
    conn.commit()  # Close read transaction
    
    if not source_keys_result:
        print("\n⚠️  No rows found in source table")
        return
    
    for source_key_row in source_keys_result:
        key_values = {pk: source_key_row[i] for i, pk in enumerate(primary_keys)}
        key_str = ", ".join(f"{k}={v}" for k, v in key_values.items())
        
        # Get source row with all columns to check
        where_clause = " AND ".join(f"{k} = {v}" for k, v in key_values.items())
        source_query = f"SELECT {', '.join(columns_to_check)} FROM {source_table} WHERE {where_clause}"
        
        # pg8000.native uses connection directly (no cursor)
        result = conn.run(source_query)
        conn.commit()  # Close read transaction
        source_row = result[0] if result else None
        
        # Get target row (latest for append_only mode)
        target_filter = " AND ".join(f"{k} == {v}" for k, v in key_values.items())
        target_filtered = target_df.filter(target_filter)
        
        # For append_only mode, get the LATEST row by __crdb__updated (required; checked above)
        if cdc_mode == "append_only":
            target_filtered = target_filtered.orderBy(F.col("__crdb__updated").desc())
        
        target_row = target_filtered.select(*columns_to_check).first()
        
        if not target_row:
            severity = "🚨" if cdc_mode == "update_delete" else "⚠️"
            print(f"\n{severity} Key ({key_str}): Row missing in TARGET")
            if cdc_mode == "append_only":
                print(f"     This indicates CDC events for this key didn't sync properly")
            else:
                print(f"     This is a data loss issue - source row not replicated to target")
            continue
        
        if not source_row:
            # This shouldn't happen since we got the key from source, but handle it
            print(f"\n⚠️  Key ({key_str}): Unable to read source row")
            continue
        
        # Compare columns
        mismatches = []
        for i, col in enumerate(columns_to_check):
            source_val = source_row[i]
            target_val = target_row[col]
            
            # Check for NULL mismatches first
            source_is_null = source_val is None or source_val == ''
            target_is_null = target_val is None or target_val == ''
            
            if source_is_null != target_is_null:
                # One is NULL, the other is not - MISMATCH
                mismatches.append(f"{col}: {source_val} vs {target_val}")
                continue
            
            # If both are NULL, they match
            if source_is_null and target_is_null:
                continue
            
            # Both have values - strip non-numeric for comparison
            import re
            source_num = int(re.sub(r'[^0-9]', '', str(source_val)) or '0')
            target_num = int(re.sub(r'[^0-9]', '', str(target_val)) or '0')
            
            if source_num != target_num:
                mismatches.append(f"{col}: {source_num} vs {target_num}")
        
        if mismatches:
            print(f"\n❌ Key ({key_str}):")
            for m in mismatches:
                print(f"   {m}")
        else:
            print(f"\n✅ Key ({key_str}): All columns match")


def analyze_cdc_events_by_column_family(
    spark,
    storage_path: str,
    table_name: str,
    primary_keys: List[str],
    storage_type: str = "azure"
) -> None:
    """
    Analyze CDC events from storage to check column family fragment distribution.
    
    Supports both Azure Blob Storage and Unity Catalog External Volumes.
    
    Args:
        spark: Spark session
        storage_path: Path to changefeed files (Azure or UC Volume)
        table_name: Table name to filter
        primary_keys: List of primary key columns
        storage_type: Type of storage ("azure" or "uc_volume")
    """
    storage_label = "Unity Catalog Volume" if storage_type == "uc_volume" else "Azure"
    
    print("=" * 80)
    print("CDC EVENT ANALYSIS (Column Family Distribution)")
    print("=" * 80)
    
    print(f"\n📁 Storage path ({storage_label}): {storage_path}")
    
    try:
        # Use batch read with pathGlobFilter (same pattern as notebook's Auto Loader)
        # This excludes .RESOLVED files which have incompatible schema
        # CRITICAL: Use mergeSchema=true to combine schemas from all column family fragments
        df_raw = (spark.read
            .format("parquet")
            .option("pathGlobFilter", f"*{table_name}*.parquet")  # Match notebook pattern
            .option("recursiveFileLookup", "true")
            .option("mergeSchema", "true")  # ← Merge schemas from all column families
            .load(storage_path)
        )
        
        print(f"✅ Schema inferred from Parquet files")
        print(f"   Filter: *{table_name}*.parquet (excludes .RESOLVED files)")
        print(f"   📋 Columns detected: {len(df_raw.columns)} columns")
        
        # Count total events
        total_events = df_raw.count()
        print(f"📊 Total CDC events: {total_events:,}")
        
        if total_events == 0:
            print(f"  ℹ️  No CDC events found (changefeed may not be running yet)")
            return
        
        # Analyze by operation
        print("\n📊 Events by Operation:")
        df_raw.groupBy("__crdb__event_type").count().orderBy("__crdb__event_type").show()
        
        # Check for null columns (indicator of column family fragments)
        print("\n📊 Column Completeness (NULL count per column):")
        
        # Get all columns except metadata
        data_cols = [c for c in df_raw.columns if not c.startswith('__crdb__')]
        
        null_counts = []
        for col in data_cols:
            null_count = df_raw.filter(F.col(col).isNull()).count()
            null_counts.append((col, null_count, f"{null_count/total_events*100:.1f}%"))
        
        for col, count, pct in sorted(null_counts, key=lambda x: x[1], reverse=True):
            indicator = "⚠️ " if count > 0 else "✅"
            print(f"  {indicator} {col:15s}: {count:6,} NULL ({pct})")
        
        # Analyze UPDATE events specifically
        print("\n📊 UPDATE Events Analysis:")
        df_updates = df_raw.filter(F.col("__crdb__event_type") == "c")
        update_count = df_updates.count()
        print(f"  Total UPDATE events: {update_count:,}")
        
        if update_count > 0:
            print(f"\n  Column completeness in UPDATE events:")
            for col in data_cols:
                null_in_updates = df_updates.filter(F.col(col).isNull()).count()
                pct = null_in_updates / update_count * 100
                indicator = "⚠️ " if null_in_updates > 0 else "✅"
                print(f"    {indicator} {col:15s}: {null_in_updates:6,} NULL ({pct:.1f}%)")
        
        # Check for fragmentation (multiple events with same PK and timestamp)
        print("\n📊 Column Family Fragmentation Check:")
        if primary_keys and primary_keys[0] in df_raw.columns:
            pk_col = primary_keys[0]
            
            # Group by PK and timestamp, count fragments
            df_grouped = df_raw.groupBy(pk_col, "__crdb__updated").count()
            df_multi_fragments = df_grouped.filter(F.col("count") > 1)
            
            fragmented_keys = df_multi_fragments.count()
            if fragmented_keys > 0:
                print(f"  ⚠️  Found {fragmented_keys} keys with multiple fragments")
                print(f"  Sample fragmented keys:")
                df_multi_fragments.orderBy(F.col("count").desc()).show(5)
            else:
                print(f"  ✅ No fragmentation detected (all keys have single events)")
        
    except Exception as e:
        print(f"❌ Error analyzing CDC events: {e}")


def check_merge_completeness(
    spark,
    staging_table: str,
    primary_keys: List[str]
) -> None:
    """
    Check if column family fragments were properly merged in staging table.
    
    Args:
        spark: Spark session
        staging_table: Fully qualified staging table name
        primary_keys: List of primary key columns
    """
    print("=" * 80)
    print("MERGE COMPLETENESS CHECK (Staging Table)")
    print("=" * 80)
    
    try:
        df = spark.read.table(staging_table)
        total_rows = df.count()
        
        print(f"\n📊 Total rows in staging: {total_rows:,}")
        
        if total_rows == 0:
            print(f"  ℹ️  Staging table is empty - data has been merged to target")
            return
        
        # Check for NULL values in each column
        print(f"\n📊 NULL values by column:")
        
        all_cols = df.columns
        data_cols = [c for c in all_cols if not c.startswith('_cdc_')]
        
        for col in sorted(data_cols):
            null_count = df.filter(F.col(col).isNull()).count()
            pct = null_count / total_rows * 100
            indicator = "⚠️ " if null_count > 0 else "✅"
            print(f"  {indicator} {col:15s}: {null_count:6,} NULL ({pct:.1f}%)")
        
        # Check for duplicate primary keys (merge incomplete)
        if primary_keys:
            print(f"\n📊 Duplicate primary key check:")
            df_dup_check = df.groupBy(*primary_keys).count()
            df_dups = df_dup_check.filter(F.col("count") > 1)
            
            dup_count = df_dups.count()
            if dup_count > 0:
                print(f"  ⚠️  Found {dup_count} duplicate primary keys!")
                print(f"  Sample duplicates:")
                df_dups.orderBy(F.col("count").desc()).show(5)
            else:
                print(f"  ✅ No duplicate primary keys (merge successful)")
        
    except Exception as e:
        print(f"❌ Error checking merge completeness: {e}")


def diagnose_column_family_sync(
    conn,
    source_table: str,
    target_df: DataFrame,
    primary_keys: List[str],
    mismatched_columns: List[str] = None,
    cdc_mode: str = "append_only"
) -> None:
    """
    Comprehensive diagnosis of column family sync issues.
    
    Args:
        conn: CockroachDB connection
        source_table: Source table name
        target_df: Target Spark DataFrame
        primary_keys: List of primary key columns
        mismatched_columns: Columns with sum mismatches (e.g., ['field3', 'field4', ...])
        cdc_mode: CDC mode ('append_only' or 'update_delete')
    """
    print("\n" + "=" * 80)
    print("COLUMN FAMILY SYNC DIAGNOSIS")
    print("=" * 80)
    
    # For append_only mode, recalculate actual mismatches after deduplication
    actual_mismatched_columns = None
    if cdc_mode == "append_only" and mismatched_columns:
        print("\n📝 Recalculating column mismatches after deduplication...")
        
        # Deduplicate target to latest state
        from cockroachdb_ycsb import deduplicate_to_latest, get_column_sum_spark
        target_df_dedup = deduplicate_to_latest(target_df, primary_keys, verbose=False)
        
        # Get all column names (exclude metadata)
        business_columns = [col for col in target_df.columns 
                           if not col.startswith('_') and not col.startswith('__crdb__')]
        
        # Recalculate which columns actually mismatch
        from cockroachdb_ycsb import get_column_sum
        
        actual_mismatched = []
        for col in business_columns:
            try:
                source_sum = get_column_sum(conn, source_table, col)
                target_sum = get_column_sum_spark(target_df_dedup, col)
                
                if source_sum != target_sum:
                    actual_mismatched.append(col)
            except Exception:
                # Skip columns that can't be summed (non-numeric)
                pass
        
        actual_mismatched_columns = actual_mismatched
        
        if actual_mismatched:
            print(f"   ⚠️  Found {len(actual_mismatched)} columns with actual mismatches after deduplication")
            print(f"   Columns: {', '.join(actual_mismatched)}")
        else:
            print(f"   ✅ No mismatches found after deduplication!")
            print(f"   Note: {len(mismatched_columns)} columns were reported as mismatched,")
            print(f"         but they match after deduplicating target to latest state.")
    else:
        actual_mismatched_columns = mismatched_columns
    
    # 1. Get column family assignments
    print("\n1️⃣  Column Family Assignments & Sync Status:")
    print("-" * 80)
    print("\n  Legend:")
    print("     ✅ = Column syncing correctly")
    print("     ❌ = Column has sync issues (mismatched values)")
    if cdc_mode == "append_only":
        print("     (Based on comparison after deduplicating target to latest state)")
    print()
    
    try:
        families = get_column_families(conn, source_table)
        for family_name, columns in families.items():
            print(f"  📁 Family '{family_name}':")
            for col in columns:
                indicator = "❌" if actual_mismatched_columns and col in actual_mismatched_columns else "✅"
                print(f"     {indicator} {col}")
    except Exception as e:
        print(f"  ⚠️  Could not retrieve column families: {e}")
    
    # 2. Compare sample rows
    if actual_mismatched_columns:
        print("\n2️⃣  Sample Row Comparison:")
        print("-" * 80)
        compare_row_by_row(
            conn,
            source_table,
            target_df,
            primary_keys,
            actual_mismatched_columns[:3],  # Check first 3 mismatched columns
            limit=5,
            cdc_mode=cdc_mode
        )
    elif mismatched_columns and not actual_mismatched_columns:
        print("\n2️⃣  Sample Row Comparison:")
        print("-" * 80)
        print("  ✅ No actual mismatches found after deduplication - skipping row comparison")
    
    # 3. Check for pattern in mismatches
    print("\n3️⃣  Mismatch Pattern Analysis:")
    print("-" * 80)
    
    if actual_mismatched_columns:
        # Get sum differences
        print(f"\n  Analyzing {len(actual_mismatched_columns)} mismatched columns...")
        print(f"  Columns: {', '.join(actual_mismatched_columns)}")
        
        # Hypothesis: Missing UPDATE events
        source_count = target_df.count()
        print(f"\n  💡 Hypothesis: Missing UPDATE events for column families")
        print(f"     If some UPDATE events didn't merge properly, those columns")
        print(f"     would retain old values instead of updated values.")
    elif mismatched_columns and not actual_mismatched_columns:
        print(f"\n  ✅ No actual mismatches after deduplication!")
        print(f"     {len(mismatched_columns)} columns were initially flagged as mismatched,")
        print(f"     but all values match after deduplicating target to latest state.")
        print(f"\n  💡 Conclusion: The data is in sync!")
        print(f"     The initial mismatches were due to comparing:")
        print(f"     - Source: current state (1 row per key)")
        print(f"     - Target: full CDC log (multiple rows per key)")
        print(f"     After deduplication, they match perfectly.")
    else:
        print(f"\n  ✅ No mismatches reported")


def check_staging_and_storage_for_keys(
    spark,
    staging_table: str,
    storage_path: str,
    table_name: str,
    primary_keys: List[str],
    mismatched_keys: List[Any],
    columns_to_check: List[str],
    storage_type: str = "azure"
) -> bool:
    """
    Check if specific keys exist in the staging table or raw CDC files in storage.
    
    Supports both Azure Blob Storage and Unity Catalog External Volumes.
    
    Returns:
        bool: True if the mismatched data was found in staging (MERGE issue),
              False if data is missing from storage (changefeed issue)
    
    Args:
        spark: Spark session
        staging_table: Fully qualified staging table name
        storage_path: Path to CDC files (Azure abfss:// or UC Volume /Volumes/)
        table_name: Table name (for file filtering)
        primary_keys: List of primary key column names
        mismatched_keys: List of primary key values that are mismatched
        columns_to_check: List of columns to display
        storage_type: "azure" or "uc_volume" (default: "azure")
    """
    from pyspark.sql import functions as F
    
    storage_label = "Unity Catalog Volume" if storage_type == "uc_volume" else "Azure"
    
    print("=" * 80)
    print(f"STAGING & STORAGE CHECK (for mismatched keys)")
    print("=" * 80)
    
    # Convert mismatched_keys to Python native types (avoid Spark/numpy types)
    mismatched_keys_native = [int(k) if hasattr(k, '__int__') else k for k in mismatched_keys]
    
    print(f"\n🔑 Mismatched keys: {mismatched_keys_native}")
    
    data_found_in_staging = False
    
    # 1. Check staging table
    try:
        staging_df = spark.read.table(staging_table)
        current_count = staging_df.count()
        
        print(f"\n📊 Current staging table: {current_count} rows")
        
        if current_count > 0:
            print(f"   🔍 Checking for mismatched keys in staging...")
            
            # Filter for mismatched keys (use native Python types)
            key_filter = F.col(primary_keys[0]).isin(mismatched_keys_native)
            matched_rows = staging_df.filter(key_filter)
            matched_count = matched_rows.count()
            
            if matched_count > 0:
                print(f"   ✅ Found {matched_count} rows with these keys in staging:")
                cols_to_show = primary_keys + columns_to_check[:3] + ['__crdb__event_type', '__crdb__updated']
                matched_rows.select(*[c for c in cols_to_show if c in staging_df.columns]).show(10, truncate=False)
                
                # Check if any of the matched rows have non-NULL values for the mismatched columns
                for col in columns_to_check[:3]:  # Check first 3 columns
                    if col in staging_df.columns:
                        non_null_count = matched_rows.filter(F.col(col).isNotNull()).count()
                        if non_null_count > 0:
                            data_found_in_staging = True
                            break
            else:
                print(f"   ⚠️  No rows with these keys found in current staging")
        else:
            print(f"   ℹ️  Staging is empty - all CDC events already merged")
        
    except Exception as e:
        print(f"   ❌ Error checking staging: {e}")
    
    # 2. Check raw CDC files in storage
    print(f"\n📁 Checking raw CDC files in {storage_label}...")
    
    try:
        # Read raw Parquet files (same pattern as CDC analysis)
        # CRITICAL: Use mergeSchema=true to combine schemas from all column family fragments
        df_raw = (spark.read
            .format("parquet")
            .option("pathGlobFilter", f"*{table_name}*.parquet")
            .option("recursiveFileLookup", "true")
            .option("mergeSchema", "true")  # ← Merge schemas from all column families
            .load(storage_path)
        )
        
        total_events = df_raw.count()
        print(f"   Total CDC events in {storage_label}: {total_events:,}")
        print(f"   📋 Columns detected: {len(df_raw.columns)} columns")
        print(f"      {', '.join([c for c in df_raw.columns if not c.startswith('_')])}")
        
        # Filter for mismatched keys (use native Python types)
        key_filter = F.col(primary_keys[0]).isin(mismatched_keys_native)
        matched_events = df_raw.filter(key_filter)
        matched_count = matched_events.count()
        
        if matched_count > 0:
            print(f"   ✅ Found {matched_count} CDC events for these keys in {storage_label}:")
            
            # Show ALL columns in the raw events (including NULLs) to see column family fragments
            print(f"\n   📊 All {matched_count} events for key(s) {mismatched_keys}:")
            matched_events.orderBy(primary_keys[0], '__crdb__updated').show(20, truncate=False)
            
            # Check column completeness for these specific keys
            print(f"\n   📊 Column completeness for mismatched keys:")
            all_cols = [primary_keys[0], 'field0', 'field1', 'field2'] + columns_to_check
            for col in all_cols:
                if col in df_raw.columns:
                    null_count = matched_events.filter(F.col(col).isNull()).count()
                    non_null_count = matched_count - null_count
                    indicator = "✅" if non_null_count > 0 else "❌"
                    print(f"      {indicator} {col:10s}: {non_null_count:3d} with values, {null_count:3d} NULL")
                else:
                    print(f"      ⚠️  {col:10s}: Column not in CDC files")
            
            # Interpretation
            missing_cols = [col for col in columns_to_check if col not in df_raw.columns or 
                           matched_events.filter(F.col(col).isNotNull()).count() == 0]
            if missing_cols:
                print(f"\n   💡 Analysis:")
                print(f"      • {len(missing_cols)} mismatched columns have NO events with values in {storage_label}")
                print(f"      • Missing columns: {', '.join(missing_cols[:5])}")
                print(f"      • This suggests the column family fragment for these columns never made it to {storage_label}")
                print(f"      • Likely cause: Changefeed didn't capture the INSERT for this column family")
        else:
            print(f"   ⚠️  No CDC events found for these keys in {storage_label}!")
            print(f"   💡 This suggests the changefeed may not have captured these events")
        
    except Exception as e:
        print(f"   ❌ Error checking storage files: {e}")
        import traceback
        traceback.print_exc()
    
    return data_found_in_staging


def inspect_raw_cdc_files(
    spark,
    storage_path: str,
    primary_key: str,
    key_value: Any,
    show_content: bool = True,
    storage_type: str = "azure"
) -> None:
    """
    Inspect raw CDC files for a specific key to see all fragments.
    
    Supports both Azure Blob Storage and Unity Catalog External Volumes.
    
    Args:
        spark: Spark session
        storage_path: Path to changefeed files (Azure or UC Volume)
        primary_key: Primary key column name
        key_value: Specific key value to inspect
        show_content: Whether to show full column values
        storage_type: Type of storage ("azure" or "uc_volume")
    """
    storage_label = "Unity Catalog Volume" if storage_type == "uc_volume" else "Azure"
    
    print("=" * 80)
    print(f"RAW CDC FILE INSPECTION ({storage_label}, Key: {primary_key}={key_value})")
    print("=" * 80)
    
    try:
        # CRITICAL: Use mergeSchema=true to combine schemas from all column family fragments
        df_raw = (spark.read
            .format("parquet")
            .option("mergeSchema", "true")
            .load(storage_path)
        )
        df_key = df_raw.filter(F.col(primary_key) == key_value)
        
        count = df_key.count()
        print(f"\n📊 Found {count} CDC events for this key")
        print(f"   📋 Total columns available: {len(df_raw.columns)}")
        
        if count > 0:
            # Show events ordered by timestamp
            df_sorted = df_key.orderBy("__crdb__updated")
            
            print(f"\n📄 Events (ordered by timestamp):")
            if show_content:
                df_sorted.show(truncate=False, vertical=True)
            else:
                # Show summary
                summary_cols = [primary_key, "__crdb__event_type", "__crdb__updated"] + \
                               [c for c in df_key.columns if c.startswith('field')]
                df_sorted.select(*summary_cols).show(truncate=40)
            
            # Analyze fragments
            print(f"\n📊 Fragment Analysis:")
            fragments = df_sorted.collect()
            
            for i, row in enumerate(fragments, 1):
                event_type = row["__crdb__event_type"]
                timestamp = row["__crdb__updated"]
                
                # Count non-null data columns
                data_cols = [c for c in row.asDict().keys() 
                            if not c.startswith('__crdb__') and c != primary_key]
                non_null = sum(1 for c in data_cols if row[c] is not None)
                
                print(f"  Fragment {i}: {event_type} @ {timestamp} - {non_null}/{len(data_cols)} columns populated")
                
    except Exception as e:
        print(f"❌ Error inspecting raw CDC files: {e}")


def investigate_delete_sync(
    spark,
    staging_table: str,
    target_df: DataFrame,
    primary_keys: List[str],
    target_extra_keys: List,
) -> None:
    """
    Investigate why rows that were DELETED in source still exist in target (update_delete mode).
    For each key in target_extra_keys, checks staging table: is there a DELETE event? Is it the latest?
    Compares with target row to determine why MERGE did not remove the row.
    """
    from pyspark.sql import functions as F
    from pyspark.sql.window import Window

    if not target_extra_keys:
        return

    print("\n" + "=" * 80)
    print("🔍 DELETE INVESTIGATION: Why are these rows still in target?")
    print("=" * 80)
    print(f"   Keys in target but deleted in source: {target_extra_keys[:20]}{'...' if len(target_extra_keys) > 20 else ''}")
    print()

    try:
        staging_df = spark.read.table(staging_table)
    except Exception as e:
        print(f"   ⚠️  Could not read staging table {staging_table}: {e}")
        return

    pk_col = primary_keys[0]
    # Latest event per key in staging (same dedup logic as MERGE)
    window_spec = Window.partitionBy(pk_col).orderBy(F.col("__crdb__updated").desc())
    staging_latest = (staging_df
        .withColumn("_rn", F.row_number().over(window_spec))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )

    # For each extra key: what does staging say (latest op) vs target?
    target_latest = target_df.select(pk_col, "__crdb__updated", "_cdc_operation")
    keys_to_show = target_extra_keys[:15]  # Limit output

    print("   Per-key: latest event in STAGING vs row in TARGET")
    print("-" * 80)
    for key in keys_to_show:
        st_row = staging_latest.filter(F.col(pk_col) == key).collect()
        tg_row = target_latest.filter(F.col(pk_col) == key).collect()
        st_ops = [r["_cdc_operation"] for r in st_row] if st_row else []
        st_ts = [r["__crdb__updated"] for r in st_row] if st_row else []
        tg_ops = [r["_cdc_operation"] for r in tg_row] if tg_row else []
        tg_ts = [r["__crdb__updated"] for r in tg_row] if tg_row else []
        if st_row:
            st_op, st_ts_val = st_ops[0], st_ts[0]
            if tg_row:
                tg_op, tg_ts_val = tg_ops[0], tg_ts[0]
                if st_op == "DELETE":
                    print(f"   Key {key}: Staging latest = DELETE @ {st_ts_val}")
                    print(f"            Target has     = {tg_op} @ {tg_ts_val}")
                    if tg_op == "DELETE":
                        print(f"            → Target row is a DELETE row; MERGE should have removed it (whenMatchedDelete).")
                        print(f"              Use whenMatchedDelete *before* whenMatchedUpdate in MERGE so DELETEs are applied first.")
                    print(f"            → Possible causes: MERGE clause order (put whenMatchedDelete first), run order, or re-run ingestion.")
                else:
                    print(f"   Key {key}: Staging latest = {st_op} (not DELETE) @ {st_ts_val}")
                    print(f"            Target has     = {tg_op} @ {tg_ts_val}")
                    print(f"            → After dedup, latest event for this key is {st_op}, so MERGE did not delete.")
                    print(f"              If source deleted this key, the DELETE event may be in a later batch not yet merged.")
            else:
                print(f"   Key {key}: Staging latest = {st_op} @ {st_ts_val}; Target: no row (already deleted?)")
        else:
            print(f"   Key {key}: NOT in staging → DELETE event never reached staging (watermark filter or stream not yet run).")
    print("-" * 80)

    # Summary counts: how many of target_extra_keys have DELETE as latest in staging?
    keys_df = spark.createDataFrame([(k,) for k in target_extra_keys], [pk_col])
    joined = keys_df.join(staging_latest.select(pk_col, "_cdc_operation"), on=pk_col, how="left")
    with_delete_latest = joined.filter(F.col("_cdc_operation") == "DELETE").count()
    not_in_staging = joined.filter(F.col("_cdc_operation").isNull()).count()
    other_latest = len(target_extra_keys) - with_delete_latest - not_in_staging

    print("\n   Summary (why DELETEs didn't remove these rows):")
    print(f"   • {with_delete_latest} key(s): Staging has DELETE as latest → MERGE should have deleted (re-run MERGE?)")
    print(f"   • {not_in_staging} key(s): Key not in staging → DELETE never landed in staging (watermark or stream order)")
    print(f"   • {other_latest} key(s): Staging has UPSERT as latest → DELETE was overwritten by later event in same batch/dedup")
    print("=" * 80)


# Convenience function to run all diagnostics
def run_full_diagnosis(
    conn,
    spark,
    source_table: str,
    target_df: DataFrame,
    staging_table: str,
    storage_path: str,
    primary_keys: List[str],
    mismatched_columns: List[str],
    storage_type: str = "azure"
) -> None:
    """
    Run comprehensive diagnosis of CDC sync issues.
    
    Automatically detects CDC mode:
    - If staging_table exists → update_delete mode (two-stage ingestion)
    - If staging_table is None → append_only mode (direct ingestion)
    
    Supports both Azure Blob Storage and Unity Catalog External Volumes.
    
    Args:
        conn: CockroachDB connection
        spark: Spark session
        source_table: Source table name
        target_df: Target Spark DataFrame
        staging_table: Staging table name (None for append_only mode)
        storage_path: Path to changefeed files (Azure or UC Volume)
        primary_keys: List of primary key columns
        mismatched_columns: Columns with sum mismatches
        storage_type: Type of storage ("azure" or "uc_volume")
    """
    # Clear any aborted transaction state from previous errors
    try:
        conn.run("ROLLBACK")
    except:
        pass  # Ignore if no transaction to rollback
    
    # Detect CDC mode based on staging table presence
    is_append_only = False
    if staging_table:
        # Check if staging table actually exists
        try:
            spark.table(staging_table).limit(1).collect()
            print(f"📋 CDC Mode: UPDATE_DELETE (two-stage ingestion via staging table)")
        except Exception:
            print(f"📋 CDC Mode: APPEND_ONLY (no staging table found)")
            is_append_only = True
            staging_table = None
    else:
        print(f"📋 CDC Mode: APPEND_ONLY (direct ingestion to target)")
        is_append_only = True
    
    print("\n" + "█" * 80)
    print("🔍 FULL CDC SYNC DIAGNOSIS")
    print("█" * 80)
    
    # 1. Column family diagnosis
    cdc_mode_str = "append_only" if is_append_only else "update_delete"
    diagnose_column_family_sync(conn, source_table, target_df, primary_keys, mismatched_columns, cdc_mode=cdc_mode_str)
    
    # 2. Check merge completeness (only for update_delete mode)
    if staging_table and not is_append_only:
        check_merge_completeness(spark, staging_table, primary_keys)
    
    # 3. Analyze raw CDC events
    if storage_path:
        # Extract just the table name from fully qualified name (e.g., "catalog.schema.table" -> "table")
        table_name = source_table.split('.')[-1] if '.' in source_table else source_table
        analyze_cdc_events_by_column_family(spark, storage_path, table_name, primary_keys, storage_type)
    
    # 4. Detailed mismatch analysis (if mismatches were reported)
    actual_mismatch_found = False
    target_extra_keys_count = 0
    missing_source_keys = []
    data_in_staging = False
    if mismatched_columns:
        print(f"\n")
        actual_mismatch_found, target_extra_keys_count, target_extra_keys_list = find_mismatched_rows(
            conn, spark, source_table, target_df, primary_keys, mismatched_columns, 
            is_append_only=is_append_only
        )
        
        # For update_delete: investigate why DELETEs weren't applied (staging vs target)
        if target_extra_keys_count > 0 and staging_table and not is_append_only:
            investigate_delete_sync(spark, staging_table, target_df, primary_keys, target_extra_keys_list)
        
        # For append_only mode: Check storage for missing source keys
        storage_label = "Unity Catalog Volume" if storage_type == "uc_volume" else "Azure"
        if actual_mismatch_found and is_append_only and storage_path:
            print(f"\n")
            print(f"🔍 Investigating missing source keys in {storage_label} CDC files...")
            
            # Get source keys that are missing from target
            source_df_cols = ', '.join(primary_keys)
            source_rows = conn.run(f"SELECT {source_df_cols} FROM {source_table}")
            conn.commit()  # Close read transaction
            from pyspark.sql.types import StructType
            pk_schema = StructType([field for field in target_df.schema.fields if field.name in primary_keys])
            source_keys_df = spark.createDataFrame(source_rows, schema=pk_schema)
            
            target_keys_df = target_df.select(*primary_keys).distinct()
            missing_keys_df = source_keys_df.subtract(target_keys_df)
            missing_source_keys = [row[primary_keys[0]] for row in missing_keys_df.collect()]
            
            if missing_source_keys:
                print(f"   Found {len(missing_source_keys)} source keys missing from target")
                print(f"   Missing keys: {missing_source_keys[:20]}")  # Show first 20
                
                # Check storage for these missing keys
                table_name = source_table.split('.')[-1]
                check_staging_and_storage_for_keys(
                    spark, None, storage_path, table_name,
                    primary_keys, missing_source_keys[:10], mismatched_columns,  # Check first 10 keys
                    storage_type=storage_type
                )
        
        # For update_delete mode: Check if mismatched keys exist in staging or storage
        elif actual_mismatch_found and staging_table:
            # Get the mismatched keys from target (filter by NULL in mismatched columns)
            from pyspark.sql import functions as F
            
            mismatch_filter = F.col(mismatched_columns[0]).isNull()
            for col in mismatched_columns[1:]:
                mismatch_filter = mismatch_filter | F.col(col).isNull()
            
            mismatched_keys_df = target_df.filter(mismatch_filter).select(*primary_keys).distinct()
            mismatched_keys = [row[primary_keys[0]] for row in mismatched_keys_df.collect()]
            
            if mismatched_keys:
                print(f"\n")
                print(f"🔍 Checking if mismatched keys ({len(mismatched_keys)} keys) exist in staging/storage...")
                data_in_staging = check_staging_and_storage_for_keys(
                    spark, staging_table, storage_path, 
                    source_table.split('.')[-1], 
                    primary_keys, mismatched_keys, mismatched_columns,
                    storage_type=storage_type
                )
    
    # 5. Summary and interpretation
    print("\n" + "=" * 80)
    print("📊 DIAGNOSIS SUMMARY")
    print("=" * 80)
    
    if actual_mismatch_found or target_extra_keys_count > 0:
        print(f"\n❌ CONFIRMED: Source and target are NOT the same")
        print(f"\n🔍 Found Issues:")
        if target_extra_keys_count > 0:
            print(f"    • Target has {target_extra_keys_count} row(s) that were DELETED in source (MERGE did not apply DELETEs)")
        if actual_mismatch_found and mismatched_columns:
            print(f"    • {len(mismatched_columns)} columns have mismatched values: {', '.join(mismatched_columns[:5])}")
            print(f"    • See 'DETAILED MISMATCH ANALYSIS' section above for specific rows and values")
        
        if is_append_only:
            print(f"\n💡 Root Cause (APPEND_ONLY Mode):")
            print(f"    • Mode: append_only - ALL source keys must exist in target")
            print(f"    • Issue: Source keys are missing from target = DATA LOSS")
            print(f"    • Changefeed did not capture these rows")
            print(f"\n🔧 Recommended Fix:")
            print(f"    • Check CockroachDB changefeed status (look for errors)")
            print(f"    • Re-run ingestion pipeline (Cell 12) - Auto Loader may not have processed all files yet")
            print(f"    • If problem persists:")
            print(f"      1. Complete reset: Drop changefeed, clear {storage_label}, clear target, recreate")
            print(f"      2. Check {storage_label} for presence of CDC files for missing keys")
            print(f"      3. Verify changefeed is still running in CockroachDB")
        else:
            print(f"\n💡 Root Cause (Column Family Issue):")
            print(f"    • These columns are in a separate column family from the primary key")
            print(f"    • Column family fragments are stored in separate Parquet files")
            
            # Determine if data is in staging or missing from Azure
            if mismatched_keys and staging_table:
                if data_in_staging:
                    print(f"    • ✅ Data EXISTS in staging table")
                    print(f"    • ❌ MERGE from staging → target failed to consolidate fragments")
                    print(f"\n🔧 Recommended Fix:")
                    print(f"    • Issue: MERGE logic didn't properly consolidate column family fragments")
                    print(f"    • Solution 1: Check MERGE deduplication logic in Cell 6")
                    print(f"    • Solution 2: Manually run MERGE again (re-run Cell 12)")
                    print(f"    • Solution 3: Drop target table and re-ingest (Cell 16 + Cell 12)")
                else:
                    print(f"    • ❌ Data MISSING from staging table")
                    print(f"    • ❌ Changefeed didn't capture the column family fragment")
                    print(f"\n🔧 Recommended Fix:")
                    print(f"    • Issue: Changefeed failed to capture field3-9 column family for some rows")
                    print(f"    • Solution 1: Complete reset - drop changefeed, clear {storage_label}, recreate")
                    print(f"    • Solution 2: Manual backfill for affected keys (see ROW_112_DIAGNOSIS.md)")
                    print(f"    • Solution 3: Check CockroachDB changefeed logs for errors")
            else:
                print(f"    • The MERGE operation may not have processed all fragments")
                print(f"\n🔧 Recommended Fix:")
                print(f"    • Re-run the ingestion pipeline (Cell 12)")
                print(f"    • Ensure wait_for_changefeed_files() detects all column family files")
                print(f"    • Check that the stabilization_wait period is sufficient")
    elif mismatched_columns:
        print(f"\n⚠️  {len(mismatched_columns)} columns reported as mismatched: {', '.join(mismatched_columns[:5])}")
        print(f"    However, row-by-row comparison shows they match now!")
        print(f"\n💡 Interpretation:")
        print(f"    • The data sync is CORRECT in the current state")
        print(f"    • The mismatch was detected earlier (before diagnosis)")
        print(f"    • Running the pipeline again resolved the issue")
    
    # Single clear verdict so it's obvious whether source and target match
    print("\n" + "=" * 80)
    if actual_mismatch_found or target_extra_keys_count > 0:
        reason = []
        if target_extra_keys_count > 0:
            reason.append(f"target has {target_extra_keys_count} extra row(s) (DELETEs not applied)")
        if actual_mismatch_found and mismatched_columns:
            reason.append("column value mismatch(es)")
        print(f"SOURCE AND TARGET: DIFFER — {'; '.join(reason)}")
    else:
        print(f"SOURCE AND TARGET: MATCH")
    print("=" * 80)
    
    print("\n" + "█" * 80)
    print("✅ DIAGNOSIS COMPLETE")
    print("█" * 80)


def quick_check_missing_keys(
    conn,
    spark,
    source_table: str,
    target_catalog: str,
    target_schema: str,
    target_table: str,
    primary_key_columns: List[str],
    missing_keys: List,
) -> None:
    """
    Lightweight check to see if specific keys exist in CockroachDB and staging.
    
    Args:
        conn: CockroachDB connection
        spark: Spark session
        source_table: Source table name (e.g., 'usertable_update_delete_multi_cf')
        target_catalog: Target catalog (e.g., 'main')
        target_schema: Target schema (e.g., 'robert_lee_crdb')
        target_table: Target table name
        primary_key_columns: Primary key column names from config (e.g., ['ycsb_key'] or ['region', 'id'])
        missing_keys: List of keys to check. Single PK: e.g. [17, 18, 19]. Multi-PK: e.g. [(1, 'a'), (2, 'b')].
    """
    target_table_fqn = f"{target_catalog}.{target_schema}.{target_table}"
    staging_table_cf = f"{target_table_fqn}_staging_cf"
    
    print("🔍 Quick Debug: Checking missing keys...")
    print("=" * 80)
    print(f"Keys to check: {missing_keys}")
    print()
    
    # Check CockroachDB
    where_clause = " AND ".join([f"{pk} = %s" for pk in primary_key_columns])
    query = f"SELECT * FROM {source_table} WHERE {where_clause}"
    print(f"📊 CockroachDB ({source_table}):")
    cursor = conn.cursor()
    for key in missing_keys:
        key_vals = (key,) if len(primary_key_columns) == 1 and not isinstance(key, (list, tuple)) else tuple(key)
        cursor.execute(query, key_vals)
        result = cursor.fetchone()
        print(f"   Key {key_vals}: {'✅ EXISTS' if result else '❌ NOT FOUND (deleted)'}")
    
    # Check Staging Table
    print(f"\n📊 Staging Table ({staging_table_cf}):")
    if spark.catalog.tableExists(staging_table_cf):
        staging_df = spark.read.table(staging_table_cf)
        for key in missing_keys:
            key_vals = (key,) if len(primary_key_columns) == 1 and not isinstance(key, (list, tuple)) else tuple(key)
            key_filter = None
            for pk_col, val in zip(primary_key_columns, key_vals):
                f = F.col(pk_col) == val
                key_filter = f if key_filter is None else (key_filter & f)
            count = staging_df.filter(key_filter).count()
            print(f"   Key {key_vals}: {count} row(s)")
        
        print("\n💡 Next steps:")
        print("   - If keys exist in CockroachDB but not in staging:")
        print("     → Re-run Cell 12 to pick up new CDC files")
        print("   - If keys exist in staging but not in target:")
        print("     → Check Cell 12 output for MERGE errors")
        print("     → Run inspect_target_table() for detailed analysis")
    else:
        print("   ⚠️  Staging table doesn't exist (Cell 12 dropped it)")
        print("   💡 Re-run Cell 12 to recreate staging for debugging")


def inspect_target_table(
    spark,
    target_catalog: str,
    target_schema: str,
    target_table: str,
    primary_key_columns: List[str],
) -> None:
    """
    Comprehensive analysis of target table:
    - CDC operation distribution
    - Key distribution and gaps
    - Duplicate detection
    - Sample records
    
    Use this to diagnose sync issues and data quality problems.
    
    Args:
        spark: Spark session
        target_catalog: Target catalog (e.g., 'main')
        target_schema: Target schema (e.g., 'robert_lee_crdb')
        target_table: Target table name
        primary_key_columns: Primary key column names from config (e.g., ['ycsb_key'] or ['region', 'id'])
    """
    target_table_fqn = f"{target_catalog}.{target_schema}.{target_table}"
    
    print(f"📊 Target Table Analysis: {target_table_fqn}")
    print("=" * 80)
    
    # Read target table
    df = spark.read.table(target_table_fqn)
    total_rows = df.count()
    print(f"\n📈 Total Rows: {total_rows:,}")
    
    # Group by CDC operation type
    if "_cdc_operation" in df.columns:
        print("\n🔍 CDC Operations:")
        df.groupBy("_cdc_operation").count().orderBy("_cdc_operation").show()
        
        # Check if DELETE rows are stored as data (should NOT happen)
        delete_count = df.filter(F.col("_cdc_operation") == "DELETE").count()
        if delete_count > 0:
            print(f"\n⚠️  WARNING: Found {delete_count} DELETE rows stored as data!")
            print("   This is a bug - DELETE rows should not be in the target table.")
            print("   💡 Run Cell 16 to fix (drops and recreates table)")
    
    # Show key distribution (use first PK column for single-column stats; all PKs for duplicate check)
    pk_first = primary_key_columns[0]
    if pk_first in df.columns:
        print("\n🔍 Key Distribution:")
        key_dist = df.groupBy(*primary_key_columns).count()
        if len(primary_key_columns) == 1:
            key_dist = key_dist.orderBy(pk_first)
        key_dist.show(50)
        
        # Check for duplicates
        duplicates = key_dist.filter("count > 1")
        dup_count = duplicates.count()
        if dup_count > 0:
            print(f"\n⚠️  Found {dup_count} duplicate keys!")
            duplicates.show()
            print("\n   💡 This indicates deduplication failure in MERGE logic")
        else:
            print("\n✅ No duplicate keys found")
        
        # Show key range and gaps (only for single numeric PK)
        if len(primary_key_columns) == 1:
            key_stats = df.select(
                F.min(pk_first).alias("min_key"),
                F.max(pk_first).alias("max_key"),
                F.count(pk_first).alias("count")
            ).collect()[0]
            
            expected_count = key_stats["max_key"] - key_stats["min_key"] + 1 if (key_stats["min_key"] is not None and key_stats["max_key"] is not None) else 0
            actual_count = key_stats["count"] or 0
            missing_count = expected_count - actual_count if expected_count > 0 else 0
            
            print(f"\n📊 Key Range Analysis:")
            print(f"   Min key: {key_stats['min_key']}")
            print(f"   Max key: {key_stats['max_key']}")
            print(f"   Expected rows (if contiguous): {expected_count}")
            print(f"   Actual rows: {actual_count}")
            
            if missing_count > 0:
                print(f"   ⚠️  Missing {missing_count} keys (gaps in range)")
                print(f"\n   💡 Run quick_check_missing_keys() or detailed_missing_keys_investigation() to investigate specific keys")
            else:
                print(f"   ✅ No gaps (keys are contiguous)")
    
    # Show sample records (order by primary key columns)
    print("\n🔍 Sample Records (ordered by key)::")
    df.orderBy(*primary_key_columns).show(30, truncate=False)
    
    print("\n" + "=" * 80)
    print("💡 If you see issues:")
    print("   - DELETE rows stored as data → Run Cell 16 (recreate table)")
    print("   - Duplicate keys → Check MERGE deduplication logic")
    print("   - Missing keys → Run quick_check_missing_keys() or detailed_missing_keys_investigation()")
    print("   - Gaps in key range → Keys were deleted (normal for update_delete mode)")


def run_full_diagnosis_from_config(
    spark,
    config,
    conn=None,
    mismatched_columns: List[str] = None
) -> None:
    """
    ALL-IN-ONE CDC Diagnosis: Stats + Verification + Deep Analysis
    
    This is a COMPREHENSIVE, SELF-CONTAINED function that:
    
    SECTION 1: CDC Event Summary
    - Shows total rows in target table
    - Shows operation breakdown (DELETE, UPSERT counts)
    - Shows sample rows with CDC metadata
    - Displays CDC mode information
    
    SECTION 2: Source vs Target Verification  
    - Connects to CockroachDB source (or uses provided connection)
    - Compares source and target statistics (min/max key, count, sum)
    - Auto-deduplicates target for append_only mode
    - Shows column-by-column sum comparison
    - Detects which columns mismatch
    
    SECTION 3: Detailed Diagnosis (Only if issues found)
    - Runs comprehensive diagnosis if any discrepancies detected
    - Analyzes column families and sync status
    - Checks CDC event distribution in storage
    - Performs row-by-row comparison
    - Provides troubleshooting recommendations
    
    Smart Behavior:
    - If everything matches → Shows "✅ Perfect sync!" and exits (no detailed diagnosis needed)
    - If mismatches detected → Automatically runs detailed diagnosis with targeted analysis
    
    Args:
        spark: Spark session
        config: Config dataclass from cockroachdb_config.py with:
            - cockroachdb: CockroachDBConfig (host, port, database, user, password)
            - tables: TableConfig (source_catalog, source_schema, source_table_name, etc.)
            - data_source: str ("azure" or "uc_external_volume")
            - azure_storage: AzureStorageConfig (if data_source is "azure")
            - uc_volume: UCVolumeConfig (if data_source is "uc_external_volume")
            - cdc_config: CDCConfig (primary_key_columns, mode, column_family_mode)
        conn: Optional pg8000.native.Connection - If provided, uses this connection.
              If None, creates a new connection (default: None)
        mismatched_columns: Optional - normally leave as None for auto-detection.
                           Only provide if you want to skip stats and go straight to 
                           detailed diagnosis for specific columns.
    
    Example (Recommended - One-stop diagnosis):
        from cockroachdb_debug import run_full_diagnosis_from_config
        
        # This ONE call gives you everything:
        # - CDC event summary
        # - Source vs target verification
        # - Detailed diagnosis (only if needed)
        run_full_diagnosis_from_config(spark, config)
    
    Example (With existing connection):
        from cockroachdb_conn import get_cockroachdb_connection_native
        
        conn = get_cockroachdb_connection_native(...)
        try:
            run_full_diagnosis_from_config(spark, config, conn=conn)
        finally:
            conn.close()  # Caller manages connection
    
    Example (Skip to detailed diagnosis):
        # If you want to skip stats and focus on specific columns:
        mismatched_columns = ['field3', 'field4']
        run_full_diagnosis_from_config(spark, config, mismatched_columns=mismatched_columns)
    """
    # Extract config - CockroachDB connection
    host = config.cockroachdb.host
    port = config.cockroachdb.port
    user = config.cockroachdb.user
    password = config.cockroachdb.password
    database = config.cockroachdb.database
    
    # Extract config - Source/Target tables
    source_catalog = config.tables.source_catalog
    source_schema = config.tables.source_schema
    source_table = config.tables.source_table_name
    
    target_catalog = config.tables.destination_catalog
    target_schema = config.tables.destination_schema
    target_table = config.tables.destination_table_name
    
    # Extract config - CDC settings
    primary_keys = config.cdc_config.primary_key_columns
    
    # Get storage path based on data source
    from cockroachdb_config import get_storage_path
    storage_cdc_path = get_storage_path(config)
    
    # Determine storage type
    storage_type = "uc_volume" if config.data_source == "uc_external_volume" else "azure"
    storage_label = "Unity Catalog Volume" if storage_type == "uc_volume" else "Azure Blob Storage"
    
    # Construct paths
    target_table_fqn = f"{target_catalog}.{target_schema}.{target_table}"
    staging_table = f"{target_catalog}.{target_schema}.{target_table}_staging_cf"
    
    # Print configuration
    print("\n" + "=" * 80)
    print("🔍 CDC SYNC DIAGNOSIS CONFIGURATION")
    print("=" * 80)
    print(f"   Source: {source_catalog}.{source_schema}.{source_table}")
    print(f"   Target: {target_table_fqn}")
    print(f"   Staging: {staging_table}")
    print(f"   Storage: {storage_label}")
    print(f"   Path: {storage_cdc_path[:80]}...")
    if mismatched_columns:
        print(f"   Mismatched columns: {len(mismatched_columns)} columns")
    print()
    
    # Refresh target DataFrame
    print("📊 Refreshing target DataFrame...")
    target_df = spark.read.table(target_table_fqn)
    if '__crdb__updated' not in target_df.columns:
        raise ValueError(
            "Target table is missing __crdb__updated. Diagnosis requires this column. "
            "Ensure the target was written by the connector pipeline that keeps __crdb__updated."
        )
    total_rows = target_df.count()
    print(f"✅ Target DataFrame refreshed: {total_rows:,} rows\n")
    
    # ========================================================================
    # SECTION 1: CDC EVENT SUMMARY
    # ========================================================================
    print("\n" + "=" * 80)
    print("📊 CDC EVENT SUMMARY")
    print("=" * 80)
    
    # Detect CDC mode
    cdc_mode_config = config.cdc_config.mode
    column_family_mode_config = config.cdc_config.column_family_mode
    
    print(f"Total rows: {total_rows:,}")
    print(f"CDC Processing Mode: {cdc_mode_config}")
    print(f"Column Family Mode: {column_family_mode_config}")
    print()
    
    # Show operation breakdown
    if "_cdc_operation" in target_df.columns:
        print("Rows by last CDC operation:")
        ops_df = target_df.groupBy("_cdc_operation").count().orderBy("_cdc_operation")
        ops_df.show()
        
        print("\n📋 Sample rows (showing first 5):")
        sample_cols = [c for c in list(primary_keys) + ["field0", "_cdc_operation", "__crdb__updated"] if c in target_df.columns]
        target_df.select(*sample_cols).orderBy(F.col("__crdb__updated").desc()).show(5, truncate=False)
    
    if cdc_mode_config == "append_only":
        print("\n📊 Mode: APPEND_ONLY")
        print("   • All CDC events stored as rows")
        print("   • _cdc_operation shows: DELETE, UPSERT for each event")
        print("   • Row count = all events (including DELETEs and multiple UPDATEs)")
    else:
        print("\n📊 Mode: UPDATE_DELETE")
        print("   • MERGE operations applied: DELETEs removed, UPDATEs applied, INSERTs added")
        print("   • _cdc_operation shows: UPSERT (last operation on each row)")
        print("   • Row count = current state (deduplicated)")
    
    # ========================================================================
    # SECTION 2: SOURCE VS TARGET VERIFICATION
    # ========================================================================
    print("\n" + "=" * 80)
    print("🔍 SOURCE vs TARGET VERIFICATION")
    print("=" * 80)
    
    # Establish connection (or use provided one)
    conn_provided = conn is not None
    
    if conn_provided:
        print("\n🔌 Using provided CockroachDB connection...")
        print("✅ Connection ready\n")
    else:
        print("\n🔌 Establishing CockroachDB connection...")
        
        from cockroachdb_conn import get_cockroachdb_connection_native
        
        conn = get_cockroachdb_connection_native(
            cockroachdb_host=host,
            cockroachdb_port=port,
            cockroachdb_user=user,
            cockroachdb_password=password,
            cockroachdb_database=database,
            test=False  # Skip test, already validated in main connection cell
        )
        print("✅ Connection established\n")
    
    try:
        # Import helper functions
        from cockroachdb_ycsb import (
            get_table_stats, get_column_sum, get_column_sum_spark, 
            deduplicate_to_latest
        )
        
        # Get source table stats
        source_table_fqn = f"{source_catalog}.{source_schema}.{source_table}"
        source_stats = get_table_stats(conn, source_table_fqn)
        pk_first = primary_keys[0]
        source_sum = get_column_sum(conn, source_table_fqn, pk_first)
        
        # Get raw target stats first (before any processing)
        raw_target_stats_row = target_df.agg(
            F.min(pk_first).alias("min_key"),
            F.max(pk_first).alias("max_key"),
            F.count("*").alias("count")
        ).collect()[0]
        raw_target_stats = {
            'min_key': raw_target_stats_row['min_key'],
            'max_key': raw_target_stats_row['max_key'],
            'count': raw_target_stats_row['count'],
        }
        raw_target_sum = get_column_sum_spark(target_df, pk_first)
        
        # Prepare target DataFrame (deduplicate if append_only)
        target_df_for_comparison = target_df
        if cdc_mode_config == "append_only":
            print("📝 Mode: APPEND_ONLY - Processing target for comparison...")
            print()
            print("   Step 1: Deduplicating to latest state per key...")
            target_df_deduplicated = deduplicate_to_latest(target_df, primary_keys, verbose=False)
            deduplicated_count = target_df_deduplicated.count()
            print(f"   ✅ Deduplicated: {total_rows} rows → {deduplicated_count} rows")
            
            # For append_only, filter target to only keys that exist in source
            # This allows fair comparison (source may have deleted rows)
            print(f"\n   Step 2: Filtering to source keys only for comparison...")
            source_keys_query = f"SELECT {', '.join(primary_keys)} FROM {source_table_fqn}"
            source_keys_result = conn.run(source_keys_query)
            conn.commit()  # Close read transaction
            # Join target to source keys (works for single or multi-column PK)
            from pyspark.sql import Row
            source_keys_df = spark.createDataFrame(
                [Row(**dict(zip(primary_keys, row))) for row in source_keys_result]
            )
            target_df_for_comparison = target_df_deduplicated.join(source_keys_df, primary_keys, "inner")
            filtered_count = target_df_for_comparison.count()
            print(f"   ✅ Filtered: {deduplicated_count} rows → {filtered_count} rows (matching source keys)")
            
            if deduplicated_count > filtered_count:
                extra_keys = deduplicated_count - filtered_count
                print(f"   ℹ️  Excluded {extra_keys} keys (deleted from source, retained in append_only log)")
            print()
        
        # Get target stats from processed DataFrame
        stats_row = target_df_for_comparison.agg(
            F.min(pk_first).alias("min_key"),
            F.max(pk_first).alias("max_key"),
            F.count("*").alias("count")
        ).collect()[0]
        target_stats = {
            'min_key': stats_row['min_key'],
            'max_key': stats_row['max_key'],
            'count': stats_row['count'],
            'is_empty': stats_row['min_key'] is None and stats_row['max_key'] is None
        }
        target_sum = get_column_sum_spark(target_df_for_comparison, pk_first)
        
        # Display comparison
        print(f"📊 Source Table (CockroachDB): {source_table_fqn}")
        print(f"   Min key: {source_stats['min_key']}")
        print(f"   Max key: {source_stats['max_key']}")
        print(f"   Count:   {source_stats['count']}")
        print(f"   Sum ({pk_first}): {source_sum}")
        
        if cdc_mode_config == "append_only":
            # Show both raw and processed target stats for append_only mode
            print(f"\n📊 Target Table (Databricks Delta) - RAW: {target_table_fqn}")
            print(f"   Min key: {raw_target_stats['min_key']}")
            print(f"   Max key: {raw_target_stats['max_key']}")
            print(f"   Count:   {raw_target_stats['count']}")
            print(f"   Sum ({pk_first}): {raw_target_sum}")
            print(f"   Note: This includes ALL CDC events (multiple versions of same key)")
            
            print(f"\n📊 Target Table (Databricks Delta) - FOR COMPARISON: {target_table_fqn}")
            print(f"   (Deduplicated to latest + filtered to source keys)")
            print(f"   Min key: {target_stats['min_key']}")
            print(f"   Max key: {target_stats['max_key']}")
            print(f"   Count:   {target_stats['count']}")
            print(f"   Sum ({pk_first}): {target_sum}")
            print(f"   Note: This should match source (apples-to-apples comparison)")
        else:
            print(f"\n📊 Target Table (Databricks Delta): {target_table_fqn}")
            print(f"   Min key: {target_stats['min_key']}")
            print(f"   Max key: {target_stats['max_key']}")
            print(f"   Count:   {target_stats['count']}")
            print(f"   Sum ({pk_first}): {target_sum}")
        
        # Compare all columns
        print("\n📊 Column Sums Comparison (All Fields):")
        print("-" * 80)
        if cdc_mode_config == "append_only":
            print("   Comparing Source vs Target (deduplicated + filtered to source keys)")
            print("   Note: Target may have extra deleted rows - only comparing matching keys")
        print()
        
        columns_to_verify = list(primary_keys) + [c for c in
            ['field0', 'field1', 'field2', 'field3', 'field4', 'field5', 'field6', 'field7', 'field8', 'field9']
            if c in target_df_for_comparison.columns]
        
        all_columns_match = True
        detected_mismatches = []
        
        for col in columns_to_verify:
            try:
                source_col_sum = get_column_sum(conn, source_table_fqn, col)
                target_col_sum = get_column_sum_spark(target_df_for_comparison, col)
                col_matches = source_col_sum == target_col_sum
                match_icon = "✅" if col_matches else "❌"
                
                # Format with commas for readability
                source_str = f"{source_col_sum:,}" if source_col_sum else "NULL"
                target_str = f"{target_col_sum:,}" if target_col_sum else "NULL"
                
                print(f"{match_icon} {col:12s}: Source={source_str:>20s} | Target={target_str:>20s}")
                
                if not col_matches:
                    all_columns_match = False
                    detected_mismatches.append(col)
                    diff = (target_col_sum or 0) - (source_col_sum or 0)
                    print(f"   ⚠️  Difference: {diff:+,}")
            except Exception as e:
                print(f"⚠️  {col:12s}: Error calculating sum - {e}")
                all_columns_match = False
        
        # Summary
        print("\n" + "=" * 80)
        if all_columns_match:
            print("✅ ALL COLUMNS MATCH! Data is in sync.")
        else:
            print(f"⚠️  MISMATCHES DETECTED: {len(detected_mismatches)} columns differ")
            print(f"   Columns: {', '.join(detected_mismatches)}")
        print("=" * 80)
        
        # Check if we need full diagnosis
        min_key_matches = source_stats['min_key'] == target_stats['min_key']
        max_key_matches = source_stats['max_key'] == target_stats['max_key']
        count_matches = source_stats['count'] == target_stats['count']
        
        # ========================================================================
        # SECTION 3: DECISION - RUN FULL DIAGNOSIS?
        # ========================================================================
        
        if all_columns_match and min_key_matches and max_key_matches and count_matches:
            print("\n✅ Perfect sync! No need for detailed diagnosis.")
            print("   All statistics and column sums match.")
            print(f"   Source rows: {source_stats['count']}, Target rows: {target_stats['count']}")
            
            if cdc_mode_config == "append_only":
                print("\n💡 Note: In append_only mode, comparison used:")
                print("   • Deduplicated target (latest state per key)")
                print("   • Filtered to source keys only (ignoring deleted rows in target)")
                print(f"   • Raw target has {total_rows} rows (full CDC event log)")
            
            return  # Exit early - no need for detailed diagnosis
        
        # For append_only mode, also check if only difference is extra deleted rows
        if cdc_mode_config == "append_only" and all_columns_match and max_key_matches:
            print("\n✅ Data is in sync! Minor differences are expected.")
            print(f"   • All column values match for keys that exist in both")
            print(f"   • Max key matches: {source_stats['max_key']}")
            
            if not min_key_matches or not count_matches:
                print(f"\n📋 Expected differences in append_only mode:")
                if not min_key_matches:
                    print(f"   • Min key: Source={source_stats['min_key']}, Target={target_stats['min_key']}")
                    print(f"     → Target may have older deleted rows")
                if not count_matches:
                    print(f"   • Row count: Source={source_stats['count']}, Target={target_stats['count']}")
                    print(f"     → Comparison only includes matching keys")
                    
                # Get full deduplicated count
                full_dedup_count = deduplicate_to_latest(target_df, primary_keys, verbose=False).count()
                if full_dedup_count > source_stats['count']:
                    extra_keys = full_dedup_count - source_stats['count']
                    print(f"     → Target has {extra_keys} extra keys (deleted from source, retained in append_only log)")
            
            print("\n💡 This is NORMAL for append_only mode:")
            print("   • Target retains all historical data including deleted rows")
            print("   • Source only shows current state")
            print("   • All active data matches perfectly!")
            
            return  # Exit early - no need for detailed diagnosis
        
        # If we get here, there are issues - proceed with full diagnosis
        print("\n" + "█" * 80)
        print("🔍 RUNNING DETAILED DIAGNOSIS")
        print("█" * 80)
        print("\n⚠️  Discrepancies detected - running comprehensive analysis...")
        print()
        
        # Use detected mismatches if not provided
        if mismatched_columns is None:
            mismatched_columns = detected_mismatches
        
        # Proceed with detailed diagnosis
        run_full_diagnosis(
            conn=conn,
            spark=spark,
            source_table=f"{source_catalog}.{source_schema}.{source_table}",
            target_df=target_df,
            staging_table=staging_table,
            storage_path=storage_cdc_path,
            primary_keys=primary_keys,
            mismatched_columns=mismatched_columns or [],
            storage_type=storage_type
        )
        
        # After detailed diagnosis, raise exception with validation failure details
        print("\n" + "=" * 80)
        print("❌ VALIDATION FAILED - Raising Exception")
        print("=" * 80)
        
        # Prepare detailed error information
        details = {
            'source_table': f"{source_catalog}.{source_schema}.{source_table}",
            'target_table': target_table_fqn,
            'cdc_mode': cdc_mode_config,
            'column_family_mode': column_family_mode_config,
            'min_key_matches': min_key_matches,
            'max_key_matches': max_key_matches,
            'count_matches': count_matches,
        }
        
        raise CDCValidationError(
            message=f"CDC validation failed: {len(detected_mismatches)} column(s) have mismatched data",
            mismatched_columns=detected_mismatches,
            source_count=source_stats['count'],
            target_count=target_stats['count'],
            details=details
        )
        
    finally:
        # Only close connection if we created it (not if it was provided)
        if not conn_provided:
            conn.close()
            print("\n🔌 Connection closed")
        else:
            print("\n🔌 Connection kept open (managed by caller)")


def detailed_missing_keys_investigation(
    conn,
    spark,
    source_table: str,
    target_catalog: str,
    target_schema: str,
    target_table: str,
    primary_key_columns: List[str],
    missing_keys: List,
) -> None:
    """
    Detailed investigation of missing keys:
    - Checks CockroachDB source
    - Checks staging table (if it exists)
    - Shows CDC operation and timestamps
    - Provides detailed troubleshooting steps
    
    Args:
        conn: CockroachDB connection
        spark: Spark session
        source_table: Source table name (e.g., 'usertable_update_delete_multi_cf')
        target_catalog: Target catalog (e.g., 'main')
        target_schema: Target schema (e.g., 'robert_lee_crdb')
        target_table: Target table name
        primary_key_columns: Primary key column names from config (e.g., ['ycsb_key'] or ['region', 'id'])
        missing_keys: List of keys to investigate. For single PK: e.g. [17, 18, 19]. For multi-PK: e.g. [(1, 'a'), (2, 'b')].
    """
    target_table_fqn = f"{target_catalog}.{target_schema}.{target_table}"
    staging_table_cf = f"{target_table_fqn}_staging_cf"
    
    print("🔍 Detailed Missing Keys Investigation")
    print("=" * 80)
    print(f"Investigating keys: {missing_keys}")
    print()
    
    # Step 1: Check CockroachDB Source
    print("📊 STEP 1: Checking CockroachDB Source")
    print("-" * 80)
    
    cursor = conn.cursor()
    where_clause = " AND ".join([f"{pk} = %s" for pk in primary_key_columns])
    query = f"SELECT * FROM {source_table} WHERE {where_clause}"
    
    for key in missing_keys:
        key_vals = (key,) if len(primary_key_columns) == 1 and not isinstance(key, (list, tuple)) else tuple(key)
        cursor.execute(query, key_vals)
        result = cursor.fetchone()
        
        if result:
            print(f"✅ Key {key_vals}: EXISTS in CockroachDB")
            # Show first 3 fields for verification
            print(f"   Sample data: {result[:min(3, len(result))]}")
        else:
            print(f"❌ Key {key_vals}: NOT FOUND in CockroachDB")
            print(f"   → This key was deleted (expected for update_delete mode)")
    
    # Step 2: Check Staging Table
    print(f"\n📊 STEP 2: Checking Staging Table")
    print("-" * 80)
    
    if spark.catalog.tableExists(staging_table_cf):
        staging_df = spark.read.table(staging_table_cf)
        print(f"✅ Staging table exists: {staging_table_cf}")
        print()
        
        for key in missing_keys:
            key_vals = (key,) if len(primary_key_columns) == 1 and not isinstance(key, (list, tuple)) else tuple(key)
            key_filter = None
            for pk_col, val in zip(primary_key_columns, key_vals):
                f = F.col(pk_col) == val
                key_filter = f if key_filter is None else (key_filter & f)
            key_rows = staging_df.filter(key_filter)
            count = key_rows.count()
            
            if count > 0:
                print(f"✅ Key {key_vals}: {count} row(s) in staging table")
                print("   Details:")
                detail_cols = [c for c in list(primary_key_columns) + ["__crdb__updated", "_cdc_operation", "field0", "field1"] if c in key_rows.columns]
                key_rows.select(*detail_cols).show(truncate=False)
            else:
                print(f"❌ Key {key_vals}: NOT in staging table")
        
        # Show staging table summary
        print("\n📈 Staging Table Summary:")
        staging_df.groupBy("_cdc_operation").count().show()
        
    else:
        print(f"⚠️  Staging table doesn't exist: {staging_table_cf}")
        print("   This means Cell 12 completed and dropped the staging table")
        print("\n💡 To debug further:")
        print("   1. Re-run Cell 12 (it will process new files and recreate staging)")
        print("   2. Run this function again to check staging table")
    
    # Step 3: Troubleshooting Recommendations
    print("\n" + "=" * 80)
    print("💡 TROUBLESHOOTING GUIDE")
    print("=" * 80)
    
    print("\n📋 If keys EXIST in CockroachDB but NOT in staging:")
    print("   → CDC files haven't been picked up by Auto Loader yet")
    print("   ✅ Solution: Re-run Cell 12 to process new CDC files")
    
    print("\n📋 If keys EXIST in staging but NOT in target:")
    print("   → MERGE logic failed or conditions are wrong")
    print("   ✅ Solution: Check Cell 12 output for MERGE errors")
    print("   ✅ Alternative: Check MERGE conditions in Cell 6")
    
    print("\n📋 If keys DON'T EXIST in CockroachDB:")
    print("   → Keys were deleted (normal for update_delete mode)")
    print("   ✅ Expected: Target should also not have these keys")
    print("   ⚠️  If target HAS these keys: MERGE delete logic isn't working")
    
    print("\n📋 If keys DON'T EXIST anywhere:")
    print("   → Keys were never created, or CDC didn't capture them")
    print("   ✅ Check: Run Cell 10 again to verify workload ran correctly")
