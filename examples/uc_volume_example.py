"""
Unity Catalog Volume Example

This example demonstrates how to use the Unity Catalog Volume utilities
to check for and wait for CockroachDB changefeed files.

Prerequisites:
    1. Unity Catalog volume created:
       CREATE VOLUME IF NOT EXISTS main.default.cdc_landing;
    
    2. CockroachDB changefeed writing to volume:
       CREATE CHANGEFEED FOR TABLE usertable
       INTO 'external://volume_sink'  -- Configured to write to volume
       WITH format='parquet', updated, resolved='10s';
    
    3. Databricks workspace environment with spark
"""

from crdb_to_dbx.cockroachdb_uc_volume import check_volume_files, wait_for_changefeed_files


def example_check_files(spark):
    """
    Example: Check for existing changefeed files in Unity Catalog Volume.
    """
    print("=" * 80)
    print("Example 1: Check for existing files")
    print("=" * 80)
    
    result = check_volume_files(
        volume_path="/Volumes/main/default/cdc_landing",
        source_catalog="defaultdb",
        source_schema="public",
        source_table="usertable",
        target_table="usertable_cdc",
        spark=spark,
        verbose=True
    )
    
    print(f"\nResults:")
    print(f"  Data files: {len(result['data_files'])}")
    print(f"  RESOLVED files: {len(result['resolved_files'])}")
    print(f"  Total files: {result['total']}")
    
    if result['data_files']:
        print(f"\nFirst data file:")
        print(f"  Name: {result['data_files'][0]['name']}")
        print(f"  Path: {result['data_files'][0]['path']}")
        print(f"  Size: {result['data_files'][0]['size']:,} bytes")
    
    if result['resolved_files']:
        print(f"\nLatest RESOLVED file:")
        print(f"  Name: {result['resolved_files'][-1]['name']}")
        print(f"  Path: {result['resolved_files'][-1]['path']}")


def example_wait_for_resolved(spark):
    """
    Example: Wait for RESOLVED file to appear (recommended for production).
    
    This ensures all CDC events and column family fragments are complete.
    """
    print("\n" + "=" * 80)
    print("Example 2: Wait for RESOLVED file (Production Pattern)")
    print("=" * 80)
    
    result = wait_for_changefeed_files(
        volume_path="/Volumes/main/default/cdc_landing",
        source_catalog="defaultdb",
        source_schema="public",
        source_table="usertable",
        target_table="usertable_cdc",
        spark=spark,
        max_wait=300,  # Wait up to 5 minutes
        check_interval=10,  # Check every 10 seconds
        wait_for_resolved=True  # ✅ Wait for RESOLVED (recommended!)
    )
    
    if result['success']:
        print(f"\n✅ SUCCESS!")
        print(f"  RESOLVED file: {result['resolved_file']}")
        print(f"  Data files: {result['file_count']}")
        print(f"  Elapsed time: {result['elapsed_time']}s")
        print(f"\n💡 All CDC events up to this RESOLVED timestamp are guaranteed complete")
    else:
        print(f"\n❌ TIMEOUT")
        print(f"  Elapsed time: {result['elapsed_time']}s")
        print(f"  Files found: {result['file_count']}")


def example_wait_for_data_files_legacy(spark):
    """
    Example: Wait for data files with stabilization (legacy mode).
    
    Not recommended for production - use RESOLVED mode instead.
    This is kept for backward compatibility.
    """
    print("\n" + "=" * 80)
    print("Example 3: Wait for data files (Legacy Mode)")
    print("=" * 80)
    
    result = wait_for_changefeed_files(
        volume_path="/Volumes/main/default/cdc_landing",
        source_catalog="defaultdb",
        source_schema="public",
        source_table="usertable",
        target_table="usertable_cdc",
        spark=spark,
        max_wait=120,
        stabilization_wait=10,  # Wait 10s for file count to stabilize
        wait_for_resolved=False  # Legacy mode
    )
    
    if result['success']:
        print(f"\n✅ Files appeared!")
        print(f"  File count: {result['file_count']}")
        print(f"  Elapsed time: {result['elapsed_time']}s")
    else:
        print(f"\n❌ Timeout after {result['elapsed_time']}s")


def example_multi_table_coordination(spark):
    """
    Example: Multi-table CDC with RESOLVED watermark coordination.
    
    This ensures all tables are synchronized to the same transactional point in time.
    """
    print("\n" + "=" * 80)
    print("Example 4: Multi-Table CDC Coordination")
    print("=" * 80)
    
    tables = [
        ("orders", "orders_cdc"),
        ("order_items", "order_items_cdc"),
        ("customers", "customers_cdc")
    ]
    
    resolved_files = []
    
    # Step 1: Wait for RESOLVED files from all tables
    print("\nStep 1: Waiting for RESOLVED files from all tables...")
    for source_table, target_table in tables:
        print(f"\n  Checking {source_table}...")
        result = wait_for_changefeed_files(
            volume_path="/Volumes/main/default/cdc_landing",
            source_catalog="defaultdb",
            source_schema="public",
            source_table=source_table,
            target_table=target_table,
            spark=spark,
            max_wait=300,
            wait_for_resolved=True
        )
        
        if result['success']:
            print(f"    ✅ RESOLVED: {result['resolved_file']}")
            resolved_files.append({
                'table': source_table,
                'resolved_file': result['resolved_file'],
                'file_count': result['file_count']
            })
        else:
            print(f"    ❌ Timeout for {source_table}")
            return
    
    # Step 2: Extract minimum RESOLVED timestamp (for transaction consistency)
    print(f"\n\nStep 2: Coordinating RESOLVED watermark across tables...")
    resolved_timestamps = [
        int(rf['resolved_file'].split('.')[0])  # Extract timestamp from filename
        for rf in resolved_files
    ]
    min_resolved = min(resolved_timestamps)
    
    print(f"  Resolved timestamps:")
    for rf in resolved_files:
        timestamp = int(rf['resolved_file'].split('.')[0])
        is_min = " ← MIN" if timestamp == min_resolved else ""
        print(f"    {rf['table']}: {timestamp}{is_min}")
    
    print(f"\n  💡 Using minimum RESOLVED watermark: {min_resolved}")
    print(f"     This ensures all tables are synchronized to the same transactional point")
    
    # Step 3: Process all tables with shared watermark
    print(f"\nStep 3: Processing tables with coordinated watermark...")
    print(f"  All tables will filter: __crdb__updated <= {min_resolved}")
    print(f"  This maintains referential integrity across tables!")


# ============================================================================
# Main execution (for notebook or script)
# ============================================================================

if __name__ == "__main__":
    print("Unity Catalog Volume Examples")
    print("=" * 80)
    print()
    print("Note: This script requires spark to be available.")
    print("Run in Databricks workspace notebook or with Databricks Connect.")
    print()
    
    # Check if spark is available
    try:
        # In Databricks notebooks, spark is automatically available
        spark  # noqa: F821
        
        # Run examples
        example_check_files(spark)
        example_wait_for_resolved(spark)
        # example_wait_for_data_files_legacy(spark)  # Optional
        # example_multi_table_coordination(spark)  # Optional
        
    except NameError:
        print("❌ Error: spark not available")
        print()
        print("To run this example:")
        print("  1. Copy to Databricks notebook, OR")
        print("  2. Use Databricks Connect:")
        print()
        print("     from databricks.connect import DatabricksSession")
        print()
        print("     spark = DatabricksSession.builder.getOrCreate()")
        print()
        print("     # Then run examples")
        print("     example_check_files(spark)")
