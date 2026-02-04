"""
CockroachDB CDC Test Loop

This module provides a function to run CDC end-to-end tests in a loop,
useful for continuous validation and stress testing.

The loop executes:
1. Generate workload (INSERT/UPDATE/DELETE)
2. Wait for CDC files in storage (Azure or UC Volume)
3. Ingest CDC data to Databricks
4. Run validation diagnosis

Storage Support:
- Azure Blob Storage
- Unity Catalog External Volumes
- Automatically switches based on config.data_source

Usage:
    from cockroachdb_testloop import run_cdc_test_loop
    
    # Run 10 iterations
    run_cdc_test_loop(spark=spark, config=config, iterations=10)
    
    # Run for 10 minutes
    run_cdc_test_loop(spark=spark, config=config, duration_minutes=10)
"""

import time
from typing import Optional
from datetime import datetime, timedelta


def run_cdc_test_loop(
    spark,
    config,
    iterations: Optional[int] = None,
    duration_minutes: Optional[int] = None,
    wait_for_files_timeout: int = 90,
    skip_validation: bool = False,
    conn=None
) -> dict:
    """
    Run CDC end-to-end test loop for continuous validation.
    
    This function executes a complete CDC cycle repeatedly:
    1. Generate workload (INSERT/UPDATE/DELETE operations)
    2. Wait for CDC files to appear in storage (Azure or UC Volume)
    3. Ingest CDC data to Databricks
    4. Run validation diagnosis (optional)
    
    The loop runs either for a specified number of iterations or duration.
    Automatically switches between Azure Blob Storage and Unity Catalog External
    Volume based on config.data_source.
    
    Args:
        spark: Spark session
        config: Config dataclass from cockroachdb_config.py
        iterations: Number of iterations to run (mutually exclusive with duration_minutes)
        duration_minutes: Duration in minutes to run (mutually exclusive with iterations)
        wait_for_files_timeout: Max seconds to wait for CDC files (default: 90)
        skip_validation: If True, skip validation diagnosis (default: False)
        conn: Optional pg8000.native.Connection - If provided, reuses this connection
              for validation. If None, creates new connections as needed (default: None)
    
    Returns:
        dict: Statistics about the test loop run
            - total_iterations: Number of iterations completed
            - total_duration_seconds: Total time elapsed
            - successful_iterations: Number of successful iterations
            - failed_iterations: Number of failed iterations
            - validation_failures: Number of validation failures
    
    Raises:
        ValueError: If both iterations and duration_minutes are None or both are specified
        Exception: If a critical error occurs during the loop
    
    Example:
        >>> # Run 10 iterations (creates connections as needed)
        >>> stats = run_cdc_test_loop(
        ...     spark=spark, config=config, iterations=10
        ... )
        >>> print(f"Completed {stats['successful_iterations']}/{stats['total_iterations']}")
        
        >>> # Run for 10 minutes
        >>> stats = run_cdc_test_loop(
        ...     spark=spark, config=config, duration_minutes=10
        ... )
        
        >>> # Run with reusable connection (more efficient)
        >>> from cockroachdb_conn import get_cockroachdb_connection_native
        >>> conn = get_cockroachdb_connection_native(...)
        >>> try:
        ...     stats = run_cdc_test_loop(
        ...         spark=spark, config=config, 
        ...         iterations=10, conn=conn
        ...     )
        ... finally:
        ...     conn.close()
        
        >>> # Run 5 iterations without validation (faster)
        >>> stats = run_cdc_test_loop(
        ...     spark=spark, config=config,
        ...     iterations=5, skip_validation=True
        ... )
    """
    # Validate parameters
    if iterations is None and duration_minutes is None:
        raise ValueError("Must specify either 'iterations' or 'duration_minutes'")
    if iterations is not None and duration_minutes is not None:
        raise ValueError("Cannot specify both 'iterations' and 'duration_minutes'")
    
    # Import dependencies
    from cockroachdb_conn import get_cockroachdb_connection, get_cockroachdb_connection_native
    from cockroachdb_storage import check_files  # Storage-agnostic (Azure + UC Volume)
    from cockroachdb_ycsb import run_ycsb_workload_with_random_nulls
    from cockroachdb_autoload import (
        ingest_cdc_append_only_single_family,
        ingest_cdc_append_only_multi_family,
        ingest_cdc_with_merge_single_family,
        ingest_cdc_with_merge_multi_family
    )
    from cockroachdb_debug import run_full_diagnosis_from_config, CDCValidationError
    
    # Initialize statistics
    stats = {
        'total_iterations': 0,
        'total_duration_seconds': 0,
        'successful_iterations': 0,
        'failed_iterations': 0,
        'validation_failures': 0,
        'start_time': datetime.now(),
        'end_time': None
    }
    
    start_time = datetime.now()
    end_time = start_time + timedelta(minutes=duration_minutes) if duration_minutes else None
    
    # ========================================================================
    # CONNECTION MANAGEMENT
    # ========================================================================
    # If no connection provided, create one for the entire loop
    conn_provided = conn is not None
    
    if not conn_provided:
        print("\n🔌 Creating CockroachDB connection for loop...")
        conn = get_cockroachdb_connection_native(
            cockroachdb_host=config.cockroachdb.host,
            cockroachdb_port=config.cockroachdb.port,
            cockroachdb_user=config.cockroachdb.user,
            cockroachdb_password=config.cockroachdb.password,
            cockroachdb_database=config.cockroachdb.database,
            test=True  # Test connection at start
        )
        print("✅ Connection ready for all iterations\n")
    
    # Print header
    storage_label = "Unity Catalog Volume" if config.data_source == "uc_external_volume" else "Azure"
    print("=" * 80)
    print("🔄 CDC TEST LOOP STARTING")
    print("=" * 80)
    if iterations:
        print(f"   Mode: {iterations} iterations")
    else:
        print(f"   Mode: {duration_minutes} minutes ({end_time.strftime('%H:%M:%S')} end time)")
    print(f"   Storage: {storage_label}")
    print(f"   CDC Mode: {config.cdc_config.mode}")
    print(f"   Column Family Mode: {config.cdc_config.column_family_mode}")
    print(f"   Validation: {'Disabled' if skip_validation else 'Enabled'}")
    if conn_provided:
        print(f"   Connection: Reused (provided by caller)")
    else:
        print(f"   Connection: Created once, reused across all iterations")
    print("=" * 80)
    print()
    
    iteration = 0
    
    try:
        while True:
            iteration += 1
            stats['total_iterations'] = iteration
            
            # Check stopping condition
            if iterations and iteration > iterations:
                break
            if duration_minutes and datetime.now() >= end_time:
                print(f"\n⏰ Time limit reached ({duration_minutes} minutes)")
                break
            
            print("\n" + "━" * 80)
            print(f"🔄 ITERATION {iteration}")
            if iterations:
                print(f"   Progress: {iteration}/{iterations}")
            else:
                elapsed = (datetime.now() - start_time).total_seconds() / 60
                remaining = duration_minutes - elapsed
                print(f"   Elapsed: {elapsed:.1f}min | Remaining: {remaining:.1f}min")
            print("━" * 80)
            
            try:
                # ================================================================
                # STEP 1: GENERATE WORKLOAD
                # ================================================================
                print(f"\n📝 Step 1: Generating workload...")
                
                # Capture baseline file count
                result_before = check_files(
                    config=config,
                    spark=spark,
                    verbose=False
                )
                files_before = len(result_before['data_files'])
                resolved_before = len(result_before['resolved_files'])
                print(f"   Baseline: {files_before} data files, {resolved_before} resolved files in {storage_label}")
                
                # Run workload using the loop's connection
                run_ycsb_workload_with_random_nulls(
                    conn=conn,
                    table_name=config.tables.source_table_name,
                    insert_count=config.workload_config.insert_count,
                    update_count=config.workload_config.update_count,
                    delete_count=config.workload_config.delete_count,
                    null_probability=0.5,
                    columns_to_randomize=['field0', 'field1', 'field2', 'field3', 
                                          'field4', 'field5', 'field6', 'field7', 
                                          'field8', 'field9'],
                    seed=None,  # Random seed for each iteration
                    force_all_null_update=True
                )
                
                print(f"   ✅ Workload complete: {config.workload_config.insert_count} INSERTs, "
                      f"{config.workload_config.update_count} UPDATEs, "
                      f"{config.workload_config.delete_count} DELETEs")
                
                # ================================================================
                # STEP 2: WAIT FOR CDC FILES
                # ================================================================
                print(f"\n⏳ Step 2: Waiting for CDC files in {storage_label} (timeout: {wait_for_files_timeout}s)...")
                
                check_interval = 10
                elapsed = 0
                files_appeared = False
                
                while elapsed < wait_for_files_timeout:
                    result = check_files(
                        config=config,
                        spark=spark,
                        verbose=False
                    )
                    files_now = len(result['data_files'])
                    resolved_now = len(result['resolved_files'])
                    
                    if files_now > files_before or resolved_now > resolved_before:
                        print(f"   ✅ CDC files appeared after {elapsed}s")
                        print(f"      Data files: {files_before} → {files_now} (+{files_now - files_before})")
                        print(f"      Resolved files: {resolved_before} → {resolved_now} (+{resolved_now - resolved_before})")
                        files_appeared = True
                        break
                    
                    time.sleep(check_interval)
                    elapsed += check_interval
                
                if not files_appeared:
                    print(f"   ⚠️  Timeout after {wait_for_files_timeout}s - proceeding anyway")
                
                # ================================================================
                # STEP 3: INGEST CDC DATA
                # ================================================================
                print(f"\n📥 Step 3: Ingesting CDC data from {storage_label}...")
                print(f"   Mode: {config.cdc_config.mode} + {config.cdc_config.column_family_mode}")
                
                # Select appropriate ingestion function
                # All functions now use config-based approach (no individual storage params)
                if config.cdc_config.mode == "append_only" and config.cdc_config.column_family_mode == "single_cf":
                    query = ingest_cdc_append_only_single_family(
                        config=config,
                        spark=spark
                    )
                    query.awaitTermination()
                    
                elif config.cdc_config.mode == "append_only" and config.cdc_config.column_family_mode == "multi_cf":
                    query = ingest_cdc_append_only_multi_family(
                        config=config,
                        spark=spark
                    )
                    query.awaitTermination()
                    
                elif config.cdc_config.mode == "update_delete" and config.cdc_config.column_family_mode == "single_cf":
                    result = ingest_cdc_with_merge_single_family(
                        config=config,
                        spark=spark
                    )
                    
                elif config.cdc_config.mode == "update_delete" and config.cdc_config.column_family_mode == "multi_cf":
                    result = ingest_cdc_with_merge_multi_family(
                        config=config,
                        spark=spark
                    )
                else:
                    raise ValueError(
                        f"Invalid mode combination: "
                        f"cdc_mode='{config.cdc_config.mode}', "
                        f"column_family_mode='{config.cdc_config.column_family_mode}'"
                    )
                
                print(f"   ✅ CDC ingestion complete")
                
                # ================================================================
                # STEP 4: RUN VALIDATION
                # ================================================================
                if not skip_validation:
                    print(f"\n🔍 Step 4: Running validation diagnosis...")
                    
                    try:
                        # Pass connection if provided (reuse across iterations)
                        run_full_diagnosis_from_config(
                            spark=spark, 
                            config=config, 
                            conn=conn  # Reuses connection if provided
                        )
                        print(f"   ✅ Validation passed")
                    except CDCValidationError as e:
                        stats['validation_failures'] += 1
                        print(f"   ❌ Validation failed: {e.message}")
                        print(f"      Mismatched columns: {e.mismatched_columns}")
                        # Don't raise - continue with next iteration
                else:
                    print(f"\n⏭️  Step 4: Validation skipped (skip_validation=True)")
                
                # Mark iteration as successful
                stats['successful_iterations'] += 1
                print(f"\n✅ Iteration {iteration} completed successfully")
                
            except Exception as e:
                stats['failed_iterations'] += 1
                print(f"\n❌ Iteration {iteration} failed: {e}")
                print(f"   Continuing to next iteration...")
                # Continue to next iteration instead of stopping
        
        # Loop completed successfully
        stats['end_time'] = datetime.now()
        stats['total_duration_seconds'] = (stats['end_time'] - stats['start_time']).total_seconds()
        
        # Print summary
        print("\n" + "=" * 80)
        print("🏁 CDC TEST LOOP COMPLETE")
        print("=" * 80)
        print(f"   Total iterations: {stats['total_iterations']}")
        print(f"   Successful: {stats['successful_iterations']}")
        print(f"   Failed: {stats['failed_iterations']}")
        if not skip_validation:
            print(f"   Validation failures: {stats['validation_failures']}")
        print(f"   Duration: {stats['total_duration_seconds']:.1f}s ({stats['total_duration_seconds']/60:.1f}min)")
        print(f"   Start: {stats['start_time'].strftime('%H:%M:%S')}")
        print(f"   End: {stats['end_time'].strftime('%H:%M:%S')}")
        print("=" * 80)
        
        return stats
        
    except KeyboardInterrupt:
        print("\n\n⚠️  Loop interrupted by user (Ctrl+C)")
        stats['end_time'] = datetime.now()
        stats['total_duration_seconds'] = (stats['end_time'] - stats['start_time']).total_seconds()
        print(f"   Completed {stats['successful_iterations']}/{stats['total_iterations']} iterations")
        return stats
    except Exception as e:
        print(f"\n\n❌ Fatal error in test loop: {e}")
        stats['end_time'] = datetime.now()
        stats['total_duration_seconds'] = (stats['end_time'] - stats['start_time']).total_seconds()
        raise
    finally:
        # Close connection if we created it
        if not conn_provided:
            conn.close()
            print("\n🔌 Connection closed")
