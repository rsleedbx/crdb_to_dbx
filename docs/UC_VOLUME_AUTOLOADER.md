# Unity Catalog Volume Auto Loader Configuration

*Updated: February 4, 2026*

## Overview

This document explains critical configuration and caching behaviors when using Auto Loader with Unity Catalog (UC) Volumes.

## Issue 1: Auto Loader Checkpoint Caching

### What is Auto Loader Checkpointing?

Auto Loader maintains a **checkpoint** that tracks:
- Which files have been processed
- Schema evolution history
- Offsets for incremental processing

**Location**: `/checkpoints/{schema}_{table_name}/`

### The "Files Not Being Processed" Problem

**Symptom**: After deleting and recreating target table, Auto Loader doesn't reprocess files.

**Root Cause**: Auto Loader checkpoint remembers files as "already processed" even though the target table was dropped.

**Example Scenario**:
```python
# Run 1: Process files successfully
ingest_cdc_append_only_single_family(config, spark)
# ✅ Processes 100 files → target table

# Delete target table
spark.sql(f"DROP TABLE {target_table}")

# Run 2: Try to reprocess same files
ingest_cdc_append_only_single_family(config, spark)
# ❌ Processes 0 files! Checkpoint says "already done"
```

### Solution: Clear Checkpoint When Resetting

**When to clear checkpoint**:
- Dropping and recreating target table
- Reprocessing same files from scratch
- Testing/development iterations
- Complete pipeline reset

**How to clear checkpoint**:

**Option 1: Using Databricks UI** (Recommended for notebooks):
1. Navigate to Workspace
2. Find checkpoint location: `/checkpoints/{schema}_{table_name}`
3. Right-click → Delete

**Option 2: Using Databricks CLI**:
```bash
databricks fs rm -r /checkpoints/{schema}_{table_name}
```

**Option 3: Using notebook magic command**:
```python
# In Databricks notebook
%fs rm -r /checkpoints/{schema}_{table_name}
```

**Complete reset pattern** (notebooks Cell 18 cleanup):
```python
# 1. Drop target table
spark.sql(f"DROP TABLE IF EXISTS {target_table_fqn}")

# 2. Clear checkpoint (using notebook magic)
checkpoint_path = f"/checkpoints/{config.tables.destination_schema}_{config.tables.destination_table_name}"
%fs rm -r {checkpoint_path}

# 3. Optionally clear staging table (for update_delete mode)
staging_table = f"{target_table_fqn}_staging_cf"
spark.sql(f"DROP TABLE IF EXISTS {staging_table}")
```

### Production Behavior (Why Checkpointing Exists)

In production streaming, checkpointing is **critical**:
- Prevents duplicate processing
- Enables exactly-once semantics
- Handles job restarts gracefully
- Tracks schema evolution

**Production workflow**:
```python
# Day 1: Initial load (100 files)
ingest_cdc_append_only_single_family(config, spark)
# Processes 100 files

# Day 2: New files arrive (10 more files)
ingest_cdc_append_only_single_family(config, spark)
# Processes only 10 NEW files (checkpoint remembers first 100)
```

This is **desired behavior** in production! Only clear checkpoints during development/testing.

---

## Issue 2: Auto Loader Notifications vs Directory Listing

### UC Volume File Discovery Modes

Auto Loader has two file discovery modes:

1. **Notifications** (default for cloud storage)
   - Uses cloud provider events (Azure Event Grid, AWS SQS, etc.)
   - Very fast for large directories
   - **NOT supported for UC Volumes**

2. **Directory Listing** (default for local paths)
   - Scans directory to find new files
   - Works for all storage types
   - Must be explicitly enabled for UC Volumes

### The Problem

UC Volumes use paths like `/Volumes/catalog/schema/volume/`, which Auto Loader treats as **local file system**, but it may try to use optimizations that don't work correctly.

### Solution: Explicit Configuration

**Before (missing configuration)**:
```python
df_raw = spark.readStream \
    .format("cloudFiles") \
    .option("cloudFiles.format", "parquet") \
    .option("cloudFiles.schemaLocation", f"{checkpoint_path}/schema") \
    .load(source_path)
```

**After (UC Volume optimized)**:
```python
# Detect UC Volume
is_uc_volume = source_path.startswith("/Volumes/")

reader = spark.readStream \
    .format("cloudFiles") \
    .option("cloudFiles.format", "parquet") \
    .option("cloudFiles.schemaLocation", f"{checkpoint_path}/schema")

# Force directory listing for UC Volumes
if is_uc_volume:
    reader = reader.option("cloudFiles.useNotifications", "false")

df_raw = reader.load(source_path)
```

### Fixed in Code

This fix has been applied to `cockroachdb_autoload.py` in the `_setup_autoloader()` function:
- Automatically detects UC Volume paths (`/Volumes/`)
- Sets `cloudFiles.useNotifications = false` for UC Volumes
- Uses directory listing for reliable file discovery

---

## Summary: Complete UC Volume Best Practices

### 1. Auto Loader Configuration
✅ **Automatic** - `_setup_autoloader()` auto-detects UC Volumes and disables notifications

### 2. File Listing
✅ **Automatic** - Uses Spark for parallel file discovery (fast and efficient)

### 3. Checkpoint Management
⚠️ **Manual** - You must clear checkpoints when resetting:

```python
# Development/testing reset (in Databricks notebook)
checkpoint_path = f"/checkpoints/{schema}_{table}"
%fs rm -r {checkpoint_path}

# Then re-run ingestion
ingest_cdc_...(config, spark)
```

### 4. Production Streaming
✅ **Keep checkpoints** - Never clear checkpoints in production. They enable:
- Exactly-once processing
- Incremental file discovery
- Job restart recovery

---

## Troubleshooting

### Problem: "No files processed" after table drop

**Cause**: Checkpoint still active  
**Fix**: Clear checkpoint before reprocessing
```python
# In Databricks notebook
%fs rm -r /checkpoints/{schema}_{table}
```

### Problem: "Files not discovered"

**Possible causes**:
1. Wrong volume path
2. Auto Loader notifications enabled (fixed in code)
3. Checkpoint contains old schema

**Fix**:
```python
# Verify path
from crdb_to_dbx.cockroachdb_config import get_storage_path
print(f"Volume path: {get_storage_path(config)}")

# Clear checkpoint (in Databricks notebook)
%fs rm -r /checkpoints/{schema}_{table}
```

---

## Code Changes Summary

### Files Modified

1. **`cockroachdb_autoload.py`** (`_setup_autoloader`)
   - Auto-detects UC Volume paths
   - Sets `cloudFiles.useNotifications = false` for UC Volumes
   - Enables directory listing mode

2. **`cockroachdb_uc_volume.py`** (`check_volume_files`)
   - Uses Spark for parallel file listing
   - Optimized for large file counts
   - Removed `dbutils` dependency

3. **`cockroachdb_storage.py`**
   - Simplified API (removed `dbutils` parameter)
   - Unified storage access layer

### Migration Guide

**No action required** - all changes are backward compatible:
- Auto Loader config is automatic
- Spark file listing is the only method (fast)
- Function signatures simplified

**For checkpoint management in development**:
- Use `%fs rm -r /checkpoints/{path}` in notebooks
- Or use Databricks UI/CLI to delete checkpoints
- Never clear checkpoints in production
