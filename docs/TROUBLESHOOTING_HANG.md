# Troubleshooting: UC Volume File Listing Hang

*Updated: February 4, 2026*

## Issue

When using UC External Volume mode, the notebook may hang at:

```
🔍 Scanning for .RESOLVED files in Unity Catalog Volume...
```

This typically takes **2-10 seconds** but may appear to hang if:
1. There are many files (1000+)
2. Spark cluster is cold (first run)
3. Network latency to Unity Catalog

## Where the Code Can Hang

### Spark File Listing

**Location:** `cockroachdb_uc_volume.py` - Spark-based parallel listing

```python
# Uses Spark for parallel file listing (2-5 seconds typical)
file_df = spark.read.format("binaryFile") \
    .option("recursiveFileLookup", "true") \
    .load(full_path)  # ← May hang here

file_paths = file_df.select("path").collect()  # ← Or here
```

**Can hang when:**
- Spark cluster is cold/starting up (first run: +30-60s)
- Unity Catalog connection issues
- Large number of files (1000+)
- Network latency to Unity Catalog

## Identifying Where It's Stuck

### Enhanced Logging

The code shows **detailed progress** during file listing:

```
🔍 Scanning for .RESOLVED files in Unity Catalog Volume...
   📂 Volume path: /Volumes/catalog/schema/volume
   ⏳ Listing files (this may take 5-10 seconds)...
   
   🚀 Using Spark for fast parallel file listing...
   ⏳ Spark: Reading directory structure...
   ⏳ Spark: Collecting file paths...
   ✅ Spark: Found 100 total paths
   
   ✅ File listing completed in 3.5s
```

**Key indicators:**
- If you see "🚀 Using Spark" → It's in **our code** (Spark listing)
- If you see "⏳ Spark: Reading directory structure..." for >30s → **Databricks Spark is slow**
- If you see "⏳ Spark: Collecting file paths..." for >30s → **Databricks is processing files**

## Solutions

### Option 1: Wait It Out (Recommended First Time)

**First run takes longer** due to:
- Cold Spark cluster startup (~30-60s)
- Unity Catalog connection initialization
- Metadata caching

**Subsequent runs should be faster** (2-5s).

### Option 2: Disable RESOLVED Watermarking (Quick Fix)

If RESOLVED scanning is too slow, disable it temporarily:

```python
# In your config file
{
  "cdc_config": {
    "use_resolved_watermark": false  // ← Add this
  }
}
```

**Trade-off:**
- ✅ Faster startup (no file scanning)
- ⚠️  **No column family completeness guarantee**
- ⚠️  May process incomplete fragments (multi_cf mode)

**Use when:**
- Testing/debugging
- Single column family tables (doesn't matter)
- Willing to risk incomplete data

### Option 3: Pre-warm Spark Cluster

Run a simple Spark query first to warm up the cluster:

```python
# Warm up Spark before CDC ingestion
spark.sql("SELECT 1").collect()
print("✅ Spark cluster warmed up")

# Now run CDC ingestion
query = ingest_cdc_with_merge_multi_family(config, spark)
```

### Option 4: Increase Timeout (If Actually Working)

If it's working but just slow, be patient:

```python
# Expected times:
# - First run: 30-60s (cold start)
# - Subsequent: 2-10s (warm)
# - 100 files: ~3-5s
# - 1000 files: ~10-20s
```

## Debugging Steps

### Step 1: Check Current Status

Look at the console output to see where it stopped:

```
# Stopped here? → Spark is reading directory
⏳ Spark: Reading directory structure...

# Stopped here? → Spark is collecting paths
⏳ Spark: Collecting file paths...
```

### Step 2: Check Spark UI

1. Go to Databricks cluster
2. Click "Spark UI"
3. Check "Jobs" tab
4. Look for active job reading `binaryFile`

**If job is running:** Databricks Spark is working (be patient)
**If no job shown:** May be stuck before Spark starts

### Step 3: Try Manual Listing

Test file listing manually:

```python
# Test Spark listing
from pyspark.sql.utils import AnalysisException

volume_path = "/Volumes/catalog/schema/volume/parquet/defaultdb/public/table/table/"

try:
    df = spark.read.format("binaryFile") \
        .option("recursiveFileLookup", "true") \
        .load(volume_path)
    count = df.count()
    print(f"✅ Spark listing works: {count} files")
except Exception as e:
    print(f"❌ Spark listing failed: {e}")
```

### Step 4: Check Volume Access

Verify volume is accessible:

```python
# Test volume access with Spark
try:
    test_path = "/Volumes/catalog/schema/volume/"
    df = spark.read.format("binaryFile").load(test_path)
    count = df.count()
    print(f"✅ Volume accessible: {count} files")
except Exception as e:
    print(f"❌ Volume not accessible: {e}")
```

## Root Cause Analysis

### Databricks/Unity Catalog (External)
- Spark cluster startup (30-60s cold start)
- Unity Catalog API latency
- Network issues
- Volume metadata loading

### Our Code (Internal)
- Spark file listing implementation
- File filtering logic
- DataFrame collection operations

**The logging shows where the time is spent!**

## Performance Comparison

| Method | 10 Files | 100 Files | 1000 Files |
|--------|----------|-----------|------------|
| **Azure SDK** | ~0.5s | ~1s | ~3s |
| **UC Volume (Spark)** | ~2s | ~5s | ~15s |
| **First Run (Cold)** | +30-60s | +30-60s | +30-60s |

**Note:** Cold start penalty applies to first Spark operation only, not subsequent calls.

## Quick Decision Tree

```
Is it taking >60s?
├─ Yes → Likely stuck
│  ├─ Check console for last message
│  ├─ Check Spark UI for active jobs
│  └─ Consider Option 2 (disable RESOLVED)
│
└─ No (10-30s) → Probably normal
   ├─ First run? → Add 30-60s for cold start
   ├─ Many files? → Add more time
   └─ Just wait...
```

## Summary

**90% of "hangs" are actually:**
1. Cold Spark cluster startup (first run)
2. Normal file listing (just slow)
3. Network latency

**Actual hangs are rare** but can happen due to:
1. Unity Catalog connection issues
2. Spark cluster problems  
3. Very large file counts (10,000+)

**With detailed logging, you can now see:**
- ✅ Current operation (reading directory vs collecting paths)
- ✅ Progress indicators for each step
- ✅ Whether it's in our code or Databricks/Unity Catalog
- ✅ Total time elapsed

**Try this first:** Just wait 60 seconds. Most "hangs" resolve themselves, especially on first run!
