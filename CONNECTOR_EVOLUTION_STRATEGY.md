# CockroachDB CDC Connector: Evolution Strategy

## 🎯 Mission & Goals

### Primary Goal

**Enable seamless CockroachDB CDC data consumption in Databricks using multiple patterns. Primary approach uses JSON and Parquet files written by changefeeds; instream changefeed connections also supported for testing.**

### Specific Objectives

1. **Format Support** ✅ COMPLETE
   - ✅ Support CockroachDB **JSON** changefeed format (wrapped envelope with `before`/`after`)
   - ✅ Support CockroachDB **Parquet** changefeed format (columnar with `__crdb__event_type`)
   - ✅ Auto-detect format from path structure
   - ✅ Handle both formats with shared CDC transformation logic

2. **Consumption Patterns** 🟡 IN PROGRESS
   - ✅ **Community Connector** (Iterator Pattern) - Supports both JSON/Parquet files from changefeeds AND instream changefeed for testing, prototyping, low-volume workloads
   - ✅ **Standalone Autoloader** - For validation, ad-hoc analysis, migrations (file-based)
   - 🟡 **DLT + Autoloader** - For production streaming pipelines (building blocks ready, file-based)

3. **Data Quality & Operations** ✅ COMPLETE
   - ✅ Accurate CDC operation classification (SNAPSHOT, INSERT, UPDATE, DELETE)
   - ✅ Column family fragment merging (`split_column_families` support)
   - ✅ **RESOLVED timestamp watermarking** (guarantees column family completeness)
   - ✅ Primary key extraction and deduplication
   - ✅ Timestamp-based snapshot cutoff detection
   - ✅ DELETE operation filtering
   - ✅ Multi-table transaction boundary coordination

### Architecture Vision

```
┌─────────────────────────────────────────────────────────┐
│           CockroachDB Changefeeds                        │
│          (Write to Azure Blob Storage)                   │
│                                                          │
│     JSON Format          │         Parquet Format       │
│   (wrapped envelope)     │      (columnar, native)      │
│   📄 .ndjson files       │      📄 .parquet files       │
└──────────────┬───────────┴──────────────┬───────────────┘
               │                          │
               │                          │ (Instream also
               └──────────┬───────────────┘  available for
                          │                  testing)
                ┌─────────▼─────────┐
                │  Unity Catalog    │
                │     Volumes       │
                │  (File Storage)   │  ← JSON/Parquet files land here
                └─────────┬─────────┘
                          │
              ┌───────────┴───────────────┐
              │   Shared CDC Engine       │
              │ (File-based + Instream)   │
              │                           │
              │ • Format detection        │
              │ • Event transformation    │
              │ • PK extraction           │
              │ • Coalescing              │
              │ • Timestamp analysis      │
              └───────────┬───────────────┘
                          │
         ┌────────────────┼────────────────┐
         │                │                │
    ┌────▼─────┐    ┌────▼─────┐    ┌────▼─────┐
    │Community │    │Standalone│    │   DLT    │
    │Connector │    │Autoloader│    │Autoloader│
    │          │    │          │    │          │
    │Iterator  │    │CloudFiles│    │CloudFiles│
    │Files OR  │    │Batch     │    │Streaming │
    │Instream  │    │(Files)   │    │(Files)   │
    └──────────┘    └──────────┘    └──────────┘
     ↑                ↑                ↑
     Primary: File-based consumption
     Community Connector also supports instream for testing
```

### Success Criteria

| Criterion | Target | Current Status |
|-----------|--------|----------------|
| JSON format support | 100% feature parity with Parquet | ✅ **ACHIEVED** |
| Parquet format support | Full CDC operations | ✅ **ACHIEVED** |
| Community Connector (Iterator) | Working with volumes | ✅ **ACHIEVED** |
| Standalone Autoloader | Complete mode to Delta | ✅ **ACHIEVED** |
| DLT + Autoloader | Streaming pipeline | 🟡 **75% (building blocks ready)** |
| Test coverage | All format/table/split scenarios | ✅ **8/8 passing** |
| Code reuse | >50% shared logic | ✅ **55% achieved** |
| Performance | <30 sec validation | ✅ **60× faster** |

### Current Milestone: Step 5 COMPLETE! 🎉🎉🎉

We have successfully completed **Step 5: Community Connector** (100% complete)! Iterator pattern now supports both JSON and Parquet files from Unity Catalog Volumes with automatic format detection and shared CDC processing logic. 4 out of 5 steps complete - ready to proceed to Step 4: DLT + Autoloader!

---

## 📊 Executive Summary (Updated Jan 30, 2026)

### Current Status: ✅ STEP 5 COMPLETE - PROOF OF CONCEPT WITH COMPREHENSIVE DIAGNOSTICS!

**Mission:** Build proof-of-concept CDC connector with 5-step implementation roadmap

**Latest Milestone (Jan 30, 2026):** Complete End-to-End Tutorial + Smart Diagnostics
- ✅ Interactive tutorial notebook with 4 CDC modes
- ✅ All-in-one diagnosis: stats + verification + deep analysis
- ✅ Smart append_only mode handling (deduplication + filtering)
- ✅ Self-contained - no external dependencies
- ✅ Reusable helper modules for connection, workload, and diagnosis

**Progress:** 
- ✅ **Step 1: CDC Generation** - 100% Complete (8/8 scenarios validated)
- ✅ **Step 2: One-Time Load** - 100% Complete (Parquet ✅, JSON ✅)
- ✅ **Step 3: Incremental Load** - 100% Complete (Autoloader checkpoints working!)
- ✅ **Step 5: Community Connector** - 100% Complete (JSON ✅, Parquet ✅, Deduplication ✅)
- ⏸️ **Step 4: DLT + Autoloader** - Not started (ready to begin)

**🎉 MILESTONE ACHIEVED (Jan 21, 2026):** Format-Agnostic Iterator Pattern Complete!
- ✅ JSON file support with column family fragmentation handling
- ✅ Parquet file support (no fragmentation needed - format optimized!)
- ✅ Auto-detection and format-specific processing
- ✅ Unified deduplication logic (matches Autoloader exactly)
- ✅ Recursive directory reading (handles date-based partitions)
- ✅ Perfect row count matching: 9,950 rows (Iterator = Autoloader)
- ✅ Shared CDC processing logic (55% code reuse)

**Visual Progress:**
```
[████████████████████] Step 1: CDC Generation      ✅ 100%
[████████████████████] Step 2: One-Time Load       ✅ 100%
[████████████████████] Step 3: Incremental Load    ✅ 100%
[░░░░░░░░░░░░░░░░░░░░] Step 4: DLT + Autoloader    ⏸️   0%
[████████████████████] Step 5: Community Connector ✅ 100%

Overall Progress: 80% (4.0 / 5 steps)
```

### 🆕 New: Comprehensive Diagnostics & Tutorial (Jan 30, 2026)

**All-in-One CDC Diagnosis Function:**
```python
from cockroachdb_debug import run_full_diagnosis_from_config
run_full_diagnosis_from_config(spark, config)
```

**What it does:**
1. **CDC Event Summary** - Total rows, operation breakdown, sample data
2. **Source vs Target Verification** - Column-by-column comparison with smart deduplication
3. **Detailed Diagnosis** - Only runs if issues detected (automatic)

**Key Features:**
- ✅ Smart append_only mode handling (deduplicates + filters to source keys)
- ✅ Shows both RAW target stats AND comparison stats
- ✅ Auto-detects mismatches (no manual column lists needed)
- ✅ Exits early if data syncs perfectly (no unnecessary analysis)
- ✅ Clear legends and explanations for all symbols
- ✅ Source→Target comparison direction (verifies all source rows exist)
- ✅ Timestamp-aware row selection (latest per key)

**Tutorial Notebook:**
- Complete end-to-end CDC tutorial: `docs/cockroachdb-cdc-tutorial.ipynb`
- 4 CDC modes: append_only + update_delete × single_cf + multi_cf
- Self-contained with helper modules (ycsb, debug, conn, azure, autoload)
- YCSB workload simulation with NULL value support
- Automated diagnosis and troubleshooting

---

## 🚀 Getting Started

This section guides you through the complete setup workflow for testing the CockroachDB CDC connector.

### 📖 Quick Links to Key Resources

**Want to build a CDC pipeline? Start here:**
- **[docs/stream-changefeed-to-databricks-azure.md](docs/stream-changefeed-to-databricks-azure.md)** - Quick start guide
  - 6-step basic CDC pipeline setup (includes Azure access configuration)
  - Simple examples for getting started
- **[docs/cockroachdb-cdc-tutorial.ipynb](docs/cockroachdb-cdc-tutorial.ipynb)** - Complete interactive tutorial
  - End-to-end CDC setup (CockroachDB → Azure → Databricks)
  - 4 CDC modes with automatic function selection
  - Built-in diagnosis and troubleshooting
  - No external dependencies - fully self-contained

**Supporting modules for the tutorial:**
- `cockroachdb_ycsb.py` - YCSB workload generation & statistics
- `cockroachdb_debug.py` - All-in-one CDC diagnosis (stats + verification + analysis)
- `cockroachdb_conn.py` - CockroachDB connection utilities
- `cockroachdb_azure.py` - Azure changefeed management
- `cockroachdb_autoload.py` - 4 CDC ingestion modes

**Want to understand the architecture? Read:**
- This document (CONNECTOR_EVOLUTION_STRATEGY.md) - Complete strategy, testing, and evolution plan

---

### Prerequisites

**Required Tools:**
```bash
# Check if you have all required tools
command -v az >/dev/null 2>&1 || echo "❌ Azure CLI missing: brew install azure-cli"
command -v psql >/dev/null 2>&1 || echo "❌ PostgreSQL client missing: brew install postgresql"
command -v python3 >/dev/null 2>&1 || echo "❌ Python 3 missing"
command -v jq >/dev/null 2>&1 || echo "❌ jq missing: brew install jq"
command -v yq >/dev/null 2>&1 || echo "❌ yq missing: brew install yq"
command -v databricks >/dev/null 2>&1 || echo "❌ Databricks CLI missing: pip install databricks-cli"
```

**Required Access:**
- Azure subscription with permissions to create:
  - Resource groups
  - Storage accounts and containers
  - Managed identities (optional, for Unity Catalog)
- CockroachDB cluster (Cloud or self-hosted)
- Databricks workspace with Unity Catalog enabled

### Step 1: Environment Setup

**1.1 Source Environment Configuration**

The `00_lakeflow_connect_env.sh` script provides common bash functions and environment setup used by all other scripts.

```bash
cd sources/cockroachdb/scripts

# Source the environment (must be sourced, not executed)
source ./00_lakeflow_connect_env.sh
```

**What it provides:**
- Cloud CLI wrappers (`AZ`, `DBX`, `AWS`, `GCLOUD`)
- Database connection helpers (`PSQL`, `SQLCLI`, `MYSQLCLI`)
- Secret management functions
- JSON/YAML parsing utilities
- Standardized error handling

**Key Environment Variables:**
```bash
# Databricks configuration
export DBX_PROFILE="DEFAULT"              # Your Databricks CLI profile
export DBX_USERNAME="user@example.com"    # Auto-detected from databricks auth

# Azure configuration  
export CLOUD_LOCATION="East US"           # Your Azure region
export RG_NAME="cockroachdb-cdc-rg"       # Resource group name

# Cleanup automation (optional)
export REMOVE_AFTER="2026-02-01"          # Auto-cleanup date (YYYY-MM-DD)
```

### Step 2: Azure Storage Setup

**2.1 Initialize Azure and Create Storage Resources**

The `01_azure_storage.sh` script creates all required Azure infrastructure:

```bash
# Make sure you're in the scripts directory
cd sources/cockroachdb/scripts

# Source environment first (required)
source ./00_lakeflow_connect_env.sh

# Run Azure setup
./01_azure_storage.sh
```

**What it creates:**
- ✅ Azure Resource Group
- ✅ Storage Account (with hierarchical namespace)
- ✅ Blob Container: `changefeed-events`
- ✅ Managed Identity (user-assigned)
- ✅ Access Connector for Databricks
- ✅ Unity Catalog Storage Credential
- ✅ Unity Catalog External Locations (Parquet and JSON)

**Output Files:**
The script creates a configuration file with all credentials:
```
sources/cockroachdb/.env/cockroachdb_cdc_azure.json
```

**Configuration Format:**
```json
{
  "timestamp": "1737500000",
  "resource_group": "cockroachdb-cdc-rg",
  "azure_storage_account": "cockroachcdc1737500000",
  "azure_storage_container": "changefeed-events",
  "azure_storage_key": "xxx...",
  "changefeed_uri": "azure-blob://changefeed-events?AZURE_ACCOUNT_NAME=...",
  "abfss_base_url": "abfss://changefeed-events@xxx.dfs.core.windows.net",
  "unity_catalog": {
    "storage_credential_name": "cockroachdb_cdc_storage_credential_xxx",
    "parquet_location_name": "cockroachdb_cdc_parquet_xxx",
    "json_location_name": "cockroachdb_cdc_json_xxx"
  }
}
```

**Troubleshooting:**
- If you get permission errors on RBAC role assignments, the script will continue
- You can configure file events manually later via Azure Portal
- See script comments for minimal vs full setup options

### Step 3: CockroachDB Configuration

**3.1 Create CockroachDB Credentials File**

Create a JSON file with your CockroachDB connection details:

```bash
# Create credentials file
mkdir -p sources/cockroachdb/.env
cat > sources/cockroachdb/.env/cockroachdb_credentials.json <<EOF
{
  "cockroachdb_url": "postgresql://user:password@host:26257/defaultdb?sslmode=require"
}
EOF
```

**For CockroachDB Cloud:**
1. Go to your cluster's "Connect" page
2. Copy the connection string
3. Replace `<password>` with your actual password

**For Self-Hosted:**
```json
{
  "cockroachdb_url": "postgresql://root@localhost:26257/defaultdb?sslmode=disable"
}
```

**3.2 Verify Connection**

```bash
# Test connection
psql "postgresql://user:password@host:26257/defaultdb?sslmode=require" -c "SELECT version();"
```

### Step 4: Databricks Unity Catalog Setup

**4.1 Create Unity Catalog Volume**

The test scripts expect a Unity Catalog Volume to store CDC data:

```bash
# Create pipeline configuration
cat > sources/cockroachdb/.env/cockroachdb_pipelines.json <<EOF
{
  "catalog": "your_catalog",
  "schema": "your_schema",
  "volume_name": "cockroachdb_cdc_data"
}
EOF
```

**4.2 Create Volume in Databricks**

Option A - Use the provided script:
```bash
cd sources/cockroachdb/scripts
./create_volume_pipeline.sh
```

Option B - Manually via SQL:
```sql
-- In Databricks SQL or notebook
CREATE VOLUME IF NOT EXISTS your_catalog.your_schema.cockroachdb_cdc_data;
```

### Step 5: Run CDC Test Matrix

**5.1 Execute Full Test Suite**

The `test_cdc_matrix.sh` script tests all CDC scenarios:

```bash
cd sources/cockroachdb/scripts

# Run all tests (JSON + Parquet, all table types, all split options)
./test_cdc_matrix.sh

# Or test specific format
./test_cdc_matrix.sh json      # JSON format only
./test_cdc_matrix.sh parquet   # Parquet format only
```

**What it does:**
1. Creates test tables in CockroachDB
2. Creates changefeeds to Azure Blob Storage
3. Runs workload (INSERT, UPDATE, DELETE)
4. Waits for CDC files to flush
5. Analyzes CDC event counts
6. Syncs files to Unity Catalog Volume
7. Leaves changefeeds running for notebook testing

**Test Matrix (8 scenarios):**
```
✅ test-json_usertable_with_split     (JSON + column families)
✅ test-json_usertable_no_split       (JSON, no column families)
✅ test-json_simple_test_with_split   (JSON + column families)
✅ test-json_simple_test_no_split     (JSON, no column families)
✅ test-parquet_usertable_with_split  (Parquet + column families)
✅ test-parquet_usertable_no_split    (Parquet, no column families)
✅ test-parquet_simple_test_with_split(Parquet + column families)
✅ test-parquet_simple_test_no_split  (Parquet, no column families)
```

**Expected Output:**
```
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
📊 TEST SUMMARY
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

Test 1/8: json_usertable_with_split - SUCCESS (files: snapshot=1 cdc=2, rows: snap=9500 ins=50 upd=400 del=100)
Test 2/8: json_usertable_no_split - SUCCESS (files: snapshot=1 cdc=2, rows: snap=9500 ins=50 upd=400 del=100)
...
Test 8/8: parquet_simple_test_no_split - SUCCESS (files: snapshot=1 cdc=2, rows: snap=500 ins=0 upd=450 del=100)

Summary:
  ✅ SUCCESS (Snapshot + CDC): 8/8
  ⚠️  PARTIAL (Snapshot only): 0/8
  ❌ FAILED: 0/8
```

**5.2 Fast Validation Mode**

After code changes, validate against existing data (60× faster):

```bash
# Validate latest test run
./test_cdc_matrix.sh --validate-only

# Validate specific timestamp
./test_cdc_matrix.sh --validate-only 1737500000

# Validate specific format only
./test_cdc_matrix.sh -v json
```

**5.3 Incremental Mode (Test Step 3)**

Test incremental CDC processing:

```bash
# Run incremental workload on latest test data
./test_cdc_matrix.sh --incremental

# Run incremental on specific timestamp
./test_cdc_matrix.sh --incremental 1737500000
```

### Step 6: Test with Databricks Notebook

**6.1 Open Test Notebook**

```
sources/cockroachdb/notebooks/test_cdc_scenario.ipynb
```

**Upload to Databricks using CLI:**

```bash
# From the repo root directory
databricks workspace import \
  /Users/<your-username>@databricks.com/test_cdc_scenario \
  --file sources/cockroachdb/notebooks/test_cdc_scenario.ipynb \
  --language PYTHON \
  --format JUPYTER \
  --profile <your-profile>
```

Replace `<your-username>` with your Databricks username and `<your-profile>` with your CLI profile name.

**6.2 Configure Test Scenario**

The notebook is pre-configured to test all 8 scenarios. Main configuration cells:

**Cell 3: Import ConnectorMode**
```python
from cockroachdb import ConnectorMode

# Available modes:
# - ConnectorMode.VOLUME: Read from Unity Catalog Volumes (file-based)
# - ConnectorMode.AZURE_PARQUET: Read Parquet from Azure Blob
# - ConnectorMode.AZURE_JSON: Read JSON from Azure Blob
# - ConnectorMode.DIRECT: Instream changefeed (live CDC)
```

**Cell 6: Select Test Scenario**
```python
# Choose which test to run
FORMAT = "parquet"  # or "json"
TABLE = "usertable"  # or "simple_test"
SPLIT = "with_split"  # or "no_split"

# Construct volume path
VOLUME_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}/{FORMAT}/defaultdb/public/test-{FORMAT}_{TABLE}_{SPLIT}"
```

**Cell 23: Configure Iterator Mode (Optional)**
```python
# Test Iterator Pattern (Community Connector)
ITERATOR_MODE = ConnectorMode.VOLUME  # Read from Volume files
# ITERATOR_MODE = ConnectorMode.DIRECT  # Read from live changefeed

connector_options = {
    "volume_path": VOLUME_PATH if ITERATOR_MODE == ConnectorMode.VOLUME else None,
    # ... other options
}
```

**6.3 Run Notebook**

**Autoloader Pattern (Cells 1-19):**
1. Loads CDC files from Volume using Autoloader
2. Applies CDC transformations
3. Merges column family fragments
4. Writes to Delta table
5. Compares results with source files

**Iterator Pattern (Cells 20-26):**
1. Uses `LakeflowConnect` iterator
2. Reads batches from Volume or instream
3. Writes to separate Delta table
4. Compares with Autoloader results

**Expected Output:**
```
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
✅ TEST COMPLETE: parquet_usertable_with_split
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

📊 Delta table: 9,950 rows
📊 Source files: 9,950 rows
✅✅✅ PERFECT MATCH! ✅✅✅
```

### Step 7: Cleanup (Optional)

**7.1 Cancel Test Changefeeds**

```bash
# List all running changefeeds
cd sources/cockroachdb/scripts
python3 changefeed_helper.py find-changefeeds \
  --table test_parquet_usertable_with_split \
  --json ../.env/cockroachdb_credentials.json

# Cancel specific changefeed
python3 changefeed_helper.py cancel-changefeed \
  --job-id <JOB_ID> \
  --json ../.env/cockroachdb_credentials.json

# Or drop all test tables at once (cancels changefeeds automatically)
psql "$COCKROACHDB_URL" <<'EOF'
SELECT 'DROP TABLE IF EXISTS ' || table_name || ' CASCADE;'
FROM information_schema.tables
WHERE table_name LIKE 'test_%';
EOF
```

**7.2 Clean Azure Test Data**

```bash
# Delete all test data
az storage blob delete-batch \
  --account-name <storage-account> \
  --account-key '<storage-key>' \
  --source changefeed-events \
  --pattern 'json/defaultdb/public/test-*'

az storage blob delete-batch \
  --account-name <storage-account> \
  --account-key '<storage-key>' \
  --source changefeed-events \
  --pattern 'parquet/defaultdb/public/test-*'
```

**7.3 Delete Azure Resources (Optional)**

```bash
# Delete entire resource group (removes everything)
az group delete --name cockroachdb-cdc-rg --yes
```

### Common Issues

**Issue: "databricks: command not found"**
```bash
pip install databricks-cli
databricks configure --token  # Configure with workspace URL and token
```

**Issue: "No timestamped directories found"**
- Run `./test_cdc_matrix.sh` first to create test data
- Check that files were synced to Volume

**Issue: "Failed to create changefeed"**
- Verify CockroachDB connection with `psql`
- Check that Azure storage credentials are correct
- Ensure table has a primary key defined

**Issue: "Permission denied" on Azure setup**
- You may lack Owner/User Access Administrator role
- Script will continue with limited functionality
- Configure EventGrid manually via Azure Portal if needed

---

### Implementation Progress (5-Step Roadmap)

| Step | Component | Status | Progress | Notes |
|------|-----------|--------|----------|-------|
| **1** | **CDC Generation** | ✅ **100%** | 8/8 scenarios | `test_cdc_matrix.sh` - All formats/tables validated |
| **2** | **One-Time Load** | ✅ **100%** | Parquet ✅, JSON ✅ | `test_cdc_scenario.ipynb` - Perfect match achieved! |
| **3** | **Incremental Load** | ✅ **100%** | Autoloader ✅, Delta MERGE ✅ | `test_cdc_matrix.sh --incremental` - Checkpoints working! |
| **4** | **DLT + Autoloader** | ⏸️ **0%** | Not started | Production streaming pipelines |
| **5** | **Community Connector** | ✅ **100%** | JSON ✅, Parquet ✅ | Iterator pattern with JSON/Parquet file support |

### Step Details

#### ✅ Step 1: CDC Generation (100% Complete)
**Tool:** `test_cdc_matrix.sh`  
**Status:** Fully validated - all 8 scenarios working

**What It Does:**
- Creates CockroachDB changefeeds to Azure Blob Storage
- Generates both Parquet and JSON formats
- Tests with and without column families
- Creates schema files automatically
- Timestamp-isolated test runs

**Test Matrix (8 scenarios):**
```
✅ test-json_usertable_with_split     (JSON + column families)
✅ test-json_usertable_no_split       (JSON, no column families)
✅ test-json_simple_test_with_split   (JSON + column families)
✅ test-json_simple_test_no_split     (JSON, no column families)
✅ test-parquet_usertable_with_split  (Parquet + column families)
✅ test-parquet_usertable_no_split    (Parquet, no column families)
✅ test-parquet_simple_test_with_split(Parquet + column families)
✅ test-parquet_simple_test_no_split  (Parquet, no column families)
```

**Validation:**
- Operation counts correct (SNAPSHOT, INSERT, UPDATE, DELETE)
- Primary key extraction working
- Column family merging working
- Deduplication working

---

#### ✅ Step 2: One-Time Load to Delta (100% Complete - Jan 8, 2026)
**Tool:** `test_cdc_scenario.ipynb`  
**Status:** ✅ Parquet working, ✅ JSON working - **PERFECT MATCH ACHIEVED!**

**What It Does:**
- Loads CDC files from Unity Catalog Volume
- Auto-detects format (Parquet/JSON)
- Applies CDC transformations
- Merges column family fragments
- Writes to Delta table with proper DELETE handling
- Deduplicates to latest state per key (initial load)
- Validates against source files

**Completed Features:**
- ✅ Parquet format (all 4 scenarios tested)
- ✅ JSON format (all 4 scenarios tested)
- ✅ Format auto-detection from path
- ✅ Schema file loading
- ✅ Column family merging
- ✅ DELETE filtering and application
- ✅ Primary key extraction from JSON `key` array
- ✅ Initial table deduplication (latest per key)
- ✅ Streaming aggregation with `max_by()`
- ✅ Perfect row count validation

**Critical Fixes Applied:**
1. **JSON Primary Key Extraction** - Fixed 99 lost DELETE events
2. **Initial Table Deduplication** - Fixed 400 duplicate UPDATE rows
3. **Result:** Delta 9,950 rows = Source 9,950 rows ✅✅✅

**Test Results:**
```
📊 Delta table: 9,950 rows
📊 Source files: 9,950 rows
✅✅✅ PERFECT MATCH! ✅✅✅
```

---

#### ✅ Step 3: Incremental Load (100% Complete - Jan 8, 2026)
**Tool:** `test_cdc_matrix.sh --incremental`  
**Status:** ✅ Working - Autoloader checkpoints + Delta MERGE tested

**What It Does:**
- Runs incremental workload on existing tables
- Generates new CDC events (updates/inserts/deletes)
- Waits for CDC files to flush
- Processes only new CDC events via Autoloader checkpoints
- Applies changes using Delta MERGE
- Validates incremental changes applied correctly

**Completed Features:**
- ✅ Incremental mode flag (`--incremental`)
- ✅ Skips table/changefeed creation
- ✅ Runs second workload on existing data
- ✅ Autoloader checkpoint tracking
- ✅ Delta MERGE with DELETE/UPDATE/INSERT support
- ✅ Only processes new files (no reprocessing)

**How It Works:**
```bash
# Step 1: Run initial test
./test_cdc_matrix.sh json

# Step 2: Run incremental load
./test_cdc_matrix.sh --incremental json

# Step 3: Verify Delta table updated correctly
```

**Code Implementation:**
- `test_cdc_matrix.sh`: Lines ~26-120 (mode flags and banners)
- `cockroachdb.py`: Lines 6012-6028 (Delta MERGE logic)

**Test Results:**
```
Mode: INCREMENTAL (reusing existing table/changefeed)
🏋️  Running workload (400 UPDATEs + 100 DELETEs + 50 INSERTs)...
⏳ Waiting 60s for CDC files to flush...
🔄 Merging incremental changes...
✅ CDC merge complete!
```

#### Why Spark MERGE instead of SDP Auto CDC?

We use explicit Spark MERGE (two-stage: stream to staging, then batch MERGE to target) rather than SDP’s **AUTO CDC** APIs for these reasons:

1. **Column family fragments** – CockroachDB with `split_column_families` writes multiple Parquet rows per logical event (one per column family). We must merge those fragments into one row per (key, timestamp, operation) before any CDC merge. Auto CDC assumes one row per change and does not perform fragment merging.

2. **RESOLVED watermark** – We filter by CockroachDB RESOLVED files so that only events up to a guaranteed-complete watermark are processed (required for multi-table consistency and column-family completeness). That filtering and watermark handling is CockroachDB-specific and happens before we have a clean “change stream” that Auto CDC could consume.

3. **HLC ordering and MERGE semantics** – We use full HLC `__crdb__updated` for ordering and for the MERGE condition (`source.__crdb__updated > target.__crdb__updated`), and we require **whenMatchedDelete** before **whenMatchedUpdate** so DELETEs remove rows instead of updating them. Explicit MERGE gives us full control over clause order and conditions.

4. **Source shape** – Our pipeline reads raw Parquet from cloud storage (Auto Loader), applies CockroachDB-specific transforms and fragment merge, then writes to staging and runs MERGE. Auto CDC expects a change-data feed (e.g. a streaming table) with a single sequence column and one row per logical change; our “source” only has that shape after the connector’s preprocessing.

Using Spark MERGE keeps the pipeline compatible with all Databricks editions (including non-SDP), preserves a retryable staging layer, and lets us document and tune MERGE behavior (e.g. in [docs/nanosecond_merge.md](docs/nanosecond_merge.md)). See also [docs/stream-changefeed-to-databricks-azure.md](docs/stream-changefeed-to-databricks-azure.md).

---

#### ⏸️ Step 4: DLT + Autoloader Production (0% - Not Started)
**Scope:** Production streaming pipelines

**Requirements:**
- Delta Live Tables integration
- Continuous Autoloader streaming
- Schema evolution handling
- Error handling and recovery
- Monitoring and alerting

**Blockers:**
- Need Step 2 & 3 complete first
- Need production use case

---

#### ✅ Step 5: Community Connector Pattern (100% - Complete - Jan 20, 2026)
**Scope:** Traditional iterator pattern for low-volume use cases

**Status:** ✅ Fully implemented with JSON and Parquet support

**Completed Features:**
- ✅ Iterator-based data loading from Unity Catalog Volumes
- ✅ JSON changefeed format support (.ndjson, .json)
- ✅ Parquet changefeed format support (.parquet)
- ✅ Auto-detection of file format from extension
- ✅ Cursor-based checkpoint tracking (filename order)
- ✅ Shared CDC processing logic with Autoloader pattern
- ✅ Compatible with existing Lakeflow framework
- ✅ Memory-efficient batch processing
- ✅ Works with mixed JSON/Parquet files in same directory

**Implementation:**
- File: `cockroachdb.py`
- Method: `_read_table_from_volume()` (lines 1075-1200)
- JSON processor: `_process_json_records()` (lines 1550-1638)
- Parquet processor: `_process_parquet_records()` (lines 1456-1548)
- File lister: `_list_volume_files()` (supports both formats)

---

### Success Metrics (Step 1 Complete)

| Metric | Target | Achieved | Status |
|--------|--------|----------|--------|
| CDC Generation | 8/8 scenarios | 8/8 passing | ✅ |
| Operation accuracy | 100% | 100% | ✅ |
| DELETE handling | Correct | Fixed Jan 8 | ✅ |
| UPDATE detection | Accurate | Fixed Jan 8 | ✅ |
| Schema files | Auto-created | Implemented Jan 8 | ✅ |
| Path handling | Flexible | Legacy + Timestamped | ✅ |
| Test speed | Fast validation | 60× faster (validate mode) | ✅ |
| JSON support | Full | Format detection implemented | ✅ |
| Code reuse | 50%+ | 55% | ✅ |
| Data integrity verification | Multi-column sum check | Implemented Jan 29 | ✅ |

---

## 📋 Current Working Strategy

### Architecture: Shared Core Logic

```
┌─────────────────────────────────────────────────────────┐
│         CockroachDB CDC Data Sources                    │
│                                                          │
│  Primary: Files (JSON/Parquet) to Storage               │
│  Testing: Instream Changefeed Connection                │
└────────────────┬────────────────────────────────────────┘
                 │ Read from storage or instream
        ┌────────┴────────┐
        │  Shared Core    │
        │  CDC Logic      │  ← 100% reused
        │                 │  
        │ • File reading  │
        │ • Instream read │
        │ • Event parsing │
        │ • PK extraction │
        │ • Coalescing    │
        │ • Timestamp CDC │
        └────────┬────────┘
                 │
     ┌───────────┴───────────┐
     │                       │
┌────▼────┐          ┌──────▼──────┐
│Iterator │          │File-Based   │
│Pattern  │          │Streaming    │
│         │          │             │
│Testing  │          │ Production  │
│Low Vol  │          │ Autoloader  │
│Files OR │          │ DLT (Files) │
│Instream │          │             │
└─────────┘          └─────────────┘
```

### Three Supported Patterns

#### 1. Community Connector (Iterator Pattern)
**Use Case:** Testing, prototyping, low-volume  
**Data Source Options:**
- **File-based (Primary):** Reads from JSON/Parquet files written by CockroachDB changefeeds to Azure Blob/Unity Catalog Volumes
- **Instream (Testing):** Direct changefeed connection for testing and development

```python
connector = LakeflowConnect()

# Option A: File-based (reads from changefeed files)
for batch in connector.read(source_table="users"):
    df = spark.createDataFrame(batch)
    # Process CDC events from files...

# Option B: Instream (for testing - direct changefeed connection)
for batch in connector.read(source_table="users", mode="instream"):
    df = spark.createDataFrame(batch)
    # Process CDC events from live changefeed...
```

**Key Features:** 
- Supports both iterator-based file consumption AND instream changefeed
- Instream mode available for testing/development scenarios

#### 2. Standalone Autoloader
**Use Case:** Validation, ad-hoc analysis, migrations  
**Data Source:** Reads from JSON/Parquet changefeed files in Unity Catalog Volumes

```python
result = load_and_merge_cdc_to_delta(
    source_table="users",
    volume_path="/Volumes/.../parquet/...",  # Points to changefeed files
    target_table_path="catalog.schema.users_delta",
    version=0  # Use latest test run
)
```

**Key Feature:** Batch processing of changefeed files with Autoloader checkpointing

#### 3. Native DLT (Production)
**Use Case:** Production pipelines, continuous streaming  
**Data Source:** Streams JSON/Parquet changefeed files as they arrive in Unity Catalog Volumes

```python
@dlt.table(name="users")
def users_cdc():
    return (
        spark.readStream
        .format("cloudFiles")  # Monitors for new changefeed files
        .option("cloudFiles.format", "parquet")  # or "json"
        .load("/Volumes/.../parquet/...")  # Changefeed file location
    )
```

**Key Feature:** Continuous file-based streaming with DLT, not direct changefeed connection

---

## 🆕 Recent Fixes & Enhancements (Feb 2, 2026)

### RESOLVED Timestamp Watermarking ✅ (Feb 2, 2026)
**Achievement:** Column family completeness guarantee and multi-table consistency

**The Problem:**
- Tables with `split_column_families=true` create **multiple Parquet files per UPDATE**
- Fragments arrive at different times → race condition
- Processing without watermarking → **data corruption** (incomplete column family updates)
- Multi-table processing → inconsistent transaction boundaries

**The Solution: CockroachDB RESOLVED Timestamps**
```python
# 1. Get RESOLVED watermark from filename
watermark = get_resolved_watermark(config, source_table)
# Returns: 1770089309000000000 (Unix nanoseconds)

# 2. Apply watermark filter to CDC data
df = raw_df.filter(extract_wall_time("__crdb__updated") <= watermark)
```

**Key Discovery - HLC Timestamp Format:**
- **RESOLVED filename**: `202601292259386834839740000000000.RESOLVED` (33 digits)
  - 14 digits: `YYYYMMDDHHMMSS` (datetime)
  - 9 digits: Nanoseconds within second
  - 10 digits: Logical clock (HLC component)
- **CDC data (`__crdb__updated`)**: `'1770067697320026017.0000000002'` (decimal string)
  - Before decimal: Wall time (19-digit nanoseconds)
  - After decimal: Logical clock (10 digits)

**Implementation Highlights:**
1. **Serverless Compatible**: Uses Azure SDK instead of `dbutils` (no `cluster_id` required)
2. **Two-Step Parsing**: 
   - Parse 33-digit HLC format from RESOLVED filename
   - Extract wall time from decimal string in CDC data
3. **No Stabilization Wait**: RESOLVED guarantees completeness (no polling needed)
4. **Multi-Table Coordination**: Compute watermark once, apply to all tables

**Test Results:**
```
✅ RESOLVED filename parsed: 202601292259386834839740000000000.RESOLVED
✅ Converted to Unix nanos: 1769710778683483974
✅ CDC data filtered: extract wall time before decimal
✅ No DECIMAL precision errors
✅ Column family completeness guaranteed
```

**Impact:**
- ✅ **Zero data corruption risk** for column family tables
- ✅ **Transaction boundary coordination** across multiple tables
- ✅ **Universal compute compatibility** (Cluster + Serverless)
- ✅ **<1% performance overhead**

**Files:**
- `crdb_to_dbx/cockroachdb_autoload.py`: `_get_resolved_watermark()`, `_resolve_watermark()`, `extract_wall_time()`
- `crdb_to_dbx/cockroachdb_azure.py`: `wait_for_changefeed_files()` with `wait_for_resolved=True`
- `learnings/COCKROACHDB_HLC_TIMESTAMP_FORMAT.md`: Deep dive into HLC formats
- `learnings/RESOLVED_WATERMARKING_IMPLEMENTATION.md`: Implementation guide
- `learnings/SERVERLESS_RESOLVED_FIX.md`: Serverless compatibility fix
- `docs/stream-changefeed-to-databricks-azure.md`: Production usage patterns

**References:**
- CockroachDB source: `pkg/ccl/changefeedccl/sink_cloudstorage.go` (line 67)
- HLC paper: https://cse.buffalo.edu/tech-reports/2014-04.pdf

---

## 🆕 Recent Fixes & Enhancements (Jan 29, 2026)

### Data Integrity Verification

#### 1. Multi-Column Sum Verification ✅ (Jan 29, 2026)
**Achievement:** Comprehensive data integrity testing beyond basic row counts

**Key Discovery:**
- Row count matching (28 = 28) can hide partial data loss
- Field3-9 had ~3% data loss despite matching row counts
- Sum verification caught -723 to -729 differences per column

**Implementation:**
- `get_column_sum()` - CockroachDB sum calculation with text handling
- `get_column_sum_spark()` - Spark equivalent with identical logic
- `regexp_replace()` - Strips non-numeric characters for YCSB schema
- Robust connection management - Single try-finally for all calculations

**Test Results:**
```
✅ ycsb_key, field0-2: Perfect match
❌ field3-9: Consistent negative differences (-723 to -729)
⚠️  Diagnosis: Missing UPDATE events in multi-column family scenario
```

**Impact:** Provides deep validation for column family fragmentation issues, incomplete MERGE operations, and partial CDC event loss that basic row count checks miss.

- File: `cockroachdb-cdc-tutorial.ipynb` Cell 5 (helpers), Cell 14 (verification)
- Doc: `CONNECTOR_EVOLUTION_STRATEGY.md` Multi-Column Sum Verification Strategy

---

## 🆕 Recent Fixes & Enhancements (Jan 21, 2026)

### Iterator Pattern Production Readiness

#### 1. Format-Agnostic Deduplication ✅ (Jan 21, 2026)
**Achievement:** Iterator pattern now produces identical results to Autoloader for both JSON and Parquet

**Key Discovery:**
- **JSON with `split_column_families=true`**: Requires column family fragment merging (2.0x duplication ratio)
- **Parquet with `split_column_families=true`**: NO fragmentation! (1.0x - already optimized by CockroachDB)

**Implementation:**
```python
# Unified deduplication in _read_table_from_volume()
1. Merge column family fragments (if needed)
2. Deduplicate by PK only (keep latest by timestamp)
3. Filter out DELETE operations
4. Return final state (9,950 rows for both formats)
```

**Test Results:**
- JSON: 20,700 raw → 10,550 merged → 10,050 deduped → 9,950 final ✅
- Parquet: 9,950 raw → 9,950 merged → 9,950 deduped → 9,950 final ✅

**Impact:** Perfect alignment between Iterator and Autoloader patterns!

- File: `cockroachdb.py` lines 1407-1456, 5480-5603
- Doc: `ITERATOR_DEDUPLICATION_FIX.md`

#### 2. Recursive Directory Support ✅ (Jan 21, 2026)
**Problem:** Files in date-based subdirectories (e.g., `2026-01-21/`) not found

**Solution:** Enhanced `_list_volume_files()` and `_list_azure_files()` for recursive traversal

**Impact:** Supports CockroachDB's date-based file organization

- File: `cockroachdb.py` lines 1194-1279, 2740-2778
- Doc: `RECURSIVE_DIRECTORY_SUPPORT.md`

#### 3. Metadata Directory Refactoring ✅ (Jan 21, 2026)
**Enhancement:** Moved schema files from `_schema.json` to `_metadata/schema.json`

**Benefits:**
- Simpler filtering (check path `/_metadata/` instead of filename prefix `_`)
- Better separation of concerns
- Follows data lake patterns (like Delta's `_delta_log/`)
- Extensible for future metadata types

**Impact:** Reduced filtering code from 16 lines to 3 lines

- Files: `cockroachdb.py`, `changefeed_helper.py`, `test_cdc_matrix.sh`
- Doc: `METADATA_DIRECTORY_REFACTOR.md`

#### 4. File-Based Mode Credential Fix ✅ (Jan 21, 2026)
**Problem:** VOLUME and AZURE modes tried to connect to CockroachDB for schema/metadata

**Solution:** Mode-aware schema inference - read from files for all file-based modes

**Affected Modes:**
- ✅ `ConnectorMode.VOLUME` - Unity Catalog Volumes
- ✅ `ConnectorMode.AZURE_PARQUET` - Azure Blob Storage (Parquet)
- ✅ `ConnectorMode.AZURE_JSON` - Azure Blob Storage (JSON)
- ✅ `ConnectorMode.AZURE_DUAL` - Azure Blob Storage (Both)

**Impact:** File-based modes now fully self-contained, no CockroachDB credentials needed

- File: `cockroachdb.py` lines 421-426, 361-521, 616-626
- Docs: `VOLUME_MODE_SCHEMA_FIX.md`, `FILE_BASED_MODES_FIX.md`

#### 5. Empty `item.name` Bug Fix ✅ (Jan 21, 2026)
**Problem:** `dbutils.fs.ls()` returned directories with empty `name` attributes

**Root Cause:** Databricks API quirk across different runtime versions

**Solution:** Extract directory name from `path` when `name` is empty

**Impact:** Timestamp directory resolution now works reliably

- File: `cockroachdb.py` lines 5055-5078
- Doc: `EMPTY_NAME_BUG_FIX.md`

#### 6. Hardcoded Timestamp Fallback Removal ✅ (Jan 21, 2026)
**Problem:** Fallback used old hardcoded timestamps instead of finding actual directories

**Solution:** Removed fallback, added clear diagnostic errors

**Impact:** Forces proper debugging, no silent errors with stale data

- File: `cockroachdb.py` lines 5097-5133
- Doc: `HARDCODED_TIMESTAMP_FALLBACK_FIX.md`

---

## 🆕 Critical Bug Fixes (Jan 7-8, 2026)

### Initial Implementation

#### 1. DELETE Filter Fix ✅ (Jan 8, 2026)
**Problem:** Delta table included 100 deleted rows (1,050 rows instead of 950)

**Root Cause:** `outputMode("complete")` writes ALL aggregated rows, including DELETEs

**Fix:** Filter DELETE operations before writing
```python
# Step 5.5: Filter out DELETE operations
df_active = df_merged.filter(F.col("_cdc_operation") != "DELETE")

query = (df_active.writeStream
    .format("delta")
    .outputMode("complete")
    ...
)
```

**Impact:** Perfect row counts - Delta matches source exactly
- File: `cockroachdb.py` lines 5138-5150
- Doc: `DELETE_FILTER_FIX.md`

#### 2. Primary Key Extraction Fix ✅ (Jan 8, 2026)
**Problem:** 400 UPDATEs misclassified as SNAPSHOTs in `json_usertable_with_split`

**Root Cause:** Using `sorted(primary_key_columns)` instead of database order

**Fix:** Removed `sorted()` call - use CockroachDB's natural key order
```python
# Before (WRONG):
cdc_key = tuple(zip(sorted(primary_key_columns), key_values))

# After (CORRECT):
cdc_key = tuple(zip(primary_key_columns, key_values))
```

**Impact:** All 8/8 test scenarios now pass with correct operation counts
- File: `cockroachdb.py` line 3481
- Doc: `PRIMARY_KEY_EXTRACTION_FIX.md`

#### 3. Schema File Auto-Creation ✅ (Jan 8, 2026)
**Problem:** Test data missing `_schema.json`, causing fallback warnings

**Solution:** `test_cdc_matrix.sh` now creates schema files automatically

**Implementation:**
1. New command: `changefeed_helper.py create-schema-file`
2. Queries CockroachDB for table metadata
3. Uploads `_schema.json` to Azure alongside data files

**Impact:** Clean runs, no warnings, faster loads (no CockroachDB fallback)
- Files: `changefeed_helper.py` lines 444-551, `test_cdc_matrix.sh` line 683
- Doc: `SCHEMA_FILE_CREATION.md`

#### 4. JSON Primary Key Extraction Fix ✅ (Jan 8, 2026 - CRITICAL!)
**Problem:** 100 DELETE events merged into 1 row in JSON CDC processing

**Root Cause:** Primary key NOT extracted from JSON `key` array before merge
```json
// JSON CDC format stores PK in array:
{"key": [1234], "after": {...}, "before": {...}}

// We extracted 'after' fields but never extracted 'key'!
// All DELETEs had ycsb_key = NULL
```

**Diagnostic Output:**
```
🔍 DELETE key extraction analysis:
   Total DELETE rows: 100
   Unique (key + updated + operation): 1  ⚠️ Should be 100!
   WARNING: 99 DELETEs have duplicate (key+timestamp+operation)!
```

**Fix:** Extract primary key from `key` array in `_add_cdc_metadata_to_dataframe()`
```python
# Extract PK from 'key' array for JSON format
if primary_key_columns and 'key' in schema_columns:
    for i, pk_col in enumerate(primary_key_columns):
        df = df.withColumn(pk_col, F.col("key").getItem(i))
```

**Impact:** 
- **Before:** All 100 DELETEs had NULL key → merged into 1 row → 10,450 total rows ❌
- **After:** Each DELETE has proper key → 100 distinct rows → 9,950 total rows ✅
- File: `cockroachdb.py` lines ~1247-1250
- Commit: `457d912`
- Doc: See Failed Approach #17

#### 5. Initial Table Deduplication Fix ✅ (Jan 8, 2026)
**Problem:** Initial table had 10,350 rows instead of 9,950 (400 extra UPDATE events)

**Root Cause:** Stored ALL CDC events (SNAPSHOT + UPDATE) as separate rows instead of deduplicating to latest state per key

**Fix:** Use Window function to keep only latest event per primary key
```python
from pyspark.sql import Window

# After excluding DELETEs, deduplicate to latest per key
window_spec = Window.partitionBy(*PK).orderBy(F.col("timestamp").desc())
final_rows = (rows_after_delete
    .withColumn("_row_num", F.row_number().over(window_spec))
    .filter(F.col("_row_num") == 1)
    .drop("_row_num")
)
```

**Output:**
```
🔍 After excluding DELETEd keys: 10,350
🔍 After deduplication (latest per key): 9,950
🔍 Duplicate events removed: 400
📝 Creating initial table: 9,950 rows
```

**Impact:**
- **Before:** 9,900 SNAPSHOT + 400 UPDATE + 50 INSERT = 10,350 rows ❌
- **After:** 9,950 rows (one per key, latest state) ✅
- **Result:** ✅✅✅ PERFECT MATCH! ✅✅✅
- File: `cockroachdb.py` lines ~5980-5995
- Commit: `7db8d2c`
- Doc: See Failed Approach #18

### Infrastructure Enhancements

#### 4. Validation Mode ✅ (Jan 8, 2026)
**Enhancement:** Fast read-only testing against existing data

**Usage:**
```bash
# Validate latest test run (60× faster)
./test_cdc_matrix.sh --validate-only

# Validate specific timestamp
./test_cdc_matrix.sh --validate-only 1767823340

# Validate single format
./test_cdc_matrix.sh -v json
```

**Benefits:**
- ⚡ **60× faster** (30 seconds vs. 30 minutes)
- 🔒 **Read-only** (no database changes)
- 🔁 **Reproducible** (same data, consistent results)
- 🎯 **Auto-detects** latest timestamp

**Impact:** Rapid development iteration, perfect for CI/CD
- File: `test_cdc_matrix.sh` lines 300-520
- Doc: `VALIDATION_MODE.md`

#### 5. Path Parsing Refactor ✅ (Jan 8, 2026)
**Problem:** Duplicated path parsing logic, tuple confusion

**Solution:** Centralized `parse_volume_path()` with explicit class

**Implementation:**
```python
class VolumePathComponents:
    """Explicit container for parsed volume path components."""
    
    def __init__(self, volume_base: str, path_prefix: str, timestamp: str = None):
        self.volume_base = volume_base
        self.path_prefix = path_prefix
        self.timestamp = timestamp
    
    @property
    def full_path(self) -> str:
        """Reconstruct full path with timestamp if present."""
        if self.timestamp:
            return f"{self.volume_base}/{self.path_prefix}/{self.timestamp}"
        return f"{self.volume_base}/{self.path_prefix}"
    
    @property
    def has_timestamp(self) -> bool:
        return self.timestamp is not None

def parse_volume_path(volume_path: str) -> VolumePathComponents:
    """Parse volume path into explicit components."""
    # Automatically detects and strips 10-digit timestamps
    # Handles both legacy and timestamped paths
    ...
```

**Benefits:**
- No tuple unpacking confusion
- Self-documenting attributes
- Computed properties for convenience
- Handles legacy paths automatically

**Impact:** Zero code duplication, cleaner API
- File: `cockroachdb.py` lines 4094-4227
- Doc: `PATH_PARSING_REFACTOR.md`

#### 6. JSON Format Support ✅ (Jan 8, 2026)
**Enhancement:** Full JSON support in `load_and_merge_cdc_to_delta`

**Problem:** Notebook function was hardcoded to Parquet only

**Solution:** Auto-detect format from volume path structure

**Implementation:**
```python
# Extract format from path
file_format = components.format_type  # 'parquet' or 'json'

# Dynamic Autoloader configuration
    df_raw = (spark.readStream
        .format("cloudFiles")
    .option("cloudFiles.format", cloudfiles_format)  # json or parquet
    .option("pathGlobFilter", f"*{table}*{file_extension}")  # .ndjson or .parquet
    .load(volume_path)
)
```

**Benefits:**
- ✅ All 8 test scenarios now work (4 JSON + 4 Parquet)
- ✅ Auto-detects from path structure
- ✅ No manual configuration needed
- ✅ Backward compatible

**Impact:** Notebook can now test all scenarios
- File: `cockroachdb.py` lines 5183-5412, 966-1016
- Doc: `JSON_FORMAT_SUPPORT.md`

#### 7. Volume Path Catalog/Schema Extraction ✅ (Jan 8, 2026)
**Enhancement:** Extract catalog/schema from volume path structure

**Problem:** Hardcoded catalog/schema in notebooks, even though it's in the path

**Solution:** Enhanced `VolumePathComponents` with extraction properties

**Path Structure:**
```
/Volumes/{unity_cat}/{unity_schema}/{volume}/{format}/{crdb_catalog}/{crdb_schema}/{scenario}/{timestamp?}
```

**New Properties:**
```python
vol_components = parse_volume_path(volume_path)
vol_components.format_type    # 'parquet' or 'json'
vol_components.crdb_catalog   # 'defaultdb', 'ecommerce', etc.
vol_components.crdb_schema    # 'public', 'staging', etc.
vol_components.scenario       # 'test-parquet_usertable_no_split'
```

**Benefits:**
- ✅ Path IS the source of truth (not hardcoded)
- ✅ Always in sync with actual path structure
- ✅ Self-documenting - clear where values come from
- ✅ Eliminates configuration drift

**Code Duplication Check:**
- ✅ Verified NO duplication between `cockroachdb.py` and `changefeed_helper.py`
- Helper script properly imports and reuses library functions

**Impact:** Cleaner notebooks, single source of truth
- File: `cockroachdb.py` lines 4094-4227 (enhanced class)
- File: `notebooks/test_cdc_scenario.ipynb` cell 6 (updated)
- Doc: `VOLUME_PATH_CATALOG_EXTRACTION.md`

#### 8. Workload Parameters Fix ✅ (Jan 8, 2026)
**Enhancement:** Eliminate hardcoded assumptions in test script

**Problem:** Hardcoded expected values disconnected from workload generation

**Solution:** Centralized workload parameters as constants

**Implementation:**
```bash
# Top of script - single source of truth
SIMPLE_TEST_INITIAL_ROWS=1000
USERTABLE_INITIAL_ROWS=10000
WORKLOAD_UPDATE_COUNT=400
WORKLOAD_INSERT_COUNT=50
WORKLOAD_DELETE_COUNT=100

# Calculated expectations
expected_unique_keys=$((initial_rows + WORKLOAD_INSERT_COUNT - WORKLOAD_DELETE_COUNT))
```

**Benefits:**
- ✅ Single source of truth for all workload sizes
- ✅ Expected values calculated, not hardcoded
- ✅ Self-documenting with explicit formula
- ✅ Easy to change workload sizes

**Impact:** Improved maintainability, eliminated magic numbers
- File: `test_cdc_matrix.sh` (constants at lines 18-24)
- Doc: `WORKLOAD_PARAMETERS_FIX.md`

#### 9. Version Parameter Behavior ✅ (Jan 8, 2026)
**Enhancement:** Explicit control over timestamp resolution

**API:**
```python
# Use exact path (legacy format or full timestamped path)
result = load_and_merge_cdc_to_delta(..., version=None)

# Auto-resolve to latest timestamp
result = load_and_merge_cdc_to_delta(..., version=0)

# Auto-resolve to oldest timestamp
result = load_and_merge_cdc_to_delta(..., version=-1)

# Auto-resolve to 2nd oldest
result = load_and_merge_cdc_to_delta(..., version=1)
```

**Logic:**
- `version=None` → Use exact path, no resolution
- `version=int` → Parse prefix, find timestamped directories, select by version

**Impact:** Flexible path handling, backward compatible
- File: `cockroachdb.py` lines 4750-4810
- Doc: `VERSION_PARAMETER_BEHAVIOR.md`

#### 7. Timestamped Path Helper ✅ (Jan 8, 2026)
**Enhancement:** Versioned test data access

**Implementation:**
```python
def get_timestamped_path(
    volume_base: str,
    path_prefix: str,
    version: int = 0,
    dbutils = None
) -> str:
    """
    Get versioned timestamped path.
    
    version=0: Latest timestamp
    version=-1: Oldest timestamp
    version=+N: Nth oldest from latest
    """
    # Lists timestamped directories (10-digit names)
    # Sorts by timestamp
    # Returns path for requested version
    ...
```

**Workaround:** Direct directory check for Databricks file listing quirk
```python
# If ls() doesn't return directories, try direct checks
for ts in [1767823340, 1767823100, ...]:
    try:
        dbutils.fs.ls(f"{full_prefix}/{ts}")
        # Found it!
    except:
        pass
```

**Impact:** Reliable timestamp resolution across all Databricks versions
- File: `cockroachdb.py` lines 4308-4428
- Doc: `TIMESTAMPED_PATH_HELPER.md`

---

## 📊 Test Results - ALL PASSING ✅

### Final Test Matrix (Jan 8, 2026)

| Test | Format | Table | Split | Snap | Ins | Upd | Del | Unique | Status |
|------|--------|-------|-------|------|-----|-----|-----|--------|--------|
| 1 | JSON | usertable | ✓ | 9500 | 50 | 400 | 100 | 9950 | ✅ PERFECT |
| 2 | JSON | usertable | ✗ | 9500 | 50 | 400 | 100 | 9950 | ✅ PERFECT |
| 3 | JSON | simple_test | ✓ | 500 | 50 | 400 | 100 | 950 | ✅ PERFECT |
| 4 | JSON | simple_test | ✗ | 500 | 50 | 400 | 100 | 950 | ✅ PERFECT |
| 5 | Parquet | usertable | ✓ | 9500 | 0* | 450* | 100 | 9950 | ✅ PERFECT* |
| 6 | Parquet | usertable | ✗ | 9500 | 0* | 450* | 100 | 9950 | ✅ PERFECT* |
| 7 | Parquet | simple_test | ✓ | 500 | 0* | 450* | 100 | 950 | ✅ PERFECT* |
| 8 | Parquet | simple_test | ✗ | 500 | 0* | 450* | 100 | 950 | ✅ PERFECT* |

**Note:** Parquet `ins=0, upd=450` is **expected behavior** (INSERTs shown as 'c' change events, documented)

### Operation Count Formula

**Formula:** `snap + ins + upd + del = total_events` (before deduplication)  
**Unique Keys:** `snap + ins - del = unique_keys` (after deduplication)

**Example (simple_test):**
- Snapshot: 500 rows
- Inserts: 50 rows
- Updates: 400 rows (to existing keys)
- Deletes: 100 rows
- **Unique keys:** 500 + 50 - 100 = **950** ✅

### Validation Speed Comparison

| Operation | Full Test | Validation Mode | Speedup |
|-----------|-----------|-----------------|---------|
| Setup | 5 min | 0 sec | ∞ |
| Changefeed creation | 8 × 2 min | 0 sec | ∞ |
| Workload execution | 8 × 1 min | 0 sec | ∞ |
| File analysis | 8 × 10 sec | 8 × 3 sec | 3× |
| **Total** | **~30 min** | **~30 sec** | **60×** |

---

## 🏗️ Implementation Details

### Core Functions

#### 1. CDC Event Processing
```python
def _add_cdc_metadata_to_dataframe(df, primary_key_columns, format='parquet'):
    """
    Add CDC metadata columns to DataFrame.
    
    Handles:
    - Snapshot cutoff timestamp detection
    - INSERT vs SNAPSHOT classification
    - UPDATE vs INSERT distinction
    - DELETE event identification
    """
    # Lines 1070-1220 in cockroachdb.py
```

#### 2. Column Family Coalescing
```python
def merge_column_family_fragments(df, primary_key_columns, debug=False):
    """
    Merge column family fragments into complete rows.
    
    When split_column_families=true, each PK has multiple fragments.
    This function merges them using groupBy + agg(first(...)).
    
    Handles:
    - Fragmentation detection
    - Streaming vs batch mode
    - NULL value handling
    - Metadata preservation
    """
    # Lines 4467-4636 in cockroachdb.py
```

#### 3. Azure File Analysis
```python
def analyze_azure_changefeed_files(
    azure_account, azure_key, container_name,
    path_prefix, file_format='parquet',
    primary_key_columns=None, debug=False
):
    """
    Analyze changefeed files in Azure Blob Storage.
    
    Returns:
    - File count
    - Row count (before dedup)
    - Operation counts (SNAPSHOT, INSERT, UPDATE, DELETE)
    - Unique keys (after dedup)
    - Snapshot cutoff timestamp
    """
    # Lines 3096-3390 for Parquet
    # Lines 3393-3750 for JSON
```

#### 4. Volume Analysis (Databricks)
```python
def analyze_volume_changefeed_files(
    volume_path, primary_key_columns=None,
    debug=False, spark=None, dbutils=None
):
    """
    Analyze changefeed files in Unity Catalog Volume.
    
    Same as analyze_azure_changefeed_files but for volumes.
    Auto-detects format, loads schema, performs analysis.
    """
    # Lines 3610-3904 in cockroachdb.py
```

#### 5. Load and Merge to Delta
```python
def load_and_merge_cdc_to_delta(
    source_table, volume_path, target_table_path,
    spark=None, dbutils=None, crdb_config=None,
    catalog=None, schema=None,
    clear_checkpoint=False, verify=True,
    compare_source=True, debug=True,
    version=None  # New: Timestamp resolution control
):
    """
    Complete end-to-end CDC pipeline.
    
    Steps:
    1. Parse and validate volume path
    2. Resolve timestamp (if version != None)
    3. Load schema (from volume or CockroachDB)
    4. Setup Autoloader with CDC metadata
    5. Merge column family fragments
    6. Filter DELETE operations
    7. Write to Delta (complete mode)
    8. Verify and compare with source
    
    Returns: Statistics dict
    """
    # Lines 4639-5287 in cockroachdb.py
```

### Utility Functions

#### Path Handling
```python
parse_volume_path(volume_path) → VolumePathComponents
get_timestamped_path(volume_base, path_prefix, version, dbutils) → str
```

#### Primary Key Management
```python
get_primary_keys(table_name, crdb_config) → List[str]
```

#### SQL Generation
```python
generate_test_table_sql(table_name, schema_type, column_families) → str
generate_test_insert_sql(table_name, schema_type, rows, ...) → str
generate_test_update_sql(table_name, schema_type, rows, ...) → str
generate_test_delete_sql(table_name, schema_type, rows, ...) → str
```

---

## 🧪 Testing Infrastructure

### Quick Reference Card

**Local Testing (Fastest - Recommended for Development)**
```bash
# Test CDC logic (pure Python, <1 sec)
python scripts/test_cdc_classification.py .cache/cdc_test_data/json/**/*.ndjson

# Analyze JSON structure
python scripts/diagnose_json_struct.py .cache/cdc_test_data/json/**/*.ndjson
```

**Cached Testing (Fast - After Initial Setup)**
```bash
# Run full tests (caches data automatically)
./test_cdc_matrix.sh json

# Validate against cached data (60× faster)
./test_cdc_matrix.sh --validate-only
```

**Databricks Testing (Slowest - Final Validation)**
```bash
# End-to-end test in notebook
# Run: notebooks/test_cdc_scenario.ipynb
```

**Development Workflow:**
1. 🔧 Edit `cockroachdb.py`
2. ⚡ Test locally with `test_cdc_classification.py` (<1 sec)
3. 🎯 If passes, validate in Databricks (15 min)
4. 🐛 If Databricks fails, add debug columns & investigate

**Time Savings:** 900× faster iteration during development!

---

### Test Data Strategy

#### ✅ Integrated Caching (IMPLEMENTED Jan 8, 2026)

**Location:** `$GIT_ROOT/.cache/cdc_test_data/{prefix}/`

**Flow:**
```
Azure Blob → $GIT_ROOT/.cache/cdc_test_data/ → Databricks Volume
                      ↓ PERSISTENT (reused!)
              Diagnostic scripts read from cache
```

**Benefits:**
- ✅ Download once, reuse many times
- ✅ Diagnostic scripts work offline (no Azure)
- ✅ 10-100× faster troubleshooting
- ✅ Mirrors production path structure
- ✅ Integrated into existing sync process (no separate tools needed)

**Cache Structure:**
```bash
$GIT_ROOT/.cache/cdc_test_data/
├── json/defaultdb/public/test-json_usertable_no_split/1767823340/
│   ├── 202601072205313357901250000000000-928374650001928192-1-72-00000000-test_json_usertable_no_split-1.ndjson
│   └── 202601072206035945830580000000001-928374650001928192-1-73-00000001-test_json_usertable_no_split-1.ndjson
├── parquet/defaultdb/public/test-parquet_simple_test_no_split/1767823340/
│   ├── 202601072219387314680380000000000-928374650001928192-1-72-00000000-test_parquet_simple_test_no_split-1.parquet
│   └── ...
└── ...
```

**CockroachDB File Naming Pattern:**
```
{timestamp}-{jobid}-{node}-{topic}-{sequence}-{table}-{file_num}.parquet
└─────┬────┘ └──┬──┘ └─┬─┘ └─┬──┘ └───┬───┘ └──┬──┘ └────┬────┘
  Nanosec   Job ID  Node  Topic  Sequence Table   File seq
  timestamp                                        number
```

**Pattern Components:**
- **Timestamp** (`202601072219387314680380000000000`) - Nanosecond precision UTC timestamp
- **Job ID** (`928374650001928192`) - Unique changefeed job identifier
- **Node** (`1`) - CockroachDB node ID that wrote the file
- **Topic** (`72`) - Internal partition/topic ID for routing
- **Sequence** (`00000000`) - File sequence number (increments per batch)
- **Table** (`test_parquet_simple_test_no_split`) - Target table name
- **File Number** (`1`) - File split number within batch (for large datasets)

**Key Properties:**
- ✅ **Chronological**: Lexicographic sort = time order
- ✅ **Deterministic**: Same pattern for JSON (`.ndjson`) and Parquet (`.parquet`)
- ✅ **Traceable**: Job ID links file back to changefeed
- ✅ **Analyzable**: Timestamp enables CDC operation classification

**How It Works:**
1. `test_cdc_matrix.sh` calls `sync_azure_to_volume_compact.py`
2. Sync script checks cache first before downloading
3. Downloaded files persist in cache for reuse
4. Diagnostic scripts use cached files directly
5. No duplicate downloads!

**Usage:**
```bash
# Normal operation (automatic caching)
./test_cdc_matrix.sh json                        # Creates data, syncs (caches automatically)

# Diagnostic scripts use cache directly
./diagnose_json_struct.py .cache/cdc_test_data/json/.../file.ndjson

# Force re-download (bypass cache)
python3 sync_azure_to_volume_compact.py --prefix ... --no-cache

# Clear cache (free disk space)
rm -rf .cache/cdc_test_data
```

**Cache Management:**
- ✅ Automatic: Enabled by default
- ✅ Persistent: Survives across runs
- ✅ Safe: Added to `.gitignore`
- ✅ Inspectable: Standard directory structure
- ✅ Manual cleanup: `rm -rf $GIT_ROOT/.cache`

**Status:** ✅ **COMPLETE** - Integrated into sync_azure_to_volume_compact.py

---

### Efficient Troubleshooting Workflow

**Recommended Pattern (with automatic caching + local testing):**
```bash
# ═══════════════════════════════════════════════════════════════════
# STEP 1: Generate test data once (caches automatically)
# ═══════════════════════════════════════════════════════════════════
./test_cdc_matrix.sh json
# ✅ Files automatically cached to $GIT_ROOT/.cache/cdc_test_data/

# ═══════════════════════════════════════════════════════════════════
# STEP 2: Develop/debug logic locally (< 1 second per iteration!)
# ═══════════════════════════════════════════════════════════════════

# Test CDC classification logic (pure Python, no Spark)
python sources/cockroachdb/scripts/test_cdc_classification.py \
  .cache/cdc_test_data/json/defaultdb/public/test-json_usertable_no_split/1767823340/*.ndjson

# Diagnose JSON structure (understand data format)
python sources/cockroachdb/scripts/diagnose_json_struct.py \
  .cache/cdc_test_data/json/defaultdb/public/test-json_usertable_no_split/1767823340/*.ndjson

# ═══════════════════════════════════════════════════════════════════
# STEP 3: Once local tests pass, validate in Databricks
# ═══════════════════════════════════════════════════════════════════
# Run test_cdc_scenario.ipynb in Databricks

# ═══════════════════════════════════════════════════════════════════
# STEP 4: If Databricks behaves differently, add debug columns
# ═══════════════════════════════════════════════════════════════════
# Edit cockroachdb.py to add debug columns that persist to Delta
# Then query Delta table to inspect actual values

# ═══════════════════════════════════════════════════════════════════
# STEP 5: Subsequent runs reuse cache (super fast validation)
# ═══════════════════════════════════════════════════════════════════
./test_cdc_matrix.sh json                        # Reuses cached files ⚡
./test_cdc_matrix.sh --validate-only             # Even faster validation

# Force re-download only when data changes
python3 scripts/sync_azure_to_volume_compact.py --prefix ... --no-cache
```

**Benefits:**
- ⚡ **10-100× faster** diagnostic runs (no Azure API calls)
- 🔒 **Offline troubleshooting** (works without network)
- 💰 **Reduced costs** (fewer Azure API calls)
- 🎯 **Reproducible** (same data across multiple runs)
- 🔧 **Zero extra tools** (integrated into existing workflow)

---

### Multi-Column Sum Verification Strategy (Jan 29, 2026)

**Purpose:** Comprehensive data integrity verification beyond basic row counts

**Problem Statement:**
- Basic verification (min/max/count) can miss incomplete CDC events
- UPDATE events that only modify specific columns may not affect row counts
- Column family fragmentation issues can cause partial data loss
- Row count matches don't guarantee all column data is correct

**Solution:** Sum all numeric columns and compare source vs target

**Implementation Details:**

#### 1. CockroachDB Sum Calculation (`get_column_sum()`)
```python
def get_column_sum(conn, table_name, column_name):
    """
    Sum numeric columns, handling text columns with embedded numbers.
    Strips non-numeric characters before casting to BIGINT.
    """
    with conn.cursor() as cur:
        cur.execute(f"""
            SELECT SUM(
                CASE
                    WHEN regexp_replace({column_name}::TEXT, '[^0-9]', '', 'g') = '' THEN 0
                    ELSE regexp_replace({column_name}::TEXT, '[^0-9]', '', 'g')::BIGINT
                END
            )
            FROM {table_name}
        """)
        return cur.fetchone()[0]
```

**Key Features:**
- Handles mixed text/numeric columns (e.g., `"updated_at_1769625671"`)
- Uses `regexp_replace()` to strip non-numeric characters
- Casts to `BIGINT` for consistent comparison
- Handles empty strings gracefully (returns 0)

#### 2. Spark Sum Calculation (`get_column_sum_spark()`)
```python
def get_column_sum_spark(df, column_name):
    """
    Spark equivalent with identical logic.
    """
    from pyspark.sql import functions as F
    
    result = df.select(
        F.sum(
            F.when(
                F.regexp_replace(F.col(column_name).cast('string'), '[^0-9]', '') == '',
                0
            ).otherwise(
                F.regexp_replace(F.col(column_name).cast('string'), '[^0-9]', '').cast('bigint')
            )
        ).alias('sum')
    ).collect()[0]['sum']
    
    return result
```

#### 3. Verification Loop (Cell 14)
```python
columns_to_verify = ['ycsb_key', 'field0', 'field1', 'field2', 'field3', 
                     'field4', 'field5', 'field6', 'field7', 'field8', 'field9']

all_columns_match = True
for col in columns_to_verify:
    source_col_sum = get_column_sum(conn, source_table, col)
    target_col_sum = get_column_sum_spark(target_df, col)
    
    if source_col_sum != target_col_sum:
        all_columns_match = False
        diff = (target_col_sum or 0) - (source_col_sum or 0)
        print(f"❌ {col:12s}: Source={source_col_sum:,} | Target={target_col_sum:,}")
        print(f"   ⚠️  Difference: {diff:+,}")
```

**Benefits:**
- **Detects partial updates** - Missing UPDATE events show as sum differences
- **Detects incomplete merges** - Column family fragmentation issues caught
- **Detects missing DELETEs** - Row count matches but sums don't
- **Column-specific diagnosis** - Identifies exactly which fields have issues
- **Handles YCSB schema** - Works with text columns containing timestamps

**Test Results (Jan 29, 2026):**

✅ **Successfully detected real data integrity issue:**
```
📊 Column Sums Comparison (All Fields):
--------------------------------------------------------------------------------
✅ ycsb_key    : Source=               2,394 | Target=               2,394
✅ field0      : Source=       1,769,729,024 | Target=       1,769,729,024
✅ field1      : Source=              23,968 | Target=              23,968
✅ field2      : Source=              23,996 | Target=              23,996
❌ field3      : Source=              24,024 | Target=              23,301
   ⚠️  Difference: -723
❌ field4      : Source=              24,052 | Target=              23,328
   ⚠️  Difference: -724
❌ field5      : Source=              24,080 | Target=              23,355
   ⚠️  Difference: -725
❌ field6      : Source=              24,108 | Target=              23,382
   ⚠️  Difference: -726
❌ field7      : Source=              24,136 | Target=              23,409
   ⚠️  Difference: -727
❌ field8      : Source=              24,164 | Target=              23,436
   ⚠️  Difference: -728
❌ field9      : Source=              24,192 | Target=              23,463
   ⚠️  Difference: -729

⚠️  Some column sums do not match - check data integrity
```

**Analysis:**
- ✅ Primary key sum matches (ycsb_key: 2,394)
- ✅ field0, field1, field2 match perfectly
- ❌ field3-field9 show consistent negative differences (-723 to -729)
- **Diagnosis**: Missing UPDATE events for fields 3-9 in multi-column family scenario
- **Root Cause**: Possible incomplete column family fragment merging

**Impact:**
- Basic row count (28) matched - would have been missed without sum verification
- Multi-column sum test caught ~3% data loss in fields 3-9
- Provides specific column-level diagnosis for debugging

**When to Use:**
- ✅ `update_delete` mode verification (requires exact match)
- ✅ Multi-column family tables (fragmentation detection)
- ✅ After MERGE operations (completeness verification)
- ✅ Production monitoring (continuous validation)
- ⚠️ `append_only` mode (sums will naturally differ due to history retention)

**Connection Management:**
```python
# Critical: Keep connection open for all calculations
conn = get_cockroachdb_connection()
try:
    # All sum calculations here
    for col in columns_to_verify:
        source_sum = get_column_sum(conn, source_table, col)
        # ...
finally:
    # Always close connection at the very end
    conn.close()
```

**Lesson Learned:** Row count matching is necessary but not sufficient for data integrity verification. Multi-column sum verification provides deep validation that catches partial data loss, incomplete merges, and column-specific CDC issues that would otherwise go undetected.

**References:**
- Notebook: `cockroachdb-cdc-tutorial.ipynb` Cell 5 (helper functions)
- Notebook: `cockroachdb-cdc-tutorial.ipynb` Cell 14 (verification logic)
- Test Run: Jan 29, 2026 (detected field3-9 discrepancies)

---

### Test Matrix Script
**File:** `sources/cockroachdb/scripts/test_cdc_matrix.sh`

**Modes:**
1. **Full Test Mode** (creates changefeeds)
   ```bash
   ./test_cdc_matrix.sh              # All formats
   ./test_cdc_matrix.sh json         # JSON only
   ./test_cdc_matrix.sh parquet      # Parquet only
   ```

2. **Validation Mode** (read-only, uses Azure)
   ```bash
   ./test_cdc_matrix.sh --validate-only
   ./test_cdc_matrix.sh -v json 1767823340
   ```

**Test Matrix:**
- 2 formats: Parquet, JSON
- 2 tables: usertable (YCSB), simple_test
- 2 configurations: with/without split_column_families
- **Total:** 8 test scenarios

**Workload:**
- Snapshot: 1,000 rows (usertable) or 500 rows (simple_test)
- Insert: 50 new rows
- Update: 400 existing rows
- Delete: 100 rows

### Helper Script
**File:** `sources/cockroachdb/scripts/changefeed_helper.py`

**Commands:**
```bash
# Primary keys
changefeed_helper.py get-primary-keys --table users --json creds.json

# SQL generation
changefeed_helper.py generate-test-sql --table users --rows 1000

# Schema file creation
changefeed_helper.py create-schema-file --table users --prefix path/to/data

# File analysis
changefeed_helper.py analyze-files --format parquet --account xxx --key yyy

# Timestamped paths
changefeed_helper.py get-timestamped-path --volume-base /Volumes/... --version 0
```

### Test Notebooks
1. **test_cdc_scenario.ipynb** - End-to-end CDC testing
2. **load_parquet_files.ipynb** - Manual file inspection
3. **cockroachdb.ipynb** - Unit tests

### Local Testing Scripts (Fast Iteration)

#### `test_cdc_classification.py` - Pure Python CDC Logic Verification

**Purpose:** Test CDC classification logic locally without PySpark/Databricks

**Usage:**
```bash
# Test specific cached file
python sources/cockroachdb/scripts/test_cdc_classification.py \
  .cache/cdc_test_data/json/defaultdb/public/test-json_usertable_no_split/1767823340/*.ndjson \
  1767823531335790125.0

# Auto-detect snapshot cutoff
python sources/cockroachdb/scripts/test_cdc_classification.py \
  .cache/cdc_test_data/json/.../file.ndjson
```

**Output:**
```
📊 Operation Counts:
   ✅ SNAPSHOT    :     0 (  0.0%)
   ✅ INSERT      :    50 (  9.1%)
   ✅ UPDATE      :   400 ( 72.7%)
   ✅ DELETE      :   100 ( 18.2%)
   ✅ UNKNOWN     :     0 (  0.0%)

   Total: 550 records
```

**Benefits:**
- ⚡ **Instant feedback** (<1 second vs minutes in Databricks)
- 🔧 **No PySpark required** (pure Python, works everywhere)
- 🐛 **Perfect for debugging** (shows exact classification logic)
- 📊 **Validates logic** before pushing to Databricks
- 💾 **Works offline** (uses cached data)

**Use Cases:**
1. **Validate CDC logic changes** - Test locally before Databricks
2. **Investigate UNKNOWN rows** - See why classification fails
3. **Verify test data** - Confirm files have expected operations
4. **Quick sanity checks** - Ensure data structure is correct

**Example Workflow:**
```bash
# 1. Make code change to cockroachdb.py
vim sources/cockroachdb/cockroachdb.py

# 2. Test logic locally (fast!)
python sources/cockroachdb/scripts/test_cdc_classification.py .cache/cdc_test_data/json/**/*.ndjson

# 3. If local test passes, test in Databricks
# Run test_cdc_scenario.ipynb

# 4. If Databricks fails but local passes, add debug columns
# (helps identify Spark-specific issues)
```

**Pro Tip:** Use this for **TDD-style development**:
```bash
# Red: Write test first (expect specific counts)
python test_cdc_classification.py file.ndjson | grep "SNAPSHOT.*9500"

# Green: Fix code until test passes
# Edit cockroachdb.py...
python test_cdc_classification.py file.ndjson | grep "SNAPSHOT.*9500"  # ✅

# Refactor: Clean up code while test still passes
```

### Troubleshooting Workflow (Tested Jan 8, 2026)

**Problem:** CDC operations classified as UNKNOWN in Databricks

**Solution Path:**
```
1. Test locally first (fast iteration)
   ↓
2. If local test passes → Spark-specific issue
   ↓
3. Add debug columns to persist data
   ↓
4. Query Delta table to inspect
   ↓
5. Fix Spark-specific issue
```

**Example:**
```bash
# Step 1: Test classification logic locally (< 1 second)
python sources/cockroachdb/scripts/test_cdc_classification.py \
  .cache/cdc_test_data/json/defaultdb/public/test-json_usertable_no_split/1767823340/*.ndjson

# Output: ✅ 0 UNKNOWN rows (logic is correct!)

# Step 2: Logic works locally but fails in Databricks → Spark issue!

# Step 3: Add debug columns in cockroachdb.py
df = df.withColumn("_debug_after_first_10", F.substring(F.col("_after_json"), 1, 10))
df = df.withColumn("_debug_before_first_10", F.substring(F.col("_before_json"), 1, 10))

# Step 4: Re-run Databricks notebook, then query Delta table
spark.read.table("...").filter(F.col("_cdc_operation") == "UNKNOWN") \
    .select("_debug_after_first_10", "_debug_before_first_10", "_after_json") \
    .show(truncate=False)

# Step 5: Inspect output to find root cause
# (e.g., JSON strings look different than expected)
```

**Time Savings:**
| Iteration | Without Local Test | With Local Test | Savings |
|-----------|-------------------|-----------------|---------|
| 1st fix attempt | 15 min (Databricks) | 1 sec (local) | **900×** |
| 2nd fix attempt | 15 min (Databricks) | 1 sec (local) | **900×** |
| 3rd fix attempt | 15 min (Databricks) | 1 sec (local) | **900×** |
| **Total (3 iterations)** | **45 min** | **3 sec + 15 min (final)** | **3× faster** |

**Key Insight:** Test logic locally first to eliminate 90% of issues before touching Databricks!

---

## 📚 Documentation

### Primary Documents (40+ files)

#### Core Documentation
1. **CONNECTOR_EVOLUTION_STRATEGY.md** - This document (strategy, architecture, testing)
2. **docs/cockroachdb-cdc-tutorial.ipynb** - Complete end-to-end CDC tutorial (Azure + Databricks)
   - 4 CDC modes: append_only + update_delete × single_cf + multi_cf
   - Full workload simulation (YCSB benchmark)
   - Automated diagnosis and verification
   - Self-contained with helper modules
3. **TEST_CACHE_STRATEGY.md** - Local caching for efficient troubleshooting
4. **Local Testing Scripts** - Fast iteration tools (see section above)
   - `test_cdc_classification.py` - Pure Python CDC logic verification
   - `diagnose_json_struct.py` - JSON structure analysis

#### Tutorial & User-Facing Resources
5. **docs/cockroachdb-cdc-tutorial.ipynb** - Interactive notebook for users
   - Prerequisites and setup instructions
   - Step-by-step CDC pipeline creation
   - 4 CDC ingestion modes with automatic function selection
   - Comprehensive diagnostics and troubleshooting
   - Helper modules: `cockroachdb_ycsb.py`, `cockroachdb_debug.py`, `cockroachdb_conn.py`, `cockroachdb_azure.py`, `cockroachdb_autoload.py`
6. **docs/stream-changefeed-to-databricks-azure.md** - Blog post: "Stream a Changefeed to Databricks (Azure Edition)"
   - Getting started guide for CockroachDB CDC to Databricks
   - 6-step basic CDC pipeline setup (includes Azure access configuration)
   - Append-only (SCD Type 2) and update-delete (SCD Type 1) modes
   - Column family fragment merging for wide tables
   - Code examples with accurate imports and syntax

#### Tutorial Helper Modules (Jan 30, 2026)
7. **docs/cockroachdb_ycsb.py** - YCSB table and workload utilities
   - Table creation with configurable column families
   - Snapshot and workload generation with NULL support
   - Statistics and verification functions
   - Deduplication utilities for append_only mode
8. **docs/cockroachdb_debug.py** - Comprehensive CDC diagnosis utilities
   - All-in-one diagnosis: stats + verification + deep analysis
   - Smart append_only mode handling (deduplication + filtering)
   - Column family sync diagnosis with auto-detection
   - Row-by-row comparison with timestamp awareness
   - CDC event analysis from Azure/Volume files
9. **docs/cockroachdb_conn.py** - CockroachDB connection management
   - Both cursor-based and native pg8000 APIs
   - SSL configuration and host parsing
10. **docs/cockroachdb_azure.py** - Azure changefeed utilities
    - Changefeed creation and management
    - File monitoring and validation
11. **docs/cockroachdb_autoload.py** - Autoloader ingestion functions
    - 4 CDC ingestion modes with automatic selection
    - Column family fragment merging
    - Staging table management for update_delete mode

#### Session Summaries
12. **SESSION_SUMMARY.md** - Jan 8, 2026 session summary
13. **FINAL_FIX_SUMMARY.md** - Jan 7, 2026 session summary

#### Bug Fixes & Features
6. **DELETE_FILTER_FIX.md** - DELETE handling fix
7. **PRIMARY_KEY_EXTRACTION_FIX.md** - PK sorting fix  
8. **SCHEMA_FILE_CREATION.md** - Auto schema generation
9. **COALESCE_FIX_SUMMARY.md** - Coalescing improvements

#### Infrastructure
10. **VALIDATION_MODE.md** - Fast testing guide (60× faster)
11. **PATH_PARSING_REFACTOR.md** - Path handling refactor
12. **VERSION_PARAMETER_BEHAVIOR.md** - Timestamp resolution API
13. **TIMESTAMPED_PATH_HELPER.md** - Versioned path access
14. **TIMESTAMP_PATH_IMPLEMENTATION.md** - Path isolation
15. **TEST_DATA_CLEANUP_FIX.md** - Data isolation

#### Analysis & Root Cause
16. **DELETE_DOUBLING_ROOT_CAUSE.md** - DELETE issue analysis
17. **TEST_VALIDATION_SUMMARY.md** - Test results & validation

### Code Quality Metrics (Updated Jan 30, 2026)

| Metric | Value |
|--------|-------|
| Total lines of code | ~10,000+ |
| Core CDC functions | 15+ |
| Utility functions | 30+ |
| Helper modules | 5 (ycsb, debug, conn, azure, autoload) |
| Tutorial notebooks | 1 interactive (1,825 lines) |
| Test coverage | 8/8 scenarios |
| Documentation files | 60+ |
| Linter errors | 0 |
| Type hints | Extensive |
| Docstrings | Complete |
| Diagnosis functions | 12+ (smart append_only handling) |

### Recent Documentation (Jan 30, 2026) - Diagnosis Improvements
1. **DIAGNOSIS_DEDUPLICATION_FIX.md** - Fixed append_only mode deduplication in verification
2. **REFACTOR_DEDUPLICATION_REUSABLE.md** - Extracted deduplication logic to reusable functions
3. **SMART_DIAGNOSIS_RECALCULATION.md** - Auto-recalculation of mismatches after deduplication
4. **SELF_CONTAINED_DIAGNOSIS.md** - All-in-one diagnosis with no external dependencies
5. **FIX_APPEND_ONLY_KEY_COMPARISON.md** - Filter target to source keys for accurate comparison
6. **REFACTOR_USE_CONN_UTILITY.md** - Use reusable connection utilities (no duplication)
7. **PG8000_NATIVE_API_FIX.md** - Fixed API compatibility for cursor vs native
8. **SESSION_SUMMARY_DIAGNOSIS_IMPROVEMENTS.md** - Complete session summary

### Recent Documentation (Jan 21, 2026) - Format & Infrastructure
1. **ITERATOR_DEDUPLICATION_FIX.md** - Format-agnostic deduplication (Iterator = Autoloader)
2. **ROW_COUNT_MISMATCH_FIX.md** - Timestamp resolution diagnostic
3. **METADATA_DIRECTORY_REFACTOR.md** - `_metadata/schema.json` architecture
4. **FILE_BASED_MODES_FIX.md** - VOLUME/AZURE credential-free operation
5. **VOLUME_MODE_SCHEMA_FIX.md** - Schema inference from files
6. **RECURSIVE_DIRECTORY_SUPPORT.md** - Date-based partition support
7. **EMPTY_ITEM_NAME_FIX.md** - Databricks API quirk handling
8. **EMPTY_NAME_BUG_FIX.md** - `dbutils.fs.ls()` empty name fallback
9. **HARDCODED_TIMESTAMP_FALLBACK_FIX.md** - Removed silent fallback
10. **CACHE_ASSUMPTION_REMOVAL.md** - Removed incorrect cache detection
11. **CONNECTION_URL_PRIORITY_FIX.md** - Credential parsing priority
12. **RESYNC_COMMAND_FEATURE.md** - `test_cdc_matrix.sh --resync`
13. **VOLUME_SYNC_DIRECTORY_FIX.md** - Directory structure preservation

### Investigation Documents (Jan 27, 2026)
1. **verify_decimal_issue.md** - Investigation plan for `__crdb__updated` DECIMAL(2^31, 0) precision issue

---

## 🎯 Production Readiness Checklist

### ✅ Completed

- [x] Column family fragment merging
- [x] **RESOLVED timestamp watermarking** (Feb 2, 2026) - Guarantees column family completeness and multi-table consistency
- [x] DELETE operation filtering
- [x] Primary key extraction (database order)
- [x] INSERT vs SNAPSHOT detection (timestamp-based)
- [x] UPDATE vs INSERT distinction (Parquet limitation documented)
- [x] Schema file auto-creation
- [x] Timestamped path isolation
- [x] Version-based path resolution
- [x] Backward compatibility (legacy paths)
- [x] Fast validation mode (60× faster)
- [x] Comprehensive testing (8/8 pass)
- [x] Complete documentation
- [x] Zero linter errors
- [x] Spark Connect compatibility
- [x] Unity Catalog support
- [x] Databricks Serverless compatibility (cluster-agnostic code)

### 📋 Optional Enhancements

- [ ] Unit tests for path parsing
- [ ] CI/CD integration using validation mode
- [ ] Performance benchmarking suite
- [ ] S3/ABFSS support (currently Azure-only)
- [ ] DLT production deployment example
- [ ] foreachBatch + MERGE for continuous streaming
- [ ] **NULL Value Testing** - Test NULL conditions for both column family and non-column family scenarios:
  - [ ] Test INSERT with NULL columns (single column family)
  - [ ] Test INSERT with NULL columns (multi-column family with split_column_families)
  - [ ] Test UPDATE to NULL (explicit NULL assignment)
  - [ ] Test column family with all NULLs (should NOT emit fragment for non-PK families)
  - [ ] Test column family with mixed NULL/non-NULL values
  - [ ] Test coalescing logic preserves NULLs correctly
  - [ ] Verify NULL vs. not-updated distinction in MERGE operations
  - [ ] Test YCSB schema with NULL values (text columns with embedded numbers)
  - [ ] Validate sum verification handles NULL correctly (treats as 0)
- [x] **DECIMAL Precision Issue - SOLVED!** (Jan 27, 2026) - Root cause: `.RESOLVED` files (CDC watermarks) use `DECIMAL(2147483647, 0)` encoding, data files don't. Solution: Filter `.RESOLVED` files using `pathGlobFilter` (see `docs/DECIMAL_MYSTERY_SOLVED.md`)

### 🐛 GitHub Issues Filed (CockroachDB)

**Issue #161962** - Bug Report (Jan 28, 2026)  
**Title**: changefeedccl: parquet .RESOLVED files use DECIMAL(2147483647, 0) breaking Spark consumers  
**Status**: Open | **Jira**: CRDB-59198  
**URL**: https://github.com/cockroachdb/cockroach/issues/161962

**Problem**: `.RESOLVED` files (CDC watermark tracking) use `DECIMAL(2147483647, 0)` precision, exceeding Apache Spark's maximum DECIMAL precision (38). Affects all Spark-based CDC consumers (Databricks, AWS Glue, EMR, Azure Synapse).

**Root Cause**:
- Data files: Use `StringType` for timestamps (`__crdb__updated`) ✅
- `.RESOLVED` files: Use `DECIMAL(2147483647, 0)` for `resolved` column ❌
- Caused by: `pkg/util/parquet/schema.go` line 176 sets precision to `math.MaxInt32` when precision=0

**Current Workaround**: 
```python
# Filter .RESOLVED files from data reads (avoid DECIMAL precision issue)
.option("pathGlobFilter", "*usertable*.parquet")  # Excludes .RESOLVED files

# BUT still use RESOLVED filenames for watermarking (Feb 2, 2026)
watermark = get_resolved_watermark(config, source_table)  # Read filename, not content
df = raw_df.filter(extract_wall_time("__crdb__updated") <= watermark)
```

**Suggested Fix**: Change `.RESOLVED` files to use `StringType` (matching data files)

**⚠️ IMPORTANT UPDATE (Feb 2, 2026):**
While RESOLVED files cannot be read as data due to DECIMAL precision, their **filenames contain critical watermark information** that should be used to guarantee column family completeness. The proper approach is:
1. **Filter out** `.RESOLVED` files when reading CDC data (avoid DECIMAL error)
2. **Parse filenames** to extract watermark timestamps for data filtering
3. This ensures all column family fragments have arrived before processing

See: `learnings/COCKROACHDB_HLC_TIMESTAMP_FORMAT.md` for implementation details

---

**Issue #161963** - Feature Request (Jan 28, 2026)  
**Title**: changefeedccl: Enable primary key metadata in production Parquet files (already available in test builds)  
**Status**: Open | **Jira**: CRDB-59199  
**URL**: https://github.com/cockroachdb/cockroach/issues/161963

**Problem**: Production Parquet files don't include primary key metadata, forcing manual configuration in all downstream CDC consumers. Creates internal inconsistency—JSON, Avro, and Kafka formats already include PK information.

**Comparison with CockroachDB's own formats**:
- **JSON**: ✅ Primary keys in separate key message (`encoder_json.go:52-56`)
- **Avro**: ✅ Primary keys in separate key schema (`encoder_avro.go:27-29, 147`)
- **Kafka**: ✅ Primary keys in Kafka message key field (`sink_kafka.go:401`)
- **Parquet**: ❌ **No primary key metadata** in production (✅ available in test builds)

**Solution**: Enable test metadata for production builds (already implemented in `parquet.go:238-327`)

**Proposed Metadata**:
```
cockroach.primary_keys: "col1,col2"
```

**Benefits**:
- Self-describing Parquet files
- Auto-generate MERGE operations in Spark/Databricks
- Eliminates 500-line configuration files for multi-table CDC pipelines
- Minimal overhead: ~50-200 bytes per file (0.0002%)

**Impact**: All CockroachDB changefeed users consuming Parquet files (Databricks, AWS Glue, EMR, Azure Synapse)

---

## 🚫 Failed Approaches (DO NOT REVISIT)

*This section documents strategies that were attempted but did not work, to prevent revisiting them in the future.*

### ❌ 1. Alphabetically Sorting Primary Keys
**Attempted:** Jan 7, 2026  
**Problem:** UPDATEs misclassified as SNAPSHOTs

**What we tried:**
```python
# WRONG - Don't do this!
cdc_key = tuple(zip(sorted(primary_key_columns), key_values))
```

**Why it failed:**
- CockroachDB emits keys in database-defined order, not alphabetical
- Sorting creates mismatched keys between fragments
- Result: Failed deduplication, incorrect operation classification

**Correct approach:**
```python
# CORRECT - Use database order
cdc_key = tuple(zip(primary_key_columns, key_values))
```

**Lesson:** Never assume alphabetical ordering - always use source system's order.

**References:**
- PRIMARY_KEY_EXTRACTION_FIX.md
- cockroachdb.py line 3481

---

### ❌ 2. Writing DELETE Rows to Delta Table
**Attempted:** Before Jan 8, 2026  
**Problem:** Delta table had 100 extra rows (deleted rows remained)

**What we tried:**
```python
# WRONG - Writes all rows including DELETEs
df_merged.writeStream
    .format("delta")
    .outputMode("complete")
    .toTable(target_table_path)
```

**Why it failed:**
- `outputMode("complete")` doesn't support MERGE with DELETE clauses
- DELETE rows treated as regular data rows
- Result: Deleted keys remain in table

**Correct approach:**
```python
# CORRECT - Filter DELETEs before writing
df_active = df_merged.filter(F.col("_cdc_operation") != "DELETE")

df_active.writeStream
    .format("delta")
    .outputMode("complete")
    .toTable(target_table_path)
```

**Lesson:** Complete mode replaces entire table - filter unwanted rows before write.

**References:**
- DELETE_FILTER_FIX.md
- cockroachdb.py lines 5138-5150

---

### ❌ 3. Non-Timestamped Test Data Paths
**Attempted:** Before Jan 7, 2026  
**Problem:** Test runs contaminated each other, DELETE doubling

**What we tried:**
```bash
# WRONG - All runs write to same directory
path_prefix="${format}/${catalog}/${schema}/test-${test_name}/"
```

**Why it failed:**
- Multiple test runs accumulated in same directory
- DELETE events from different runs combined
- Result: 200 DELETEs instead of 100 per run

**Correct approach:**
```bash
# CORRECT - Unique timestamp per run
TEST_RUN_TIMESTAMP=$(date +%s)
path_prefix="${format}/${catalog}/${schema}/test-${test_name}/${TEST_RUN_TIMESTAMP}/"
```

**Lesson:** Isolate test data by timestamp for reproducible tests.

**References:**
- TIMESTAMP_PATH_IMPLEMENTATION.md
- test_cdc_matrix.sh line 17, 341

---

### ❌ 4. Tuple-Based Path Parsing
**Attempted:** Before Jan 8, 2026  
**Problem:** Tuple unpacking confusion, duplicated logic

**What we tried:**
```python
# WRONG - Tuple unpacking is error-prone
def parse_path(path):
    return (volume_base, path_prefix, timestamp)

volume_base, path_prefix, timestamp = parse_path(path)
```

**Why it failed:**
- Easy to unpack in wrong order
- No self-documentation
- Duplicated parsing logic in multiple places
- Confusing when timestamp is optional (None)

**Correct approach:**
```python
# CORRECT - Explicit class with properties
class VolumePathComponents:
    def __init__(self, volume_base, path_prefix, timestamp=None):
        self.volume_base = volume_base
        self.path_prefix = path_prefix
        self.timestamp = timestamp
    
    @property
    def full_path(self): ...
    @property  
    def has_timestamp(self): ...

components = parse_volume_path(path)
print(components.volume_base)  # Self-documenting!
```

**Lesson:** Use explicit classes instead of tuples for complex return values.

**References:**
- PATH_PARSING_REFACTOR.md
- cockroachdb.py lines 4094-4133

---

### ❌ 5. Relying on `dbutils.fs.ls()` for Directory Detection
**Attempted:** Jan 8, 2026  
**Problem:** Timestamp directories not detected even though they existed

**What we tried:**
```python
# WRONG - ls() doesn't always return subdirectories
items = dbutils.fs.ls(parent_dir)
timestamp_dirs = [item for item in items if item.isDir() and item.name.isdigit()]
```

**Why it failed:**
- Databricks file listing can be inconsistent with many files
- Caching issues cause subdirectories to not appear in ls() output
- `isDir()` implementation varies between Databricks versions
- Result: "No timestamped directories found" even when they exist

**Correct approach:**
```python
# CORRECT - Try direct checks for common timestamps
if not timestamp_dirs:
    for ts in [1767823340, 1767823100, 1767822800, ...]:
        try:
            dbutils.fs.ls(f"{parent_dir}/{ts}")
            timestamp_dirs.append({'name': str(ts), 'timestamp': ts})
            break
        except:
            pass
```

**Lesson:** Don't trust directory listings in distributed file systems - use direct checks as fallback.

**References:**
- TIMESTAMPED_PATH_HELPER.md
- cockroachdb.py lines 4395-4418

---

### ❌ 6. Using `after`/`before` Fields for Primary Key Extraction (JSON + Split Column Families)
**Attempted:** Before Jan 8, 2026  
**Problem:** UPDATE events missing primary keys, misclassified as SNAPSHOTs

**What we tried:**
```python
# WRONG - PK not in after/before when split_column_families=true
if after_data:
    key_values = [after_data.get(pk) for pk in primary_key_columns]
```

**Why it failed:**
- With split_column_families, `+pk` fragments only have `after` (no `before`)
- `+data` fragments have `before`/`after` but NO primary key columns
- Result: `key_values` contained `None`, failed deduplication

**Correct approach:**
```python
# CORRECT - Use top-level 'key' field (always present)
key_values = event_data.get('key', [])
cdc_key = tuple(zip(primary_key_columns, key_values))
```

**Lesson:** Use the 'key' field explicitly - it always contains full primary key.

**References:**
- PRIMARY_KEY_EXTRACTION_FIX.md
- cockroachdb.py lines 3472-3487

---

### ❌ 7. Rerunning Full Test Matrix for Every Fix
**Attempted:** Before Jan 8, 2026  
**Problem:** 30 minutes per test cycle, slow development

**What we tried:**
```bash
# WRONG - Full test takes 30 minutes
./test_cdc_matrix.sh
# ... wait 30 minutes ...
# Check if fix worked
```

**Why it failed:**
- Changefeed creation: 2 min × 8 = 16 min
- Workload execution: 1 min × 8 = 8 min
- Analysis: 1 min × 8 = 8 min
- Total: ~30 min per iteration
- Result: Slow feedback cycle

**Correct approach:**
```bash
# CORRECT - Validation mode, 30 seconds
./test_cdc_matrix.sh --validate-only
# ... wait 30 seconds ...
# Immediate feedback!
```

**Performance:**
- File analysis only: 3 sec × 8 = 24 sec
- **60× faster** than full test

**Lesson:** Separate data generation from validation - reuse test data for rapid iteration.

**References:**
- VALIDATION_MODE.md
- test_cdc_matrix.sh lines 300-520

---

### ❌ 8. Implicit `version` Parameter Default
**Attempted:** Jan 8, 2026  
**Problem:** Confusion about when timestamp resolution occurs

**What we tried:**
```python
# WRONG - Unclear when resolution happens
def load_and_merge_cdc_to_delta(..., version=0):  # Always resolves!
```

**Why it failed:**
- `version=0` default meant timestamp resolution always attempted
- Broke legacy paths without timestamps
- User couldn't opt-out of resolution
- Result: "No timestamped directories found" for legacy data

**Correct approach:**
   ```python
# CORRECT - Explicit None default
def load_and_merge_cdc_to_delta(..., version=None):  # Resolution opt-in
    if version is not None:
        # User explicitly requested resolution
        resolved = get_timestamped_path(..., version=version)
    else:
        # Use exact path (legacy or full path)
        resolved = volume_path
```

**Lesson:** Use `None` as default when behavior should be opt-in, not automatic.

**References:**
- VERSION_PARAMETER_BEHAVIOR.md
- cockroachdb.py lines 4750-4810

---

### ❌ 9. Checking Only String Values for JSON NULL (Missing SQL NULL)
**Attempted:** Jan 8, 2026  
**Problem:** 9,651 SNAPSHOT rows classified as UNKNOWN in Spark

**What we tried:**
```python
# WRONG - Misses SQL NULL case!
after_empty = (F.col("_after_json") == F.lit("null")) | (F.col("_after_json") == F.lit("{}"))
```

**Why it failed:**
- Spark's `F.to_json()` returns **SQL NULL** (not string `"null"`) when input column is NULL
- SQL NULL never equals anything (not even string `"null"`)
- Comparison always fails: `NULL == "null"` → FALSE in SQL
- Result: 9,651 rows with `before=NULL` misclassified as UNKNOWN

**Root Cause - Spark vs Python Behavior:**
```python
# Python (json.dumps):
json.dumps(None)  → "null"  # String!

# Spark (F.to_json):
F.to_json(null_column)  → NULL  # SQL NULL, not string!
```

**How We Found It:**
1. Local Python test passed (0 UNKNOWN rows) ✅
2. Spark test failed (9,651 UNKNOWN rows) ❌
3. Added debug columns to persist to Delta table
4. Queried Delta: `_before_json_sample: NULL` (SQL NULL, not `"null"`)
5. Realized: Need `.isNull()` check, not just string comparison!

**Correct approach:**
```python
# CORRECT - Checks for SQL NULL, string "null", AND string "{}"
after_empty = F.col("_after_json").isNull() | \
              (F.col("_after_json") == F.lit("null")) | \
              (F.col("_after_json") == F.lit("{}"))

before_empty = F.col("_before_json").isNull() | \
               (F.col("_before_json") == F.lit("null")) | \
               (F.col("_before_json") == F.lit("{}"))
```

**Lesson:** Spark's `F.to_json()` can return **THREE** different values:
1. **SQL NULL** - when input column is NULL
2. **String `"null"`** - when input is null struct (but column not NULL)
3. **String `"{}"`** - when input is empty struct

Always check for ALL three cases in Spark!

**References:**
- cockroachdb.py lines 1281-1284 (fix applied)
- `test_cdc_classification.py` updated to match Spark behavior
- Local testing documented in CONNECTOR_EVOLUTION_STRATEGY.md

**Time to Debug:** 3 hours saved by local testing approach! 
- Local test isolated logic issue immediately (< 1 sec)
- Debug columns revealed Spark-specific behavior
- Without local testing, would have spent hours in Databricks

---

### ❌ 10. Two-Phase CDC Processing (Snapshot First, Then CDC Changes)
**Attempted:** Jan 8, 2026  
**Problem:** Schema inference issues with JSON envelope columns

**What we tried:**
```python
# WRONG - Separate snapshot and CDC processing
# Phase 1: Stream snapshot files
df_snapshot = spark.readStream.format("cloudFiles").option("pathGlobFilter", "*-00000000-*")...

# Phase 2: Stream CDC files  
df_cdc = spark.readStream.format("cloudFiles").option("pathGlobFilter", "*-0000000[1-9]-*")...
```

**Why it failed:**
- Autoloader inferred `before` as STRING (not STRUCT) for snapshot files
- `to_json(before)` failed with: "Input schema STRING must be a struct"
- Separate checkpoints added complexity
- Result: `AnalysisException: INVALID_JSON_SCHEMA`

**Lesson:** Keep CDC processing unified - splitting by file sequence breaks schema inference.

**References:**
- Code Version: cockroachdb@eba8132 (pre-revert)
- Error: `[DATATYPE_MISMATCH.INVALID_JSON_SCHEMA]`

---

### ❌ 11. Using `foreachBatch` for DELETE Support
**Attempted:** Jan 8, 2026  
**Problem:** Python version mismatch between client and server

**What we tried:**
```python
# WRONG - foreachBatch requires matching Python versions
def merge_batch(batch_df, batch_id):
    delta_table.merge(...).whenMatchedDelete(...)

df.writeStream.foreachBatch(merge_batch).start()
```

**Why it failed:**
- Local client: Python 3.11
- Databricks workers: Python 3.12
- `foreachBatch` executes Python code on workers
- Result: `Python versions in client and server are different: 3.11 != 3.12`

**Lesson:** Avoid `foreachBatch` when client/server Python versions don't match - use pure Spark operations instead.

**References:**
- Exception: `Python in worker has different version: 3.12 than that in driver: 3.11`

---

### ❌ 12. Streaming Write with `complete` Output Mode + Column Family Merge
**Attempted:** Jan 8, 2026  
**Problem:** Complete mode aggregates away DELETE events

**What we tried:**
```python
# WRONG - complete mode loses DELETEs!
df_merged = merge_column_family_fragments(df_raw)  # Has aggregations

df_merged.writeStream
    .outputMode("complete")  # Replaces entire table each batch
    .toTable(target)
```

**Why it failed:**
- `complete` mode performs implicit groupBy aggregation
- Only keeps latest state per key
- DELETE events get aggregated away
- Result: 10,050 rows instead of 9,950 (100 DELETEs lost)

**Lesson:** `complete` mode with aggregations loses CDC operation history - use temp table approach instead.

---

### ❌ 13. Streaming Write with `append` Output Mode + Column Family Merge
**Attempted:** Jan 8, 2026  
**Problem:** Aggregations incompatible with append mode

**What we tried:**
```python
# WRONG - append doesn't support streaming aggregations!
df_merged = merge_column_family_fragments(df_raw)  # groupBy aggregation

df_merged.writeStream
    .outputMode("append")  # Doesn't work with aggregations
    .toTable(target)
```

**Why it failed:**
- `append` mode doesn't support streaming aggregations
- `merge_column_family_fragments` uses `groupBy().agg()`
- Result: `UnsupportedOperationException: append mode not supported for streaming aggregations`

**Lesson:** Streaming aggregations require `complete` or `update` mode - or move aggregation to batch processing.

---

### ❌ 14. Streaming Write with `update` Output Mode to Delta
**Attempted:** Jan 8, 2026  
**Problem:** Delta doesn't support update output mode

**What we tried:**
```python
# WRONG - Delta doesn't support update mode!
df_merged.writeStream
    .outputMode("update")
    .format("delta")
    .toTable(target)
```

**Why it failed:**
- Delta Lake only supports `append` and `complete` for streaming
- `update` mode is for memory/console sinks only
- Result: `DELTA_UNSUPPORTED_OUTPUT_MODE: Delta does not support Update output mode`

**Lesson:** Delta streaming only supports `append` or `complete` - use batch processing for complex transformations.

---

### ❌ 15. Treating JSON Envelope Columns as Data Columns
**Attempted:** Jan 8, 2026  
**Problem:** UPDATE and DELETE events lost during column family merge

**What we tried:**
```python
# WRONG - Merges envelope columns as data!
metadata_columns = ['_cdc_operation', '_cdc_timestamp', ...]
# Missing: 'after', 'before', 'key', 'updated'

data_columns = [col for col in all_columns if col not in metadata_columns]
# Result: ['after', 'before', 'key', 'updated', 'field0', ...] ← WRONG!

df_merged = df.groupBy('ycsb_key').agg(first(col) for col in data_columns)
```

**Why it failed:**
- JSON envelope columns (`after`, `before`) treated as data to merge
- `first()` picked one fragment's envelope, lost others
- Result: Only 10,001 SNAPSHOT + 51 INSERT (lost 400 UPDATE + 100 DELETE)

**Correct approach:**
```python
# CORRECT - Exclude envelope columns from data merge
metadata_columns = [
    ...,
    'after', 'before', 'key', 'updated',  # JSON envelope
    '_after_json', '_before_json', ...     # Debug columns
]
```

**Lesson:** JSON envelope columns are metadata, not data - exclude from aggregation.

---

### ❌ 16. Grouping by Primary Key Only During Column Family Merge
**Attempted:** Jan 8, 2026  
**Problem:** Merged ALL CDC events for a key into one row

**What we tried:**
```python
# WRONG - Loses CDC event history!
df_merged = df.groupBy('ycsb_key').agg(
    max_by(col, '_cdc_timestamp') for col in data_columns
)

# For key "user001":
#   - SNAPSHOT @ T1 (11 fragments) ━┓
#   - UPDATE @ T2 (11 fragments)   ━┫━> Only keeps latest event
#   - DELETE @ T3 (1 fragment)     ━┛
```

**Why it failed:**
- `groupBy(primary_key)` combines ALL events for a key
- Only keeps the latest event (based on timestamp)
- Result: Lost SNAPSHOT and UPDATE events, only kept DELETE

**Correct approach:**
```python
# CORRECT - Group by PK + timestamp to preserve all CDC events
df_merged = df.groupBy('ycsb_key', 'updated').agg(
    first(col, ignorenulls=True) for col in data_columns
)

# For key "user001":
#   - SNAPSHOT @ T1 (11 fragments) ━> Merged to 1 row ✅
#   - UPDATE @ T2 (11 fragments)   ━> Merged to 1 row ✅  
#   - DELETE @ T3 (1 fragment)     ━> Kept as 1 row ✅
```

**Why this also failed:**
- JSON column family fragments have microsecond timestamp variations
- Grouping by timestamp treated variations as separate events
- Result: 10,054 events instead of 10,050 (4 extra from timestamp variations)

**Lesson:** Column family merge strategy must differ between Parquet and JSON due to envelope structure differences.

**Final Solution:** Added `_cdc_operation` to groupBy key: `groupBy(PK + timestamp + operation)` - See Failed Approach #17 for the deeper root cause

**Status:** ✅ **RESOLVED** - See Failed Approaches #17 and #18

---

### ❌ 17. Not Extracting Primary Key from JSON `key` Array (Root Cause!)
**Attempted:** Jan 8, 2026  
**Problem:** All 100 DELETE events merged into 1 row

**What we tried:**
```python
# WRONG - Never extracted PK from JSON 'key' array!
# In JSON CDC format:
# {"key": [1234], "after": {...}, "before": {...}}

# We flattened 'after' but NOT 'key':
for field in after_fields:
    df = df.withColumn(field, F.col(f"after.{field}"))
# ❌ Missing: Extract ycsb_key from key[0]!

# When merge_column_family_fragments ran:
df.groupBy('ycsb_key', 'updated', '_cdc_operation').agg(...)
# All DELETEs had ycsb_key = NULL!
```

**Diagnostic that revealed the bug:**
```
🔍 DELETE key extraction analysis:
   Total DELETE rows: 100
   Unique (key + updated + operation): 1  ⚠️ Should be 100!
   WARNING: 99 DELETEs have duplicate (key+timestamp+operation)!
```

**Why it failed:**
- JSON CDC stores PK in `key` array: `key: [1234]`
- We extracted data from `after` struct but NEVER extracted `key` array values
- All 100 DELETEs had `ycsb_key = NULL`
- `groupBy(NULL, timestamp, 'DELETE')` → merged all into 1 row
- Lost 99 DELETE events!

**Correct approach:**
```python
# CORRECT - Extract PK from 'key' array for JSON format
if primary_key_columns and 'key' in schema_columns:
    for i, pk_col in enumerate(primary_key_columns):
        df = df.withColumn(pk_col, F.col("key").getItem(i))

# Now each DELETE has proper ycsb_key:
# DELETE 1: ycsb_key='user9901', timestamp=T1
# DELETE 2: ycsb_key='user9902', timestamp=T1
# DELETE 3: ycsb_key='user9903', timestamp=T1
# All 100 preserved as distinct rows! ✅
```

**Code Location:** `_add_cdc_metadata_to_dataframe()` in `cockroachdb.py` (lines ~1247-1250)

**Impact:**
- **Before:** 100 DELETEs → 1 row after merge → 10,450 final rows (should be 9,950)
- **After:** 100 DELETEs → 100 rows after merge → 9,950 final rows ✅

**Lesson:** For JSON CDC, primary key extraction is **MANDATORY** and **SEPARATE** from data extraction. The `key` array is the source of truth for PK values, not the `after`/`before` structs!

**Commit:** `457d912` - "Fix JSON DELETE handling: Extract primary key from 'key' array"

---

### ❌ 18. Storing All CDC Events Without Deduplication (Initial Table Creation)
**Attempted:** Jan 8, 2026  
**Problem:** Initial table had 10,350 rows instead of 9,950 (400 extra UPDATE events)

**What we tried:**
```python
# WRONG - Keep ALL non-DELETE events as separate rows!
active_rows = df_all_events.filter(F.col("_cdc_operation") != "DELETE")
final_rows = active_rows.join(delete_keys, on=PK, how="left_anti")
final_rows.write.saveAsTable(...)  # 10,350 rows!

# Result:
#   user001: SNAPSHOT row @ T1  ┐
#   user001: UPDATE row @ T2    ├─ 2 rows for same key!
#   user002: SNAPSHOT row @ T1  │
#   user002: UPDATE row @ T2    ┘
```

**Why it failed:**
- Initial table creation should store **final state**, not CDC history
- We kept SNAPSHOT + UPDATE as separate rows
- Delta table had: 9,900 SNAPSHOT + 400 UPDATE + 50 INSERT = 10,350 rows
- Source had: 9,950 unique keys (final state)

**Correct approach:**
```python
# CORRECT - Deduplicate to keep only latest state per key
from pyspark.sql import Window

# After excluding DELETEs, keep only latest event per key
window_spec = Window.partitionBy(*PK).orderBy(F.col("timestamp").desc())
final_rows = (rows_after_delete
    .withColumn("_row_num", F.row_number().over(window_spec))
    .filter(F.col("_row_num") == 1)
    .drop("_row_num")
)

# Result:
#   user001: UPDATE row @ T2 only  ✅ (SNAPSHOT @ T1 discarded)
#   user002: UPDATE row @ T2 only  ✅ (SNAPSHOT @ T1 discarded)
```

**Output after fix:**
```
🔍 After excluding DELETEd keys: 10,350
🔍 After deduplication (latest per key): 9,950
🔍 Duplicate events removed: 400
📝 Creating initial table: 9,950 rows ✅
```

**Impact:**
- **Before:** 10,350 rows (stored SNAPSHOT + UPDATE separately)
- **After:** 9,950 rows (one row per key, latest state only)

**Key Insight:**
- **Initial table creation:** Store final state → deduplicate to latest per key
- **Incremental merges:** Use Delta MERGE to apply CDC events properly
- **CDC history:** Should be in a separate CDC log table, not the main table

**Lesson:** Initial table load should represent **current state**, not **event history**. Deduplication by PK is essential!

**Commit:** `7db8d2c` - "Fix initial table creation: Deduplicate to latest state per key"

---

### ❌ 19. Hardcoded Timestamp Fallbacks
**Attempted:** Jan 21, 2026  
**Problem:** Used stale hardcoded timestamps when directory listing failed

**What we tried:**
```python
# WRONG - Falls back to old timestamps!
if not timestamp_dirs:
    potential_timestamps = [1767823340, 1767823100, 1767822800, ...]
    for ts in potential_timestamps:
        try:
            dbutils.fs.ls(f"{path}/{ts}")
            return ts  # ❌ Uses OLD data silently
        except:
            pass
```

**Why it failed:**
- Masked real issues with directory listing
- Used stale data from previous test runs
- Created confusion about which data was being used
- Silent failures prevented debugging

**Correct approach:**
```python
# CORRECT - Fail with diagnostic information
if not timestamp_dirs:
    raise ValueError(
        f"No timestamped directories found in: {path}\n"
        f"Expected: 10-digit Unix timestamp directories\n"
        f"Found {len(items)} items:\n{debug_info}\n"
        f"Run: test_cdc_matrix.sh to generate test data"
    )
```

**Lesson:** Explicit failures with diagnostics are better than silent fallbacks to stale data.

**References:**
- HARDCODED_TIMESTAMP_FALLBACK_FIX.md
- cockroachdb.py lines 5097-5133

---

### ❌ 20. Relying Only on `item.name` for Directory Detection
**Attempted:** Jan 21, 2026  
**Problem:** `dbutils.fs.ls()` returned empty `name` attributes for directories

**What we tried:**
```python
# WRONG - Assumes name is always populated
dir_name = item.name.rstrip('/')
if dir_name.isdigit() and len(dir_name) == 10:
    timestamp_dirs.append(dir_name)
```

**Why it failed:**
- `item.name` can be empty or just `/` in some Databricks versions
- `item.path` is always correct but was ignored
- Result: "No timestamped directories found" even when they existed

**Correct approach:**
```python
# CORRECT - Fallback to path extraction
dir_name = item.name.rstrip('/') if item.name else ''
if not dir_name:
    # Extract from path: '/Volumes/.../1769022634/' -> '1769022634'
    path_parts = item.path.rstrip('/').split('/')
    dir_name = path_parts[-1]

if dir_name.isdigit() and len(dir_name) == 10:
    timestamp_dirs.append(dir_name)
```

**Lesson:** Databricks `FileInfo` objects have quirks - always have a fallback to extract from path.

**References:**
- EMPTY_NAME_BUG_FIX.md
- cockroachdb.py lines 5055-5078

---

### ❌ 21. Assuming Databricks Notebooks Cache Directory Listings
**Attempted:** Jan 21, 2026  
**Problem:** Assumed "cache issue" when directories weren't found

**What we tried:**
```python
# WRONG - Assumed caching was the problem
if len(items) > 50:
    warnings.warn(
        "Notebook cache issue! Restart kernel to clear stale directory cache."
    )
```

**Why it failed:**
- No evidence that Databricks caches `dbutils.fs.ls()` results
- Real issue was empty `item.name` attributes
- Added unnecessary complexity
- Confused users with incorrect advice

**Actual Root Cause:** `dbutils.fs.ls()` API quirk with empty `name` fields

**Lesson:** Test hypotheses thoroughly before adding detection/workaround code. The simplest explanation (API quirk) was correct.

**References:**
- CACHE_ASSUMPTION_REMOVAL.md
- EMPTY_NAME_BUG_FIX.md

---

### ❌ 22. Not Deduplicating Iterator Pattern Results
**Attempted:** Before Jan 21, 2026  
**Problem:** Iterator had 500 extra rows compared to Autoloader

**What we tried:**
```python
# WRONG - Only filtered DELETEs, didn't deduplicate
df_merged = merge_column_family_fragments(df_raw)
df_final = df_merged.filter("_cdc_operation != 'DELETE'")
# Result: 10,450 rows (includes both SNAPSHOT and UPDATE for same keys)
```

**Why it failed:**
- Kept separate rows for SNAPSHOT and UPDATE of same key
- Expected: 9,950 rows (one per key)
- Got: 10,450 rows (500 duplicate keys with both SNAPSHOT + UPDATE)

**Correct approach:**
```python
# CORRECT - Deduplicate by PK, keep latest
from pyspark.sql import Window
from pyspark.sql.functions import row_number, col

window_spec = Window.partitionBy(*primary_keys).orderBy(col('_cdc_timestamp').desc())
df_deduped = df_merged.withColumn("_row_num", row_number().over(window_spec)) \
                      .filter("_row_num == 1") \
                      .drop("_row_num")
df_final = df_deduped.filter("_cdc_operation != 'DELETE'")
# Result: 9,950 rows ✅
```

**Lesson:** Initial table state should represent **latest state per key**, not event history. Match Autoloader's deduplication logic exactly.

**References:**
- ITERATOR_DEDUPLICATION_FIX.md
- cockroachdb.py lines 1407-1456

---

### ❌ 23. Treating `_cdc_updated` as Data Column (Not Metadata)
**Attempted:** Before Jan 21, 2026  
**Problem:** `_cdc_updated` was dropped during column family merge

**What we tried:**
```python
# WRONG - Missing _cdc_updated from metadata_columns
metadata_columns = [
    '_cdc_operation', '_cdc_timestamp', '__crdb__updated',
    # ❌ Missing: '_cdc_updated'
]
```

**Why it failed:**
- `_cdc_updated` treated as data column
- `first()` aggregation picked random value instead of preserving it
- Deduplication couldn't use it as timestamp
- Result: `AnalysisException: Column '_cdc_updated' not found`

**Correct approach:**
```python
# CORRECT - Include _cdc_updated in metadata
metadata_columns = [
    '_cdc_operation', '_cdc_timestamp', '_cdc_updated',  # ✅ Added
    '__crdb__updated', ...
]
```

**Lesson:** CDC timestamp columns (`_cdc_updated`, `_cdc_timestamp`) are metadata, not data - preserve them during aggregations.

**References:**
- ITERATOR_DEDUPLICATION_FIX.md
- cockroachdb.py lines 5480-5603

---

### ❌ 24. Reading Directory Without Filtering .RESOLVED Files
**Attempted:** Before Jan 27, 2026  
**Problem:** `DECIMAL_PRECISION_EXCEEDS_MAX_PRECISION` error when reading changefeed directories

**What we tried:**
```python
# WRONG - Reads ALL files including .RESOLVED
raw_df = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "parquet")
    .load(source_path)  # Reads everything!
)
```

**Why it failed:**
- CockroachDB creates `.RESOLVED` files (Parquet format) to track CDC watermarks
- `.RESOLVED` files contain `resolved` column with `DECIMAL(2147483647, 0)` encoding
- Spark validates ALL Parquet files in directory, including `.RESOLVED`
- Result: `DECIMAL_PRECISION_EXCEEDS_MAX_PRECISION` error during schema validation

**Key Discovery (Jan 27, 2026):**
- **Data files** (`usertable-*.parquet`) → Use `StringType` for timestamps → ✅ No DECIMAL issue
- **`.RESOLVED` files** → Use `DECIMAL(2147483647, 0)` for watermarks → ❌ Exceeds Spark's max precision (38)

**Evidence:**
```
📁 2026-01-26: 1 data file + 138 .RESOLVED files
📁 2026-01-27: 0 data files + 94 .RESOLVED files

🧪 Test Results:
   ✅ usertable-*.parquet → Reads successfully
   ❌ *.RESOLVED → DECIMAL_PRECISION_EXCEEDS_MAX_PRECISION
```

**Correct approach for reading data:**
```python
# CORRECT - Filter out .RESOLVED files when reading CDC data
raw_df = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "parquet")
    .option("pathGlobFilter", "*usertable*.parquet")  # ← Excludes .RESOLVED!
    .load(source_path)
)
```

**Why this works:**
- `pathGlobFilter` only matches data files with "usertable" in name
- `.RESOLVED` files are excluded from CDC data processing
- Spark never encounters the problematic DECIMAL metadata
- No explicit schema needed!

**⚠️ IMPORTANT UPDATE (Feb 2, 2026): RESOLVED Files Should Be Used for Watermarking!**

**New Understanding:** While `.RESOLVED` files cannot be read as data (due to DECIMAL precision), they are **CRITICAL for data integrity** when using column families.

**The Problem RESOLVED Files Solve:**
- Tables with column families create **multiple Parquet files per UPDATE**
- These fragments arrive at different times → race condition
- Processing without watermarking → **data corruption** (incomplete updates)

**The Solution:**
```python
# 1. Read RESOLVED filename (not content) using Azure SDK to get watermark timestamp
watermark = get_resolved_watermark(config, source_table)

# 2. Filter CDC data by watermark
df = raw_df.filter(extract_wall_time("__crdb__updated") <= watermark)
```

**RESOLVED Timestamp Format:**
- **Filename format**: `202601292259386834839740000000000.RESOLVED` (33 digits)
  - 14 digits: `YYYYMMDDHHMMSS` (datetime)
  - 9 digits: Nanoseconds
  - 10 digits: Logical clock (HLC)
- **In CDC data (`__crdb__updated`)**: `'1770067697320026017.0000000002'` (decimal string)
  - Before decimal: Wall time (19 digit nanoseconds)
  - After decimal: Logical clock (10 digits)

**When to Use RESOLVED Watermarking:**
- ✅ **ALWAYS** for tables with column families (`split_column_families`)
- ✅ **RECOMMENDED** for multi-table consistency (transaction boundaries)
- ✅ **OPTIONAL** for single-CF tables (but adds guarantee of completeness)

**References:**
- `learnings/COCKROACHDB_HLC_TIMESTAMP_FORMAT.md` - Deep dive into HLC formats
- `learnings/RESOLVED_WATERMARKING_IMPLEMENTATION.md` - Implementation guide
- `learnings/COLUMN_FAMILY_COMPLETENESS_ANALYSIS.md` - Why RESOLVED is critical
- `docs/stream-changefeed-to-databricks-azure.md` - Production usage patterns
- DECIMAL_MYSTERY_SOLVED.md - Original DECIMAL discovery
- verify_decimal_issue.md - Test evidence

---

## 📖 Key Lessons Learned

### 1. Column Order Matters
Never assume alphabetical ordering - always use source system's natural order.
- **Impact:** Prevented 400 UPDATEs from being misclassified
- **Fix:** Removed `sorted()` call on primary keys

### 2. Test Data Isolation
Timestamps are essential for reproducible tests.
- **Impact:** Eliminated DELETE doubling, enabled validation mode
- **Fix:** Added `TEST_RUN_TIMESTAMP` to path structure

### 3. Explicit > Implicit
Classes with named attributes beat tuple unpacking.
- **Impact:** Eliminated path parsing bugs, improved readability
- **Fix:** Created `VolumePathComponents` class

### 4. Fast Feedback Loops
Validation mode enables rapid development cycles.
- **Impact:** 60× speed improvement (30 min → 30 sec)
- **Fix:** Implemented `--validate-only` mode

### 5. Backward Compatibility
Support legacy formats during migration.
- **Impact:** No breaking changes, smooth adoption
- **Fix:** Added fallback logic for non-timestamped paths

### 6. Delete Operation Handling
Streaming complete mode requires explicit DELETE filtering.
- **Impact:** Perfect row count matches
- **Fix:** Filter DELETEs before writing to Delta

### 7. Schema Files Matter
Auto-generating schema files eliminates warnings and speeds up loads.
- **Impact:** Cleaner logs, faster processing
- **Fix:** Added `create-schema-file` command to helper

### 8. Use Top-Level Key Field
Always use the 'key' field for primary key extraction in JSON.
- **Impact:** Correct UPDATE detection with split column families
- **Fix:** Changed from `after`/`before` to `event_data['key']`

### 9. Spark's F.to_json() Returns SQL NULL (Not String "null")
Always check `.isNull()` in addition to string comparisons.
- **Impact:** Fixed 9,651 misclassified SNAPSHOT rows
- **Fix:** Added `.isNull()` check for SQL NULL case
- **Key Insight:** Local testing (Python) revealed logic was correct; debug columns revealed Spark-specific behavior

### 10. JSON Primary Key Extraction is MANDATORY
For JSON CDC format, primary keys must be extracted from the `key` array before merge.
- **Impact:** Fixed 99 lost DELETE events (100 DELETEs → 1 DELETE after merge)
- **Root Cause:** All DELETEs had NULL `ycsb_key`, so `groupBy(ycsb_key, timestamp, operation)` merged them into 1 row
- **Fix:** Added `F.col("key").getItem(i)` extraction for each primary key column
- **Code Location:** `_add_cdc_metadata_to_dataframe()` in `cockroachdb.py` (lines ~1242-1247)
- **Key Insight:** Diagnostic showed "Unique (key + updated + operation): 1" for 100 DELETEs → all had same NULL key!
- **Lesson:** Always validate that PK columns exist and are populated before calling `merge_column_family_fragments`

### 11. Parquet Format is Pre-Optimized (No Fragmentation)
**Discovery (Jan 21, 2026):** Parquet files with `split_column_families=true` do NOT fragment like JSON.
- **Impact:** JSON requires 2.0x merge (20,700 → 10,550), Parquet is already 1.0x (9,950 → 9,950)
- **Root Cause:** CockroachDB's Parquet writer merges column families internally
- **Key Insight:** Auto-detect fragmentation ratio before applying merge logic
- **Benefit:** Parquet processing is faster (no merge needed)
- **Lesson:** Test both formats separately - they have different characteristics

### 12. Explicit Failures Better Than Silent Fallbacks
**Discovery (Jan 21, 2026):** Hardcoded timestamp fallbacks masked real issues.
- **Impact:** Removed fallback that used stale data from Jan 6 instead of Jan 21
- **Root Cause:** Fallback hid the real problem (empty `item.name` attributes)
- **Lesson:** Clear diagnostic errors > silent fallbacks to potentially wrong data

### 13. Databricks `FileInfo` Has Quirks
**Discovery (Jan 21, 2026):** `item.name` can be empty even when `item.path` is correct.
- **Impact:** Timestamp directories not found even though they existed
- **Root Cause:** Different Databricks runtime versions handle `name` differently
- **Fix:** Always fallback to extracting from `item.path` when `item.name` is empty
- **Lesson:** Don't trust single attributes in distributed file systems - have fallbacks

### 14. Metadata Belongs in Separate Directory
**Discovery (Jan 21, 2026):** Using filename prefixes (`_schema.json`) is brittle.
- **Impact:** Reduced filtering code from 16 lines to 3 lines
- **Root Cause:** Checking filename prefixes requires basename extraction
- **Fix:** Use `_metadata/` directory, check path instead of filename
- **Lesson:** Directory structure > filename conventions for metadata separation

### 15. Iterator Must Match Autoloader Deduplication Exactly
**Discovery (Jan 21, 2026):** Iterator had 500 extra rows due to missing deduplication.
- **Impact:** Iterator now produces identical results to Autoloader (9,950 rows)
- **Root Cause:** Kept both SNAPSHOT and UPDATE rows for same keys
- **Fix:** Deduplicate by PK only, keep latest by timestamp, filter DELETEs
- **Lesson:** Different consumption patterns must produce identical final state

### 16. Row Count Matching is Necessary But Not Sufficient
**Discovery (Jan 29, 2026):** Row count matched (28 = 28) but data was incomplete.
- **Impact:** Multi-column sum verification detected ~3% data loss in fields 3-9
- **Root Cause:** Missing UPDATE events for specific column family fragments
- **Manifestation:** 
  - Basic stats matched: min key ✅, max key ✅, count ✅, primary key sum ✅
  - Field3-9 sums mismatched: -723 to -729 differences per column ❌
- **Fix:** Added `get_column_sum()` and `get_column_sum_spark()` for comprehensive validation
- **Key Insight:** Always verify ALL data columns, not just row counts and primary keys
- **Lesson:** Deep data integrity requires multi-column sum verification, especially for:
  - Multi-column family tables (fragmentation risk)
  - UPDATE-heavy workloads (partial update detection)
  - MERGE operations (completeness verification)
  - Production monitoring (continuous validation)

### 17. Column Family Columns CAN Be NULL (Verified from CockroachDB Source)
**Discovery (Jan 29, 2026):** Columns in column families do NOT need to be NOT NULL.

**Question:** "With column family, do the columns have to be defined as NOT NULL?"  
**Answer:** **NO** - Verified from CockroachDB source code.

**Evidence from CockroachDB Source Code:**

From `/Users/robert.lee/github/cockroach/pkg/ccl/changefeedccl/changefeed_test.go` (lines 3870-3877):
```go
// No messages on insert for families where no non-null values were set.
sqlDB.Exec(t, `INSERT INTO foo values (1, 'puppy', null)`)
sqlDB.Exec(t, `INSERT INTO foo values (2, null, 'kitten')`)
assertPayloads(t, foo, []string{
    `foo.most: [1]->{"after": {"a": 1, "b": "puppy"}}`,
    `foo.most: [2]->{"after": {"a": 2, "b": null}}`,  // ← b is NULL!
    `foo.only_c: [2]->{"after": {"c": "kitten"}}`,
})
```

**How NULL Works with `split_column_families`:**

1. **Column Family WITH Primary Key** (e.g., `FAMILY most (a, b)`):
   - ✅ ALWAYS emits CDC event (PK must be present)
   - ✅ Data columns CAN be NULL (e.g., `{"a": 2, "b": null}`)
   - The event includes ALL columns in the family, even if NULL

2. **Column Family WITHOUT Primary Key** (e.g., `FAMILY only_c (c)`):
   - ⚠️  NO CDC event if ALL columns are NULL (optimization)
   - ✅ Emits event if ANY column has non-NULL value

**Why Our Coalescing Fix is Correct:**

Our `F.last(col, ignorenulls=True)` approach handles all cases correctly:

1. **Column family not updated** → NULL in new fragment (family not emitted) → Coalesce keeps old value ✅
2. **Column truly NULL** → NULL in all fragments → Coalesce returns NULL ✅
3. **Explicit UPDATE to NULL** → Fragment emits ENTIRE family with NULL → Coalesce correctly uses latest NULL ✅

**Key Insight:** When CockroachDB updates ANY column in a family, it emits the **ENTIRE family** (all columns), so explicit NULLs are always in complete fragments!

**Storage Optimization:**

From CockroachDB storage tests:
```
put      k=/row1/4 v=r1e # column family 2-3 omitted (i.e. if all NULLs)
```

CockroachDB optimizes storage by omitting column families that are entirely NULL. This is an internal storage optimization and does NOT affect CDC behavior.

**Impact on Our Implementation:**
- ✅ Column-level coalescing works correctly for NULL values
- ✅ Handles mixed NULL/non-NULL columns in same family
- ✅ Correctly preserves explicit NULL updates
- ✅ No special handling needed for NULL vs. non-NULL columns

**References:**
- CockroachDB Source: `/Users/robert.lee/github/cockroach/pkg/ccl/changefeedccl/changefeed_test.go` (TestChangefeedEachColumnFamily)
- Fix Implementation: `cockroachdb-cdc-tutorial.ipynb` Cell 7 (column-level coalescing)
- Doc: `COLUMN_FAMILY_NULL_BEHAVIOR.md`

---

## 🎉 Summary

### 🎯 Major Milestones Achieved (Jan 21, 2026)

**Step 2 Complete: One-Time Load to Delta - 100%**
- ✅ Parquet CDC processing - Working perfectly
- ✅ JSON CDC processing - **FIXED AND VALIDATED!**
- ✅ Perfect match: Delta 9,950 rows = Source 9,950 rows
- ✅ All DELETE events preserved (100/100)
- ✅ All duplicate UPDATE events removed (400/400)

**Step 3 Complete: Incremental Load - 100%**
- ✅ Incremental mode testing implemented
- ✅ Autoloader checkpoints working perfectly
- ✅ Delta MERGE applies incremental changes correctly
- ✅ Only new CDC events processed (no reprocessing)
- ✅ All CDC operations supported (INSERT/UPDATE/DELETE)

**Step 5 Complete: Community Connector Iterator Pattern - 100%**
- ✅ JSON file support with column family fragmentation (2.0x merge ratio)
- ✅ Parquet file support (no fragmentation - 1.0x optimized!)  
- ✅ Format-agnostic deduplication logic
- ✅ Perfect row count matching: Iterator = Autoloader = 9,950 rows
- ✅ Recursive directory reading (supports date-based partitions)
- ✅ File-based modes work without CockroachDB credentials
- ✅ Metadata directory refactoring (`_metadata/schema.json`)
- ✅ Robust timestamp resolution (handles Databricks API quirks)
- ✅ Shared CDC processing with Autoloader (55% code reuse)
- ✅ Cursor-based progress tracking
- ✅ Memory-efficient for low-volume workloads

### What We Built
- ✅ Three patterns: Iterator, Autoloader, DLT
- ✅ 55% code reuse across patterns
- ✅ Zero CDC logic duplication
- ✅ 8/8 test scenarios passing
- ✅ 60× faster validation mode
- ✅ Automatic schema file generation
- ✅ Flexible path handling (legacy + timestamped)
- ✅ Complete documentation (40+ files)
- ✅ **JSON format support with full CDC operations**
- ✅ **Initial table deduplication to latest state**

### Key Features
- ✅ **RESOLVED timestamp watermarking** (guarantees column family completeness & multi-table consistency)
- ✅ **Databricks Serverless compatibility** (cluster-agnostic code using Azure SDK)
- ✅ Column family fragment merging (auto-detects fragmentation)
- ✅ Format-agnostic processing (JSON with 2.0x merge, Parquet with 1.0x)
- ✅ DELETE operation handling (initial + incremental)
- ✅ Accurate operation classification (SNAPSHOT/INSERT/UPDATE/DELETE)
- ✅ Timestamp-based CDC detection
- ✅ Primary key management (Parquet + JSON)
- ✅ **JSON primary key extraction from `key` array**
- ✅ **Window-based deduplication (Iterator = Autoloader)**
- ✅ **Recursive directory reading (date-based partitions)**
- ✅ **File-based modes work without CockroachDB**
- ✅ **HLC timestamp parsing** (33-digit filename + decimal string data formats)
- ✅ **Metadata directory separation (`_metadata/`)**
- ✅ **Robust timestamp resolution (handles API quirks)**
- ✅ Backward compatibility
- ✅ Unity Catalog integration
- ✅ Spark Connect support

### Critical Fixes Delivered (Jan 21, 2026)
1. **Format-Agnostic Deduplication** - Iterator matches Autoloader (9,950 rows)
2. **Recursive Directory Support** - Handles date-based file organization
3. **File-Based Mode Credentials** - VOLUME/AZURE work without CockroachDB
4. **Empty `item.name` Fix** - Handles Databricks API quirks
5. **Metadata Directory Refactor** - Simpler filtering (16 lines → 3 lines)
6. **Hardcoded Timestamp Removal** - Clear errors > silent fallbacks

### Critical Fixes Delivered (Jan 8, 2026)
1. **JSON Primary Key Extraction** - Fixed 99 lost DELETEs (Commit: `457d912`)
2. **Initial Table Deduplication** - Fixed 400 duplicate UPDATEs (Commit: `7db8d2c`)
3. **Result:** Perfect row count match with source data

### Next Steps
1. ✅ **Step 1:** CDC Generation - **COMPLETE!**
2. ✅ **Step 2:** One-Time Load - **COMPLETE!**
3. ✅ **Step 3:** Incremental Load - **COMPLETE!**
4. ✅ **Step 5:** Community Connector - **COMPLETE!**
   - ✅ JSON/Parquet format support
   - ✅ Format-agnostic deduplication
   - ✅ Recursive directory reading
   - ✅ File-based modes (no CockroachDB needed)
5. **Step 4:** DLT + Autoloader (streaming pipelines) - **NEXT PRIORITY**
6. Apply deduplication to Azure iterator methods (`_read_table_from_azure_parquet/json`)
7. Add CI/CD integration with validation mode
8. Create performance benchmarking suite
9. Add S3/ABFSS support (currently Azure-only)
10. Implement continuous streaming with foreachBatch

**Status: ✅ STEPS 1, 2, 3, & 5 COMPLETE (80%) - FILE-BASED CDC PROOF OF CONCEPT**

---

*Last updated: January 21, 2026*  
*Version: 2.3 - Iterator Pattern (Format-Agnostic)*

---

## 🎯 Major Milestone: Dual Storage Support (February 2026)

### Step 6 Complete: Unity Catalog External Volume Support - 100%

**Achievement: Configuration-Driven Dual Storage Architecture**

✅ **Unified Storage Abstraction Layer**
- Created `cockroachdb_storage.py` with overlay functions
- Single API works with both Azure Blob Storage and UC External Volumes
- Configuration-based mode switching (no code changes needed)
- Automatic backend selection based on `data_source` field

✅ **Configuration System Enhanced**
- Added `UCVolumeConfig` dataclass for volume configuration
- Added `data_source` field (`"azure_storage"` or `"uc_external_volume"`)
- Updated all ingestion functions to use config objects
- Helper functions: `get_storage_path()`, `get_volume_path()`

✅ **Performance Optimizations**
- Spark-based parallel file listing (5-10x faster than dbutils)
- Automatic fallback to dbutils for reliability
- Enhanced logging for troubleshooting hangs
- Performance: UC Volume (Spark) ~3s vs dbutils ~15s for 100 files

✅ **Notebooks Updated**
- Both notebooks use unified `check_files()` and `wait_for_files()` APIs
- All ingestion functions updated to config-based signatures
- Fixed connection management bug (bare except block)
- Added data source display in configuration output

✅ **Infrastructure**
- Azure setup script creates UC External Volume automatically
- Volume creation integrated into existing workflow
- Consistent naming with timestamp suffixes
- All metadata saved to JSON configuration

### Key Learnings & Patterns

#### 1. Overlay Function Pattern

**Problem:** Multiple storage backends (Azure, UC Volume) with same operations

**Solution:** Unified abstraction layer with automatic routing
```python
def check_files(config, spark, dbutils):
    if config.data_source == "azure_storage":
        return cockroachdb_azure.check_azure_files(...)
    elif config.data_source == "uc_external_volume":
        return cockroachdb_uc_volume.check_volume_files(...)
```

**Benefits:**
- Single API for all storage modes
- Configuration-driven (no code changes to switch)
- Consistent return structure
- Easy to add new storage providers

#### 2. Config-Based Function Signatures

**Before:**
```python
ingest_cdc_append_only_multi_family(
    storage_account_name, container_name, source_catalog, 
    source_schema, source_table, target_catalog, 
    target_schema, target_table, primary_key_columns, spark
)  # 10+ parameters!
```

**After:**
```python
ingest_cdc_append_only_multi_family(
    config, spark, dbutils
)  # 3 parameters!
```

**Benefits:**
- Cleaner function signatures
- Type-safe with config objects
- Easier to add new parameters (just extend config)
- Backward compatible with configuration evolution

#### 3. Performance: Spark vs dbutils for File Listing

**Discovery:** UC Volume file listing was 5-10x slower than Azure

**Root Cause:**
- dbutils.fs.ls() requires one API call per directory (recursive)
- Goes through: Databricks → Unity Catalog → Azure (3 layers)
- No server-side filtering

**Solution:** Spark-based parallel listing
```python
# Fast: Parallelized across Spark executors
file_df = spark.read.format("binaryFile") \
    .option("recursiveFileLookup", "true") \
    .load(volume_path)
```

**Results:**
- Azure SDK: ~1s for 100 files
- UC Volume (dbutils): ~15s for 100 files  
- UC Volume (Spark): ~3s for 100 files ⚡

**Pattern:** Always prefer Spark for distributed file operations over dbutils

#### 4. Connection Management in Notebooks

**Bug Found:** Bare `except:` block closing connection
```python
try:
    run_ycsb_workload(conn=conn, ...)
except:
    conn.close()  # ❌ BAD: Closes notebook-managed connection!
```

**Pattern:** Connection lifecycle rules
- **Notebooks:** Create once, use throughout, optionally close at end
- **Functions:** Use provided connection, NEVER close it (caller manages)
- **Scripts:** Create, use, close in finally block

**Fix:** Removed exception handler - let notebook manage connection

#### 5. Progressive Enhancement with Fallbacks

**Pattern:** Try fast method, fall back to reliable method
```python
if use_spark:
    try:
        # Fast Spark method (5-10x faster)
        files = spark_list_files()
    except:
        # Fall back to reliable dbutils
        files = dbutils_list_files()
else:
    files = dbutils_list_files()
```

**Benefits:**
- Best performance when possible
- Reliability when performance fails
- User can force fallback if needed

#### 6. Detailed Progress Logging for Long Operations

**Problem:** UC Volume file listing appeared to hang

**Solution:** Show exactly what's happening
```python
print(f"   🔍 Scanning for .RESOLVED files...")
print(f"   📂 Volume path: {volume_path}")
print(f"   ⏳ Listing files (may take 5-10 seconds)...")

if use_spark:
    print(f"   🚀 Using Spark...")
    print(f"   ⏳ Spark: Reading directory structure...")
    print(f"   ⏳ Spark: Collecting file paths...")
    print(f"   ✅ Spark: Found {len(files)} paths")
else:
    print(f"   🐢 Using dbutils (slower)...")
    print(f"   ⏳ dbutils: Listing root directory...")

print(f"   ✅ File listing completed in {elapsed:.1f}s")
```

**Benefits:**
- User knows exactly where code is
- Can distinguish our code from Databricks code
- Helps troubleshoot actual hangs vs slow operations
- Sets expectations (e.g., "may take 5-10 seconds")

### Documentation Structure

#### Current Operational Documentation (`docs/`)

User-facing documentation for current features and operations:

1. **`docs/STORAGE_PROVIDERS.md`** - Storage provider selection and configuration
   - Comparison: Azure vs UC Volume
   - Configuration guide for each provider
   - Unified interface and examples
   - When to choose each option
   
2. **`docs/TROUBLESHOOTING_HANG.md`** - Operational troubleshooting guide
   - Debugging slow file operations
   - Performance expectations
   - Common issues and solutions
   
3. **`docs/UC_VOLUME_AUTOLOADER.md`** - Auto Loader configuration guide
   - Checkpoint behavior and management
   - Schema discovery modes
   - Best practices for UC Volumes
   
4. **`docs/UC_VOLUME_CREDENTIALS.md`** - Credential requirements explained
   - When credentials are needed (changefeed creation vs Auto Loader)
   - Tutorial/demo vs production scenarios
   - Configuration examples

5. **`docs/stream-changefeed-to-databricks-azure.md`** - Complete tutorial guide
   - End-to-end setup and configuration
   - Architecture diagrams
   - Best practices and examples

#### Historical Learnings Documentation (`docs/learnings/`)

Design decisions, bug fixes, and evolution history:

1. **`docs/learnings/PERFORMANCE_UC_VOLUME.md`** - Performance analysis
   - Spark vs dbutils benchmarks (5-10x improvement)
   - Architectural comparison
   - Optimization strategies
   
2. **`docs/learnings/DUAL_STORAGE_SUPPORT.md`** - Storage abstraction patterns
   - Design pattern evolution
   - Provider interface standardization
   - Migration patterns
   
3. **`docs/learnings/CONNECTION_ERROR_FIX.md`** - AttributeError bug fix
   - Root cause: Hardcoded Azure paths
   - Solution: Config-driven path resolution
   - UC Volume sink URI handling

4. **`docs/learnings/MIGRATION_GUIDE.md`** - Historical migration guide
   - Originally framed as Azure → UC Volume migration
   - Moved to learnings: New projects choose providers, don't migrate
   - Configuration examples preserved for reference
   
5. **`docs/learnings/README.md`** - Learnings directory index
   - Purpose and organization
   - Links to related documentation
   - Historical documents

See `docs/learnings/` for complete archive of bug fixes, design decisions, and evolution history.

### Architecture Evolution

**Before (Azure Only):**
```
Notebook → cockroachdb_azure → Azure Blob Storage
```

**After (Dual Storage):**
```
Notebook → cockroachdb_storage (overlay) → {
    Azure: cockroachdb_azure → Azure Blob Storage
    UC Volume: cockroachdb_uc_volume → Unity Catalog → Azure
}
```

**Future (Extensible):**
```
Notebook → cockroachdb_storage (overlay) → {
    Azure, UC Volume, AWS S3, GCP GCS, Cloudflare R2, ...
}
```

### Breaking Changes & Migrations

**API Changes:**
- All ingestion functions now use config objects (simplified from 10+ params to 3)
- Added `dbutils` parameter (required for UC Volume, optional for Azure)
- Storage path resolution now automatic (no manual path construction)

**Migration Path:**
1. Update config JSON (add `data_source` field)
2. Add `uc_external_volume` section if using UC Volume
3. Pass `dbutils` to function calls
4. Update notebooks (already done)

**Backward Compatibility:**
- Existing Azure-only configs still work (default: `data_source: "azure_storage"`)
- Function calls with old signature no longer supported (intentional simplification)

### Production Deployment Checklist

For Azure Blob Storage:
- ✅ Config has `data_source: "azure_storage"`
- ✅ Config has `azure_storage` section
- ✅ Storage account accessible
- ✅ Functions receive `config`, `spark` (dbutils optional)

For UC External Volume:
- ✅ Config has `data_source: "uc_external_volume"`
- ✅ Config has `uc_external_volume` section  
- ✅ Config has `azure_storage` section (for changefeed URI)
- ✅ UC Volume created and accessible
- ✅ Functions receive `config`, `spark`, `dbutils` (required)

### Key Learnings from Storage Abstraction Refactoring (Feb 2026)

**Background:** Removed `dbutils` dependency and fallback logic from all CDC modules, streamlined to Spark-only file operations for Unity Catalog Volumes.

#### 1. **Dependency Elimination**
**Learning:** Remove optional dependencies completely rather than maintaining fallback logic.
- ❌ **Before:** `dbutils` as optional parameter with complex fallback chains
- ✅ **After:** Spark-only for UC Volume operations, no fallbacks
- **Impact:** Simpler code, fewer edge cases, clearer error messages
- **Tradeoff:** Requires Spark to be available, but this was already required

#### 2. **Performance Through Simplification**
**Learning:** Spark parallelization outperforms sequential API calls by 5-10x.
- UC Volume with `dbutils.fs.ls()`: ~15-20s for 100 files (recursive calls)
- UC Volume with Spark `binaryFile`: ~2-3s for 100 files (parallel)
- **Key:** Leverage built-in parallelism instead of building custom solutions
- **See:** `docs/learnings/PERFORMANCE_UC_VOLUME.md` for detailed analysis

#### 3. **Fail Fast Philosophy**
**Learning:** Explicit failures are better than silent fallbacks.
- ❌ **Before:** Silently caught exceptions, returned empty results
- ✅ **After:** `AnalysisException` (empty dir) → empty results, all other exceptions → `RuntimeError`
- **Impact:** Faster debugging, no hidden errors, clearer intentions
- **Example:** "Failed to list files in Unity Catalog Volume path" instead of silent empty results

#### 4. **API Surface Reduction**
**Learning:** Fewer parameters = fewer bugs and easier maintenance.
- ❌ **Before:** `check_files(config, spark, dbutils, verbose, use_spark_listing)`
- ✅ **After:** `check_files(config, spark, verbose)`
- **Removed:** `use_spark_listing` (no longer needed without fallback)
- **Removed:** `dbutils` parameter (not used)
- **Impact:** 40% parameter reduction across all functions

#### 5. **Module-by-Module Cleanup Strategy**
**Learning:** Systematic refactoring prevents regressions.
- **Approach:** One module at a time: `autoload` → `sql` → `storage` → `uc_volume`
- **Verification:** `grep dbutils` after each module to check propagation
- **Impact:** No breaking changes to notebooks, zero regressions
- **Key:** Update callers and callees together

#### 6. **File Object Standardization**
**Learning:** Consistent data structures eliminate conditional logic.
- ❌ **Before:** Azure returned objects with `.name`, UC returned dicts with `['name']`
- ✅ **After:** Both return standardized `{'name': str, 'path': str, 'size': int}`
- **Impact:** Removed all `hasattr()` checks, unified processing code
- **Pattern:** Define data contracts early, enforce across providers

#### 7. **Transaction Management**
**Learning:** Explicitly close read transactions to avoid serialization conflicts.
- **Issue:** Uncommitted `SELECT` statements held transactions open
- **Fix:** Add `conn.commit()` after all read-only `conn.run()` calls
- **Impact:** Eliminated spurious SQLSTATE 40001 retry errors
- **Lesson:** Even read transactions need explicit closure in serializable isolation

#### 8. **Retry Logic Specificity**
**Learning:** Only retry on specific retryable errors, fail fast on others.
- ❌ **Before:** Broad exception catching
- ✅ **After:** Check `SQLSTATE 40001` specifically, fail on all other errors
- **Pattern:** `if error_code == '40001': retry() else: raise`
- **Impact:** Faster failure on real errors, no masking of bugs

#### 9. **Documentation as Contracts**
**Learning:** Docstrings should be updated with code changes, not after.
- **Approach:** Update signature → update docstring → update examples
- **Impact:** Zero documentation drift, accurate examples
- **Moved:** Historical learnings to `docs/learnings/` for reference

#### 10. **Connector Framework Separation**
**Learning:** Separate experimental frameworks from production CDC code.
- **Decision:** Removed `connector.py` (7,100 lines) - experimental LakeflowConnect framework
- **Reason:** Different design patterns, different requirements, different users
- **Impact:** Cleaner core CDC modules, focused scope
- **Lesson:** Don't let experimental code pollute production paths

#### Quantified Improvements

| Metric | Before | After | Improvement |
|--------|--------|-------|-------------|
| UC Volume file listing | 15-20s | 2-3s | **5-10x faster** |
| Function parameters | 5-6 avg | 3 avg | **40% reduction** |
| `dbutils` references | 150+ | 0 | **100% removed** |
| Error clarity | Silent fails | Explicit exceptions | **Immediate debugging** |
| Code complexity | Fallback chains | Single path | **Simpler logic** |
| Modules cleaned | 0 | 7 | **Complete coverage** |

#### Reference Documentation

Detailed learnings moved to `docs/learnings/`:
- **`PERFORMANCE_UC_VOLUME.md`** - Performance analysis: Spark vs dbutils vs Azure
- **`DUAL_STORAGE_SUPPORT.md`** - Storage abstraction patterns and migration guide
- **`CONNECTION_ERROR_FIX.md`** - AttributeError bug fix for UC Volume path resolution

#### Architecture Pattern: Storage Provider Abstraction

**Pattern Established:**
```python
# Overlay function (cockroachdb_storage.py)
def check_files(config, spark, verbose):
    if config.data_source == "azure_storage":
        return cockroachdb_azure.check_azure_files(...)
    elif config.data_source == "uc_external_volume":
        return cockroachdb_uc_volume.check_volume_files(...)
    else:
        raise ValueError(f"Unknown data source: {config.data_source}")
```

**Benefits:**
- ✅ Config-driven provider selection
- ✅ Uniform API across all providers
- ✅ Easy to add new providers (S3, GCS, R2)
- ✅ Provider-specific optimizations hidden
- ✅ Single integration point for notebooks

**Future Extensions:**
```python
# Future providers follow same pattern
elif config.data_source == "aws_s3":
    return cockroachdb_s3.check_s3_files(...)
elif config.data_source == "gcp_gcs":
    return cockroachdb_gcs.check_gcs_files(...)
```

---

### Next Steps

1. ✅ **Step 6:** Dual Storage Support - **COMPLETE!**
   - ✅ Unity Catalog External Volume support
   - ✅ Overlay function pattern
   - ✅ Configuration-driven architecture
   - ✅ Performance optimizations
   - ✅ Comprehensive documentation
   - ✅ `dbutils` dependency removed
   - ✅ Spark-only file operations

2. **Future Extensions:**
   - Add AWS S3 support (same overlay pattern)
   - Add GCP GCS support
   - Add Cloudflare R2 support
   - Direct DBFS support (without UC Volume)

3. **Performance Enhancements:**
   - Cache file listings for repeated checks
   - Parallel RESOLVED file scanning
   - Optimize for very large file counts (10,000+)

**Status: ✅ STEPS 1, 2, 3, 5, & 6 COMPLETE (90%) - DUAL STORAGE PROOF OF CONCEPT**

### Key Metrics

- **Files Modified:** 12 (config, autoload, storage, uc_volume, sql, testloop, notebooks, examples)
- **Files Deleted:** 1 (connector.py - 7,100 lines of experimental code)
- **Files Created:** 9 (documentation and summaries)
- **Lines of Code:** ~1,500 new, ~500 modified, ~7,600 removed
- **Documentation:** ~3,500 lines (including learnings)
- **API Simplification:** 10+ params → 3 params
- **Parameter Reduction:** 40% fewer parameters across all functions
- **Performance Gain:** 5-10x for UC Volume file listing (Spark vs dbutils)
- **Storage Modes:** 2 (Azure, UC Volume) with extensible pattern for more
- **Dependency Cleanup:** 100% `dbutils` removal from core CDC modules
- **Code Complexity:** Eliminated all fallback chains, single execution path

---

*Last updated: February 4, 2026*  
*Version: 3.1 - Streamlined Storage Architecture with Spark-only Operations*
