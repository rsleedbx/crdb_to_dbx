# Unity Catalog Volumes and CockroachDB Changefeeds: Credential Requirements

*Updated: February 4, 2026*

## Quick Answer

**Do I need Azure credentials?**

| Scenario | Azure Credentials Needed? | Why? |
|----------|--------------------------|------|
| **Tutorial/Demo** (creating changefeeds) | ✅ Yes | CockroachDB's `CREATE CHANGEFEED` requires `azure://` protocol |
| **Production** (Auto Loader only) | ❌ No | Unity Catalog handles all authentication automatically |

**Key Point:** Azure credentials are for **CockroachDB changefeed creation**, not for **Databricks Auto Loader reading**. If changefeeds are already created, you need zero credentials.

## Critical Understanding

**Azure credentials are required for changefeed creation (tutorial/demo operations), but NOT required for Databricks Auto Loader when using Unity Catalog Volumes.**

## When Credentials Are Needed

### The Architecture

```
┌─────────────────────────────────────────────────────────────┐
│  Tutorial/Demo: CREATE CHANGEFEED                           │
│  (Requires Azure credentials in config)                     │
└──────────────┬──────────────────────────────────────────────┘
               │
               ↓
CockroachDB Changefeed (WRITE)
         ↓
    azure:// protocol
    + Azure credentials
         ↓
Azure Blob Storage
(actual cloud storage)
         ↑
    Unity Catalog Volume
    (managed access layer)
         ↑
Databricks Auto Loader (READ)
    NO credentials needed
    UC manages access
```

### Two Different Use Cases

#### 1. **Changefeed Creation (Tutorial/Demo Operations)**
   - **Operation:** Running `CREATE CHANGEFEED` commands
   - **Protocol:** `azure://` (CockroachDB requirement)
   - **Requires:** Azure credentials (account name + key)
   - **Why:** CockroachDB needs to write directly to Azure Blob Storage
   - **User Action:** Include `azure_storage` section in config
   - **Used By:** `create_changefeed_from_config()` function

#### 2. **Auto Loader Ingestion (Production Reading)**
   - **Operation:** Reading CDC files with Auto Loader
   - **Path:** `/Volumes/catalog/schema/volume`
   - **Requires:** NO credentials (Unity Catalog manages access)
   - **Why:** UC provides governed, credential-free access
   - **User Action:** Only need UC Volume permissions
   - **Used By:** `ingest_cdc_*()` functions

## Configuration Requirements

### For Tutorial/Demo (With Changefeed Creation)

If you're creating changefeeds (tutorial/demo), include **BOTH** sections:

```json
{
  "data_source": "uc_external_volume",
  
  "uc_volume": {
    "volume_catalog": "robert_lee",
    "volume_schema": "robert_lee_cockroachdb",
    "volume_name": "cockroachdb_cdc_1768934658",
    "volume_full_path": "robert_lee.robert_lee_cockroachdb.cockroachdb_cdc_1768934658"
  },
  
  "azure_storage": {
    "account_name": "cockroachcdc1768934658",
    "account_key": "your-azure-key-here",
    "container_name": "changefeed-events"
  }
}
```

**Why both sections:**
- `uc_volume`: For Auto Loader to READ (no credentials needed by user)
- `azure_storage`: For `CREATE CHANGEFEED` command (CockroachDB writes to azure://)

### For Production (Auto Loader Only)

If changefeeds are already created and you only need to read:

```json
{
  "data_source": "uc_external_volume",
  
  "uc_volume": {
    "volume_catalog": "robert_lee",
    "volume_schema": "robert_lee_cockroachdb",
    "volume_name": "cockroachdb_cdc_1768934658",
    "volume_full_path": "robert_lee.robert_lee_cockroachdb.cockroachdb_cdc_1768934658"
  }
}
```

**Only `uc_volume` needed** - Auto Loader reads via Unity Catalog (no Azure credentials required)

## What Happens

### Scenario 1: Tutorial/Demo (Creating Changefeeds)

**Step 1: Create Changefeed**
```python
# Requires azure_storage in config
create_changefeed_from_config(conn, config, spark)
```

**Behind the scenes:**
```python
# Reads azure_storage from config
container = config.azure_storage.container_name  # "changefeed-events"
account_name = config.azure_storage.account_name  # "cockroachcdc1768934658"
account_key = config.azure_storage.account_key    # User's Azure key

# Builds CockroachDB changefeed URI (uses azure://, NOT abfss://)
changefeed_uri = (
    "azure://changefeed-events/parquet/defaultdb/public/usertable/target"
    "?AZURE_ACCOUNT_NAME=cockroachcdc1768934658"
    "&AZURE_ACCOUNT_KEY=<encoded-key>"
)

# Creates changefeed in CockroachDB
conn.execute(f"CREATE CHANGEFEED FOR TABLE ... INTO '{changefeed_uri}'")
```

**Step 2: CockroachDB Writes Data**
- Protocol: `azure://`
- Destination: Azure Blob Storage container
- Authentication: Azure account key (from config)
- **User provided credentials to CockroachDB**

**Step 3: Auto Loader Reads Data**
```python
# NO azure_storage needed for reading
ingest_cdc_with_merge_multi_family(config, spark)
```

- Path: `/Volumes/robert_lee/robert_lee_cockroachdb/cockroachdb_cdc_1768934658/`
- Authentication: Unity Catalog (automatic)
- Source: Same Azure Blob Storage container
- **User provides NO credentials - UC handles it**

### Scenario 2: Production (Changefeeds Already Created)

**Only Step 3 is needed:**
```python
# Config only needs uc_volume section
ingest_cdc_with_merge_multi_family(config, spark)
```

- No `create_changefeed_from_config()` call
- No `azure_storage` section required
- Auto Loader reads via UC Volume
- **Zero credentials needed from user**

## Supported Storage Protocols

### CockroachDB Changefeed (WRITE side)
- ✅ `azure://` - Azure Blob Storage with credentials
- ✅ `s3://` - AWS S3 with credentials
- ✅ `gs://` - Google Cloud Storage with credentials
- ❌ `abfss://` - NOT supported by CockroachDB
- ❌ `wasb://` - NOT supported by CockroachDB

### Databricks Unity Catalog (READ side)
- ✅ `/Volumes/...` - Unity Catalog managed access
- ✅ `abfss://` - Direct Azure access (if needed)
- ✅ `s3://` - Direct S3 access (if needed)
- ✅ `gs://` - Direct GCS access (if needed)

## Benefits of UC Volumes for Auto Loader

UC Volumes **completely eliminate** credential management for Auto Loader operations:

1. **Zero credential exposure**
   - No Azure credentials in notebooks
   - No credentials in config (for production)
   - No credential rotation needed
   - Unity Catalog handles everything

2. **Simplified code**
   ```python
   # Without UC Volume - credentials required
   df = spark.readStream.format("cloudFiles") \
       .option("cloudFiles.format", "parquet") \
       .option("path", "abfss://container@account.dfs.core.windows.net/") \
       .option("account_key", secret) \
       .load()
   
   # With UC Volume - NO credentials
   df = spark.readStream.format("cloudFiles") \
       .option("cloudFiles.format", "parquet") \
       .load("/Volumes/catalog/schema/volume/")
   ```

3. **Governed access**
   - Grant access: `GRANT USE VOLUME ON volume_name TO user`
   - Works across all notebooks
   - Audit logging built-in
   - Credential-free operations

4. **Multi-cloud ready**
   - Same `/Volumes/...` path for Azure, AWS, GCP
   - Change storage backend without code changes
   - Provider-agnostic data access

## Common Misconceptions

### ❌ Misconception #1
"I need to provide Azure credentials to Auto Loader when using UC Volumes"

### ✅ Reality
"Auto Loader **never** needs Azure credentials when using UC Volumes. Unity Catalog handles all authentication automatically. Azure credentials are only needed for **creating changefeeds** (tutorial/demo operation)."

### ❌ Misconception #2
"If I use UC Volumes, I don't need Azure credentials in my config at all"

### ✅ Reality
"You need Azure credentials in config **only if** you're running the tutorial/demo and creating changefeeds with `create_changefeed_from_config()`. For production Auto Loader (just reading), UC Volumes eliminate the need for credentials completely."

## Error Messages

### If Azure Credentials Missing During Changefeed Creation
```python
ValueError: Azure credentials required in config when creating changefeeds.
CockroachDB changefeeds require azure:// protocol with credentials to write to storage.
If you're only reading with Auto Loader, you don't need azure_storage in config.
```

**Solution:** 
- **For tutorial/demo:** Add `azure_storage` section to config
- **For production (just reading):** Remove the `create_changefeed_from_config()` call

### If CockroachDB Sees abfss://
```
ERROR: unsupported sink: abfss
```

**Solution:** This is now handled automatically - the code converts `abfss://` to `azure://` with credentials.

## Future: AWS S3 and GCP GCS

When UC Volumes are backed by S3 or GCS:

- **S3**: CockroachDB supports `s3://` natively (credentials needed)
- **GCS**: CockroachDB supports `gs://` natively (credentials needed)
- **Pattern**: Same as Azure - UC Volume for Databricks, native protocol for CockroachDB

## Summary

**For Tutorial/Demo (Creating Changefeeds):**
1. ✅ Requires `azure_storage` in config - for `CREATE CHANGEFEED` command
2. ✅ CockroachDB writes using `azure://` protocol with credentials
3. ✅ Auto Loader reads using `/Volumes/...` path - **NO credentials needed**
4. ✅ Config needs **both** `uc_volume` and `azure_storage` sections

**For Production (Auto Loader Only):**
1. ✅ NO `azure_storage` needed - changefeeds already created
2. ✅ Auto Loader reads using `/Volumes/...` path - **NO credentials needed**
3. ✅ Config needs **only** `uc_volume` section
4. ✅ Zero credential management required

**Architecture:**
```
Tutorial/Demo:
  CockroachDB (azure:// + creds) → Azure Storage ← Auto Loader (/Volumes/... + UC)
     CREATE CHANGEFEED                                 NO credentials

Production:
  [Changefeeds already running] → Azure Storage ← Auto Loader (/Volumes/... + UC)
                                                       NO credentials
```

**Key Insight:** Unity Catalog completely eliminates credential management for Databricks/Auto Loader. Credentials are only needed when creating changefeeds (tutorial/demo operation).
