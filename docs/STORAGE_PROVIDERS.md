# Storage Provider Support

*Updated: February 4, 2026*

The CockroachDB to Databricks CDC connector supports multiple cloud storage providers with a unified interface.

---

## Choosing Your Storage Provider

### Quick Comparison

| Feature | Azure Blob Storage | Unity Catalog Volume |
|---------|-------------------|---------------------|
| **Setup Complexity** | Simple | Moderate (requires UC setup) |
| **Credential Management** | Manual (keys in config) | Automated (UC managed) |
| **Governance** | None | Unity Catalog built-in |
| **Auto Loader Performance** | Fast (~1-2s) | Fast (~2-3s with Spark) |
| **Best For** | Quick start, testing | Production, compliance, multi-user |
| **Production Ready** | ✅ Yes | ✅ Yes |

### When to Choose Each

**Choose Azure Blob Storage if:**
- ✅ You want the simplest setup
- ✅ Testing or prototyping
- ✅ Single-user development
- ✅ Don't need Unity Catalog governance

**Choose Unity Catalog Volume if:**
- ✅ Production deployment
- ✅ Need governance and audit logging
- ✅ Multiple users need access
- ✅ Want centralized permission management
- ✅ Compliance requirements

---

## Supported Providers

### 1. Azure Blob Storage (`cockroachdb_azure.py`)
- ✅ Production ready
- ✅ Uses Azure Storage SDK
- ✅ Direct access with storage account keys
- ✅ Works in all Databricks environments
- ✅ Fastest setup (minimal configuration)

### 2. Unity Catalog External Volumes (`cockroachdb_uc_volume.py`)
- ✅ Production ready
- ✅ Uses Spark for fast parallel file operations
- ✅ Works in Databricks workspace, Serverless, and notebooks
- ✅ **Zero credentials needed for Auto Loader** (Unity Catalog managed)
- ✅ Built-in governance and auditing
- ✅ Centralized access control

### 3. Future Providers (Extensible Design)
- 🔜 AWS S3 (`cockroachdb_s3.py`)
- 🔜 Google Cloud Storage (`cockroachdb_gcs.py`)
- 🔜 Cloudflare R2 (`cockroachdb_r2.py`)

---

## Configuration

### Azure Blob Storage Configuration

**Step 1: Create Azure Resources**
```bash
# Run the setup script (creates storage account, container, credentials)
./scripts/01_azure_storage.sh
```

**Step 2: Create Config File**

`config.json`:
```json
{
  "cdc_config": {
    "data_source": "azure_storage",
    "mode": "update_delete",
    "column_family_mode": "multi_cf",
    "format": "parquet"
  },
  "azure_storage": {
    "account_name": "cockroachcdc1234567890",
    "account_key": "your-azure-storage-key",
    "container_name": "changefeed-events"
  },
  "cockroachdb_source": {
    "catalog": "defaultdb",
    "schema": "public",
    "table_name": "usertable"
  },
  "databricks_target": {
    "catalog": "main",
    "schema": "default",
    "table_name": "usertable_cdc"
  }
}
```

**Step 3: Use in Code**
```python
from cockroachdb_config import load_and_process_config
from cockroachdb_storage import check_files
from cockroachdb_autoload import ingest_cdc_with_merge_multi_family

config = load_and_process_config("config.json")

# Check files
result = check_files(config, spark)

# Run ingestion
query = ingest_cdc_with_merge_multi_family(config, spark)
```

### Unity Catalog Volume Configuration

**Step 1: Create Unity Catalog Volume**
```bash
# Run the setup script (creates volume + all Azure resources)
./scripts/01_azure_storage.sh
```

The script creates:
- Azure storage account and container
- Managed identity and access connector
- Unity Catalog storage credential
- Unity Catalog external location
- Unity Catalog external volume

**Step 2: Create Config File**

`config.json`:
```json
{
  "cdc_config": {
    "data_source": "uc_external_volume",
    "mode": "update_delete",
    "column_family_mode": "multi_cf",
    "format": "parquet"
  },
  "uc_external_volume": {
    "volume_catalog": "main",
    "volume_schema": "default",
    "volume_name": "cockroachdb_cdc_volume",
    "volume_full_path": "main.default.cockroachdb_cdc_volume",
    "volume_id": "your-volume-id"
  },
  "azure_storage": {
    "account_name": "cockroachcdc1234567890",
    "account_key": "your-azure-storage-key",
    "container_name": "changefeed-events"
  },
  "cockroachdb_source": {
    "catalog": "defaultdb",
    "schema": "public",
    "table_name": "usertable"
  },
  "databricks_target": {
    "catalog": "main",
    "schema": "default",
    "table_name": "usertable_cdc"
  }
}
```

**Note:** `azure_storage` section is only needed for changefeed creation (tutorial/demo). For production Auto Loader (just reading), you can omit it. See [UC_VOLUME_CREDENTIALS.md](./UC_VOLUME_CREDENTIALS.md) for details.

**Step 3: Grant Volume Access**
```sql
-- Grant access to users who need to read CDC data
GRANT USE VOLUME ON main.default.cockroachdb_cdc_volume TO `user@company.com`;
```

**Step 4: Use in Code**
```python
from cockroachdb_config import load_and_process_config
from cockroachdb_storage import check_files
from cockroachdb_autoload import ingest_cdc_with_merge_multi_family

config = load_and_process_config("config.json")

# Check files (automatically uses UC Volume)
result = check_files(config, spark)

# Run ingestion (automatically uses UC Volume)
query = ingest_cdc_with_merge_multi_family(config, spark)
```

**Key Difference:** Same code, different config! The `data_source` field determines which storage provider is used.

---

## Unified Interface Design

All storage providers follow the same interface pattern:

```python
# Check for files (immediate)
result = check_<provider>_files(
    <provider_specific_params>,
    source_catalog: str,
    source_schema: str,
    source_table: str,
    target_table: str,
    verbose: bool = True,
    format: str = "parquet"
) -> Dict[str, Any]

# Wait for files (with timeout)
result = wait_for_changefeed_files(
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
) -> Dict[str, Any]
```

**Return Structure (Consistent Across Providers):**
```python
{
    'data_files': [      # List of data files
        {
            'name': str,    # Filename
            'path': str,    # Full path
            'size': int     # Size in bytes
        },
        ...
    ],
    'resolved_files': [  # List of RESOLVED files
        {
            'name': str,
            'path': str,
            'size': int
        },
        ...
    ],
    'total': int         # Total file count
}
```

---

## Usage Examples

### Azure Blob Storage

```python
from crdb_to_dbx import check_azure_files, wait_for_changefeed_files_azure

# Check for existing files
result = check_azure_files(
    storage_account_name="mystorageaccount",
    storage_account_key="<key>",
    container_name="cockroachcdc",
    source_catalog="defaultdb",
    source_schema="public",
    source_table="usertable",
    target_table="usertable_cdc"
)

print(f"Data files: {len(result['data_files'])}")
print(f"RESOLVED files: {len(result['resolved_files'])}")

# Wait for RESOLVED file (recommended for production)
result = wait_for_changefeed_files_azure(
    storage_account_name="mystorageaccount",
    storage_account_key="<key>",
    container_name="cockroachcdc",
    source_catalog="defaultdb",
    source_schema="public",
    source_table="usertable",
    target_table="usertable_cdc",
    max_wait=300,
    wait_for_resolved=True  # ✅ Guarantees completeness
)

if result['success']:
    print(f"✅ RESOLVED file: {result['resolved_file']}")
else:
    print(f"❌ Timeout after {result['elapsed_time']}s")
```

### Unity Catalog Volume

```python
from crdb_to_dbx import check_volume_files, wait_for_changefeed_files_volume

# Check for existing files
result = check_volume_files(
    volume_path="/Volumes/main/default/cdc",
    source_catalog="defaultdb",
    source_schema="public",
    source_table="usertable",
    target_table="usertable_cdc",
    spark=spark  # Required for Volume operations
)

print(f"Data files: {len(result['data_files'])}")
print(f"RESOLVED files: {len(result['resolved_files'])}")

# Wait for RESOLVED file (recommended for production)
result = wait_for_changefeed_files(
    volume_path="/Volumes/main/default/cdc",
    source_catalog="defaultdb",
    source_schema="public",
    source_table="usertable",
    target_table="usertable_cdc",
    spark=spark,
    max_wait=300,
    wait_for_resolved=True  # ✅ Guarantees completeness
)

if result['success']:
    print(f"✅ RESOLVED file: {result['resolved_file']}")
else:
    print(f"❌ Timeout after {result['elapsed_time']}s")
```

---

## Parameter Mapping

### Provider-Specific Parameters

| Azure | Unity Catalog Volume | Future: AWS S3 | Future: GCP GCS |
|-------|---------------------|----------------|-----------------|
| `storage_account_name` | `volume_path` | `bucket_name` | `bucket_name` |
| `storage_account_key` | `spark` | `access_key_id` | `credentials_json` |
| `container_name` | - | `secret_access_key` | - |

### Common Parameters (All Providers)

| Parameter | Type | Description |
|-----------|------|-------------|
| `source_catalog` | str | CockroachDB catalog (database) |
| `source_schema` | str | CockroachDB schema |
| `source_table` | str | Source table name |
| `target_table` | str | Target table name |
| `verbose` | bool | Print detailed output (default: True) |
| `format` | str | Changefeed format: "parquet" or "json" (default: "parquet") |
| `max_wait` | int | Maximum seconds to wait (default: 120) |
| `check_interval` | int | Seconds between checks (default: 5) |
| `wait_for_resolved` | bool | Wait for RESOLVED file (default: True, recommended) |

---

## When to Use Each Provider

### Azure Blob Storage
**Use when:**
- ✅ You need direct Azure storage access
- ✅ Running outside Databricks (e.g., local development with Databricks Connect)
- ✅ Need to access storage without Unity Catalog

**Advantages:**
- Works in any environment with Azure SDK
- Direct control over storage credentials
- No dependency on Databricks features

**Disadvantages:**
- Requires managing Azure credentials
- More complex setup
- Credentials must be secured

### Unity Catalog Volume
**Use when:**
- ✅ Running in Databricks workspace
- ✅ Want Unity Catalog governance
- ✅ Need Serverless compute compatibility
- ✅ Want zero credential management

**Advantages:**
- No credential management (Unity Catalog handles it)
- Native Databricks integration
- Works with Serverless compute
- Governed access (Unity Catalog permissions)

**Disadvantages:**
- Requires Unity Catalog setup
- Only works in Databricks environments
- Requires `spark` session

---

## Switching Between Providers

The unified storage functions make switching trivial - just update your config:

**Azure Configuration:**
```json
{
  "cdc_config": {
    "data_source": "azure_storage"
  },
  "azure_storage": { ... }
}
```

**UC Volume Configuration:**
```json
{
  "cdc_config": {
    "data_source": "uc_external_volume"
  },
  "uc_external_volume": { ... }
}
```

**Same Code, Different Config:**
```python
from cockroachdb_storage import check_files

# Works with BOTH Azure and UC Volume
config = load_and_process_config("config.json")
result = check_files(config, spark)

# Automatically uses the right provider based on config.data_source
print(f"Data files: {len(result['data_files'])}")
print(f"RESOLVED files: {len(result['resolved_files'])}")
```

---

## Adding New Providers

To add a new cloud provider:

1. **Create `cockroachdb_<provider>.py`**
2. **Implement two functions** with the same signature pattern:
   - `check_<provider>_files()`
   - `wait_for_changefeed_files()`
3. **Return the same dictionary structure**
4. **Add exports to `__init__.py`**

Example template:

```python
# cockroachdb_s3.py
import boto3
from typing import Dict, Any, Optional

def check_s3_files(
    bucket_name: str,
    access_key_id: str,
    secret_access_key: str,
    source_catalog: str,
    source_schema: str,
    source_table: str,
    target_table: str,
    region: str = "us-east-1",
    verbose: bool = True,
    format: str = "parquet"
) -> Dict[str, Any]:
    """Check for changefeed files in S3."""
    # S3-specific implementation
    s3 = boto3.client(
        's3',
        aws_access_key_id=access_key_id,
        aws_secret_access_key=secret_access_key,
        region_name=region
    )
    
    prefix = f"{format}/{source_catalog}/{source_schema}/{source_table}/{target_table}/"
    
    # List objects in bucket
    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
    
    # Categorize files (same logic as other providers)
    all_files = response.get('Contents', [])
    data_files = [
        f for f in all_files
        if f['Key'].endswith('.parquet')
        and '.RESOLVED' not in f['Key']
        and '/_metadata/' not in f['Key']
        and not f['Key'].split('/')[-1].startswith('_')
    ]
    resolved_files = [f for f in all_files if '.RESOLVED' in f['Key']]
    
    return {
        'data_files': data_files,
        'resolved_files': resolved_files,
        'total': len(all_files)
    }

def wait_for_changefeed_files(
    bucket_name: str,
    access_key_id: str,
    secret_access_key: str,
    source_catalog: str,
    source_schema: str,
    source_table: str,
    target_table: str,
    region: str = "us-east-1",
    max_wait: int = 120,
    check_interval: int = 5,
    stabilization_wait: Optional[int] = None,
    format: str = "parquet",
    wait_for_resolved: bool = True
) -> Dict[str, Any]:
    """Wait for changefeed files in S3."""
    # Same logic as Azure/Volume versions
    # Poll check_s3_files() until files/RESOLVED appear
    pass
```

---

## Best Practices

### 1. Always Use RESOLVED Mode in Production
```python
# ✅ Recommended
result = wait_for_changefeed_files_volume(
    ...,
    wait_for_resolved=True  # Guarantees completeness
)

# ❌ Not recommended (unless you have a specific reason)
result = wait_for_changefeed_files_volume(
    ...,
    wait_for_resolved=False  # Legacy mode
)
```

### 2. Choose Provider Based on Environment
```python
# Recommended: Use unified storage functions
from cockroachdb_storage import check_files

# Load config (determines provider automatically)
config = load_and_process_config("config.json")

# Works with both Azure and UC Volume
result = check_files(config, spark)

# Provider selection is config-driven!
```

### 3. Handle Timeouts Gracefully
```python
result = wait_for_changefeed_files_volume(
    ...,
    max_wait=300  # 5 minutes
)

if not result['success']:
    print(f"⚠️  Timeout after {result['elapsed_time']}s")
    print(f"   Files found: {result['file_count']}")
    # Decide: retry, alert, or continue
```

---

## Troubleshooting

### Azure: "Cannot list blobs"
- ✅ Check storage account name and key
- ✅ Verify container exists
- ✅ Check network connectivity

### Volume: "Path not found"
- ✅ Verify volume exists: `CREATE VOLUME IF NOT EXISTS main.default.cdc;`
- ✅ Check path format: `/Volumes/<catalog>/<schema>/<volume>`
- ✅ Verify Unity Catalog permissions

### All Providers: "No RESOLVED file"
- ✅ Ensure changefeed has `resolved='10s'` option
- ✅ Increase `max_wait` (RESOLVED writes every ~10 seconds)
- ✅ Check path and table names match exactly

---

**Next**: See [CockroachDB CDC Tutorial](../notebooks/cockroachdb-cdc-tutorial.ipynb) for complete examples.
