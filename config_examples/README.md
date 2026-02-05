# CockroachDB CDC Configuration Examples

This directory contains example configurations for different storage providers and CDC modes.

## Configuration Structure

The connector uses JSON configuration files with the following structure:

```json
{
  "cockroachdb": {
    "host": "your-cluster.cockroachlabs.cloud",
    "port": 26257,
    "user": "username",
    "password": "password",
    "database": "defaultdb"
  },
  "cdc_config": {
    "data_source": "uc_external_volume",  // or "azure_storage"
    "mode": "update_delete",              // or "append_only"
    "column_family_mode": "multi_cf",     // or "single_cf"
    "format": "parquet",                  // or "json"
    "primary_key_columns": ["ycsb_key"]
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
  },
  "uc_external_volume": {  // Use this for Unity Catalog Volumes
    "volume_catalog": "main",
    "volume_schema": "default",
    "volume_name": "cdc_volume"
  },
  "azure_storage": {  // Use this for direct Azure Blob Storage
    "account_name": "mystorageaccount",
    "account_key": "base64_key==",
    "container_name": "changefeed-events"
  }
}
```

## Quick Start

1. **Copy an example config:**
   ```bash
   cp config_examples/azure_parquet_mode.json config.json
   ```

2. **Edit the config:**
   ```bash
   nano config.json
   # Update CockroachDB credentials, storage settings, table names
   ```

3. **Load and use in Databricks:**
   ```python
   from crdb_to_dbx.cockroachdb_config import load_and_process_config
   from crdb_to_dbx import ingest_cdc_with_merge_multi_family
   
   config = load_and_process_config("config.json")
   query = ingest_cdc_with_merge_multi_family(config=config, spark=spark)
   ```

## Available Configurations

### Unity Catalog Volume Mode

**File:** `volume_mode.json` (needs updating to match current structure)

**Use for:** Governance, multi-user access, zero credential Auto Loader

**Key settings:**
- `data_source`: `"uc_external_volume"`
- Configure `uc_external_volume` section with catalog/schema/volume names

### Azure Blob Storage - Parquet

**File:** `azure_parquet_mode.json`

**Use for:** High-performance CDC with columnar format

**Key settings:**
- `data_source`: `"azure_storage"`
- `format`: `"parquet"`
- Configure `azure_storage` section with account details

### Azure Blob Storage - JSON

**File:** `azure_json_mode.json`

**Use for:** Debugging, human-readable CDC events

**Key settings:**
- `data_source`: `"azure_storage"`
- `format`: `"json"`
- Configure `azure_storage` section with account details

### Dual Mode (Azure with both formats)

**File:** `azure_dual_mode.json`

**Use for:** Migration scenarios, format comparison

**Key settings:**
- `data_source`: `"azure_storage"`
- Both Parquet and JSON changefeeds running

## CDC Modes

### Append-Only (SCD Type 2)
Stores all CDC events as rows for full history:
```json
{
  "cdc_config": {
    "mode": "append_only"
  }
}
```

Use function: `ingest_cdc_append_only_multi_family()` or `ingest_cdc_append_only_single_family()`

### Update-Delete (SCD Type 1)
Maintains current state with MERGE:
```json
{
  "cdc_config": {
    "mode": "update_delete"
  }
}
```

Use function: `ingest_cdc_with_merge_multi_family()` or `ingest_cdc_with_merge_single_family()`

## Column Family Modes

### Single Column Family (Default)
For standard tables:
```json
{
  "cdc_config": {
    "column_family_mode": "single_cf"
  }
}
```

### Multiple Column Families
For wide tables with `split_column_families` in changefeed:
```json
{
  "cdc_config": {
    "column_family_mode": "multi_cf"
  }
}
```

## Credential Files

### CockroachDB Credentials

**File:** `cockroachdb_credentials.example.json`

```json
{
  "host": "your-cluster.cockroachlabs.cloud",
  "port": 26257,
  "database": "defaultdb",
  "user": "your_username",
  "password": "your_password"
}
```

### Azure Storage Credentials

**File:** `azure_credentials.example.json`

```json
{
  "account_name": "mystorageaccount",
  "account_key": "base64_key==",
  "container_name": "changefeed-events"
}
```

## See Also

- [Storage Providers Guide](../docs/STORAGE_PROVIDERS.md) - Detailed storage provider comparison
- [Quick Start](../QUICKSTART.md) - Step-by-step setup guide
- [Main README](../README.md) - Project overview and architecture
