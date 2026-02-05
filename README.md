# CockroachDB to Databricks CDC Connector

Stream CockroachDB data changes to Databricks Delta Lake using changefeeds, cloud storage, and Auto Loader. Supports both initial snapshots and ongoing CDC events (inserts, updates, deletes) with ACID guarantees.

## Features

- ✅ **Dual Storage Support**: Azure Blob Storage or Unity Catalog External Volumes
- ✅ **Multiple CDC Modes**: Append-only (SCD Type 2) and Update-Delete (SCD Type 1) with MERGE
- ✅ **Format Support**: Parquet (recommended) and JSON changefeed formats
- ✅ **Column Families**: Automatic fragment merging for split column families
- ✅ **RESOLVED Watermarking**: Transaction consistency and completeness guarantees
- ✅ **Multi-Table Coordination**: Maintain referential integrity across tables
- ✅ **Auto-detection**: Primary keys and schema automatically detected
- ✅ **Zero Credentials**: UC Volumes eliminate credential management for Auto Loader

## Quick Start

### 1. Create a CockroachDB Table

```sql
CREATE TABLE usertable (
    ycsb_key INT PRIMARY KEY,
    field0 TEXT,
    field1 TEXT,
    field2 TEXT
);
```

### 2. Create a Changefeed to Azure Storage

```sql
CREATE CHANGEFEED FOR TABLE usertable
INTO 'azure://changefeed-events/parquet/defaultdb/public/usertable/usertable_cdc/?AZURE_ACCOUNT_NAME={account}&AZURE_ACCOUNT_KEY={key}'
WITH 
    format='parquet',
    updated,
    resolved='10s';  -- ✅ Required for data consistency
```

### 3. Configure Storage Access

**Option A: Unity Catalog External Volume** (Recommended for Production)
```bash
# Run setup script (creates Azure + UC resources)
./scripts/01_azure_storage.sh
```

**Option B: Direct Azure Access** (Simpler for Testing)
```bash
# Use Azure storage account keys directly
```

See [Storage Providers](docs/STORAGE_PROVIDERS.md) for detailed setup.

### 4. Configure the Connector

Create `config.json`:
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
    "volume_name": "cdc_volume"
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

### 5. Run CDC Ingestion in Databricks

```python
from crdb_to_dbx.cockroachdb_config import load_and_process_config
from crdb_to_dbx import ingest_cdc_with_merge_multi_family

# Load configuration
config = load_and_process_config("config.json")

# Run CDC ingestion (automatically handles storage, format, column families)
query = ingest_cdc_with_merge_multi_family(
    config=config,
    spark=spark
)
```

See the complete tutorial: [Stream CockroachDB CDC to Databricks](docs/stream-changefeed-to-databricks-azure.md)

## Architecture

```
┌───────────────────────────────────────┐
│      CockroachDB Changefeed           │
│  (Parquet CDC events with RESOLVED)   │
└──────────────┬────────────────────────┘
               ↓
┌───────────────────────────────────────┐
│      Azure Blob Storage (ADLS Gen2)   │
│    changefeed-events/parquet/...      │
└──────────────┬────────────────────────┘
               ↓
┌───────────────────────────────────────┐
│         Unity Catalog                 │
│  External Locations OR External Volumes│
└──────────────┬────────────────────────┘
               ↓
┌───────────────────────────────────────┐
│      Databricks Auto Loader           │
│  (Automatic file discovery + CDC)     │
└──────────────┬────────────────────────┘
               ↓
┌───────────────────────────────────────┐
│      Delta Lake (ACID + History)      │
└───────────────────────────────────────┘
```

## Storage Options

| Option | Best For | Credentials | Setup |
|--------|----------|-------------|-------|
| **Unity Catalog Volume** | Production, governance | Zero for Auto Loader | Moderate |
| **Azure Blob Storage** | Quick start, testing | Manual keys | Simple |

See [Storage Providers Guide](docs/STORAGE_PROVIDERS.md) for comparison and configuration.

## CDC Modes

### Append-Only (SCD Type 2)
Stores all CDC events as rows for full history tracking, compliance, and audit logs.

```python
config.cdc_config.mode = "append_only"
query = ingest_cdc_append_only_multi_family(config, spark)
```

**Target table contains:** All INSERT, UPDATE, DELETE events with timestamps

### Update-Delete (SCD Type 1)
Maintains current state only using Delta Lake MERGE operations.

```python
config.cdc_config.mode = "update_delete"
query = ingest_cdc_with_merge_multi_family(config, spark)
```

**Target table contains:** Latest state per primary key

## Column Families

For tables with multiple column families (`split_column_families`), the connector:

1. **Detects** fragmented CDC events (one file per column family)
2. **Merges** fragments using `F.first(col, ignorenulls=True)` 
3. **Reconstructs** complete rows before writing to Delta

```sql
-- CockroachDB changefeed with column families
CREATE CHANGEFEED FOR TABLE usertable_wide
INTO 'azure://...'
WITH 
    format='parquet',
    split_column_families,  -- Multiple files per update
    resolved='10s';         -- ✅ REQUIRED for completeness guarantee
```

**Why use RESOLVED timestamps:** Without `resolved`, you risk processing incomplete fragments (data corruption). The RESOLVED watermark guarantees all column family fragments have landed before processing.

## Project Structure

```
crdb_to_dbx/
├── crdb_to_dbx/                    # Core CDC modules
│   ├── cockroachdb_autoload.py     # Auto Loader + CDC ingestion
│   ├── cockroachdb_storage.py      # Unified storage abstraction
│   ├── cockroachdb_azure.py        # Azure Blob Storage provider
│   ├── cockroachdb_uc_volume.py    # Unity Catalog Volume provider
│   ├── cockroachdb_sql.py          # Changefeed management
│   ├── cockroachdb_config.py       # Configuration handling
│   ├── cockroachdb_ycsb.py         # Workload generation
│   └── cockroachdb-cdc-tutorial.ipynb  # Interactive tutorial
├── notebooks/                      # Tutorial notebooks
│   └── cockroachdb-cdc-tutorial.ipynb
├── config_examples/                # Configuration templates
│   ├── azure_json_mode.json
│   ├── azure_parquet_mode.json
│   ├── volume_mode.json
│   └── README.md
├── docs/                           # Documentation
│   ├── STORAGE_PROVIDERS.md
│   ├── stream-changefeed-to-databricks-azure.md
│   └── learnings/                  # Historical docs
├── scripts/                        # Setup automation
│   └── 01_azure_storage.sh         # Azure + UC setup
├── README.md
├── setup.py
└── requirements.txt
```

## Requirements

- Python 3.8+
- PySpark 3.3+
- Databricks Runtime 13.3+ (or Serverless)
- CockroachDB 22.1+ with changefeeds
- Unity Catalog (optional, for UC Volume mode)

## Key Concepts

### RESOLVED Timestamps

RESOLVED files guarantee that all CDC events and column family fragments are complete up to a specific timestamp. This is **critical** for:
- ✅ Tables with multiple column families (prevents incomplete rows)
- ✅ Multi-table CDC (maintains referential integrity)
- ✅ Production workloads (ensures data consistency)

```sql
-- Enable RESOLVED timestamps in changefeed
CREATE CHANGEFEED FOR TABLE usertable
INTO 'azure://...'
WITH format='parquet', updated, resolved='10s';
```

The connector automatically:
1. Scans for `.RESOLVED` files in storage
2. Extracts the watermark timestamp
3. Filters CDC events: `__crdb__updated <= watermark`
4. Guarantees completeness before processing

### Multi-Table Coordination

Maintain transactional consistency across multiple tables:

```python
# Each table's RESOLVED file provides a watermark
# Use minimum watermark across tables for atomic processing
# Ensures referential integrity (no partial transactions)
```

See the tutorial notebook for complete examples.

## Tutorials & Examples

### Interactive Tutorial
The repository includes comprehensive tutorial notebooks:

**`crdb_to_dbx/cockroachdb-cdc-tutorial.ipynb`**
- Complete end-to-end CDC pipeline
- All CDC modes (append-only, update-delete)
- Both column family modes (single, multi)
- RESOLVED watermarking examples
- YCSB workload generation

**Coverage:**
- ✅ CDC event processing (INSERT, UPDATE, DELETE)
- ✅ Column family fragment merging
- ✅ Primary key auto-detection
- ✅ RESOLVED watermark coordination
- ✅ Multi-table transaction consistency
- ✅ Both storage providers (Azure, UC Volume)

## Documentation

### Getting Started
- **[Quick Start](QUICKSTART.md)** - Get started quickly
- **[Configuration Guide](config_examples/README.md)** - Detailed configuration options
- **[Blog Post: Stream CockroachDB CDC to Databricks](docs/stream-changefeed-to-databricks-azure.md)** - Complete tutorial

### Storage & Deployment
- **[Storage Providers](docs/STORAGE_PROVIDERS.md)** - Choose and configure storage (Azure or UC Volume)
- **[UC Volume Credentials](docs/UC_VOLUME_CREDENTIALS.md)** - When credentials are needed
- **[UC Volume Auto Loader](docs/UC_VOLUME_AUTOLOADER.md)** - Auto Loader best practices

### Troubleshooting
- **[Troubleshooting Hangs](docs/TROUBLESHOOTING_HANG.md)** - Debug apparent hangs in file listing

### Implementation Details
- **[Learnings & Summaries](docs/learnings/README.md)** - Implementation learnings and bug fixes
- **[Evolution Strategy](CONNECTOR_EVOLUTION_STRATEGY.md)** - Architectural decisions and roadmap

## Related Resources

- [CockroachDB Changefeed Documentation](https://www.cockroachlabs.com/docs/stable/create-changefeed)
- [Databricks Auto Loader](https://docs.databricks.com/ingestion/auto-loader/)
- [Delta Lake MERGE INTO](https://docs.databricks.com/delta/merge.html)
- [Unity Catalog External Volumes](https://docs.databricks.com/en/connect/unity-catalog/volumes.html)

## License

Apache 2.0 - See [LICENSE](LICENSE) for details.

## Contributing

We welcome contributions! Please use GitHub Issues for bug reports and Pull Requests for changes.

## Credits

**Author**: [Robert Lee](https://github.com/rsleedbx), Field Engineer at Databricks

**Acknowledgments:**
- [Andrew Deally](https://andrewdeally.medium.com/) - Databricks
- CockroachDB team for changefeed architecture and best practices

## Changelog

### v3.0.0 (2026-02-04) - Streamlined Architecture

**Breaking Changes:**
- Removed `dbutils` dependency from all CDC modules
- Removed experimental `connector.py` framework (7,100 lines)
- Simplified function signatures (removed optional parameters)

**New Features:**
- Dual storage support: Azure Blob Storage + Unity Catalog External Volumes
- Configuration-driven storage provider selection
- Spark-only file operations (5-10x faster for UC Volumes)
- Zero credential management for UC Volume Auto Loader

**Improvements:**
- API simplification: 10+ parameters → 3 parameters
- Fail-fast error handling (explicit exceptions vs silent failures)
- Standardized file object format across providers
- Transaction management fixes (explicit commits for read queries)
- Retry logic for CockroachDB serializable conflicts (SQLSTATE 40001)

**Documentation:**
- Complete tutorial guide with architecture diagrams
- Storage provider comparison and configuration guide
- Credential requirements explained (tutorial vs production)
- Troubleshooting guides for common issues
- Historical learnings archive in `docs/learnings/`

### v1.0.0 (2026-01-30) - Initial Release
- Support for Parquet and JSON changefeed formats
- Column family fragment merging
- Auto-detection of primary keys and column families
- Azure Blob Storage support
- CDC processing with ACID guarantees
