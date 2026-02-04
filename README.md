# CockroachDB to Databricks CDC Connector

A lightweight Python connector for streaming CockroachDB changefeeds to Databricks Delta Lake.

## Features

- ✅ **Multiple CDC Modes**: Append-only (SCD Type 2) and Update-Delete (SCD Type 1)
- ✅ **Format Support**: Parquet and JSON changefeed formats
- ✅ **Column Families**: Automatic handling of split column families
- ✅ **Auto-detection**: Primary keys and column families detected from CockroachDB
- ✅ **Multiple Sources**: Unity Catalog Volumes, Azure Blob Storage, or Direct connection
- ✅ **Production Ready**: Tested with real-world CDC workloads

## Quick Start

### 1. Installation

```bash
git clone https://github.com/rsleedbx/crdb_to_dbx.git
cd crdb_to_dbx
pip install -e .
```

### 2. Configuration

Create configuration files in `.env/` directory:

**`.env/cockroachdb_credentials.json`:**
```json
{
  "host": "your-cluster.cockroachlabs.cloud",
  "port": 26257,
  "database": "defaultdb",
  "user": "your_username",
  "password": "your_password"
}
```

**`.env/cockroachdb_pipelines.json`:**
```json
{
  "catalog": "main",
  "schema": "your_schema",
  "volume_name": "cdc_files"
}
```

See `config_examples/` for complete configuration templates.

### 3. Run the Notebook

1. Upload `notebooks/test_cdc_scenario.ipynb` to Databricks
2. Update configuration variables in Step 2
3. Run all cells

## Architecture

```
CockroachDB Changefeed → Azure Blob Storage → Databricks Auto Loader → Delta Lake
                              ↓
                    Unity Catalog Volume (optional)
```

## Supported Modes

| Mode | Description | Use Case |
|------|-------------|----------|
| **VOLUME** | Read from Unity Catalog Volumes | File-based testing with pre-synced data |
| **AZURE_PARQUET** | Read Parquet from Azure Blob | Production CDC (recommended) |
| **AZURE_JSON** | Read JSON from Azure Blob | Development/debugging |
| **AZURE_DUAL** | Read both Parquet and JSON | Format migration or validation |
| **DIRECT** | Direct CockroachDB connection | Testing with sinkless changefeeds |

## CDC Modes

### Append-Only (SCD Type 2)
Stores all CDC events (INSERT, UPDATE, DELETE) as rows for full history tracking.

```python
ITERATOR_MODE = ConnectorMode.VOLUME
CDC_MODE = "append_only"  # Full history
```

### Update-Delete (SCD Type 1)
Maintains current state only using MERGE operations.

```python
CDC_MODE = "update_delete"  # Current state only
```

## Example Usage

```python
from crdb_to_dbx import ConnectorMode, load_and_merge_cdc_to_delta

# Load and merge CDC data to Delta table
result = load_and_merge_cdc_to_delta(
    spark=spark,
    source_table="usertable",
    target_table="main.my_schema.usertable_cdc",
    mode=ConnectorMode.VOLUME,
    crdb_config={"host": "...", "database": "..."},
    pipeline_config={"catalog": "main", "volume_name": "cdc_files"}
)
```

## Project Structure

```
crdb_to_dbx/
├── crdb_to_dbx/          # Main connector module
│   ├── __init__.py
│   └── connector.py      # Core CDC logic
├── notebooks/            # Example notebooks
│   └── test_cdc_scenario.ipynb
├── config_examples/      # Configuration templates
│   ├── volume_mode.json
│   ├── azure_parquet_mode.json
│   └── README.md
├── README.md
├── setup.py
├── requirements.txt
└── .gitignore
```

## Requirements

- Python 3.8+
- PySpark 3.3+
- Databricks Runtime 11.3+
- Unity Catalog enabled
- CockroachDB 22.1+ with changefeeds

## Configuration Examples

### Volume Mode (File-based Testing)
```json
{
  "pipeline_name": "my_cdc_pipeline",
  "mode": "volume",
  "catalog": "main",
  "schema": "my_schema",
  "volume_name": "cdc_files"
}
```

### Azure Parquet Mode (Production)
```json
{
  "pipeline_name": "my_cdc_pipeline",
  "mode": "azure_parquet",
  "catalog": "main",
  "schema": "my_schema",
  "azure_config": ".env/azure_credentials.json",
  "blob_prefix": "parquet/defaultdb/public/usertable"
}
```

See `config_examples/` for all modes.

## Column Families Support

The connector automatically handles CockroachDB's `split_column_families` feature:

1. **Detects** fragmented CDC events (one per column family)
2. **Merges** fragments using `F.first(col, ignorenulls=True)`
3. **Reconstructs** complete rows in Delta Lake

Perfect for high-throughput workloads with many columns.

## Testing

The repository includes a comprehensive test notebook that validates:

- ✅ CDC event processing (INSERT, UPDATE, DELETE)
- ✅ Column family fragment merging
- ✅ Primary key auto-detection
- ✅ Source-to-target data consistency
- ✅ Multiple format support (Parquet, JSON)

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

## License

Apache 2.0

## Author

Robert Lee ([@rsleedbx](https://github.com/rsleedbx))

## Contributing

Contributions welcome! Please open an issue or PR.

## Changelog

### v2.0.0 (2026-02-04)
- **Dual Storage Support**: Azure Blob Storage + Unity Catalog External Volumes
- **Unified Storage API**: Configuration-driven overlay functions
- **API Simplification**: Config-based function signatures (10+ params → 3)
- **Performance**: Spark-based file listing (5-10x faster for UC Volume)
- **Infrastructure**: Automated UC External Volume creation in setup scripts
- **Notebooks**: Updated to use unified storage API
- **Documentation**: Comprehensive migration guides and troubleshooting
- **Bug Fixes**: Connection management, function signatures

### v1.0.0 (2026-01-30)
- Initial release
- Support for Parquet and JSON formats
- Column family fragment merging
- Multiple connector modes (VOLUME, AZURE, DIRECT)
- Auto-detection of primary keys and column families
- Production-ready CDC processing
