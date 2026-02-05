# CockroachDB to Databricks CDC Connector

Stream CockroachDB data changes to Databricks Delta Lake using changefeeds, cloud storage, and Auto Loader. Supports both initial snapshots and ongoing CDC events (inserts, updates, deletes) with ACID guarantees.

## Quick Start

📖 Follow the **[Complete Tutorial](docs/stream-changefeed-to-databricks-azure.md)** for step-by-step setup of CockroachDB changefeeds, Unity Catalog External Volumes, and Auto Loader.

Once configured, run CDC ingestion in Databricks:

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
├── config_examples/                # Configuration templates
│   ├── azure_json_mode.json
│   ├── azure_parquet_mode.json
│   ├── volume_mode.json
│   └── README.md
├── docs/                           # Documentation
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
- ✅ Nanosecond-precision timestamps: `_cdc_timestamp_nanos` (bigint) for ordering/merge, `_cdc_timestamp` (string) for display

## Documentation

### Getting Started
- **[Complete Tutorial](docs/stream-changefeed-to-databricks-azure.md)** - Step-by-step guide with architecture and examples
- **[Configuration Guide](config_examples/README.md)** - Configuration options and examples

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
