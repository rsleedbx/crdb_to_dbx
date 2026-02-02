# Notebooks

## test_cdc_scenario.ipynb

A comprehensive notebook for testing and validating CockroachDB CDC integration with Databricks.

### Features

- ✅ **Multiple Connector Modes**: Test VOLUME, AZURE, or DIRECT modes
- ✅ **Auto-detection**: Primary keys and column families detected automatically
- ✅ **Format Support**: Parquet and JSON changefeed formats
- ✅ **Validation**: Automated data consistency checks
- ✅ **Column Families**: Automatic fragment merging for split column families

### Prerequisites

1. **Configuration Files:**
   - `.env/cockroachdb_credentials.json`
   - `.env/cockroachdb_pipelines.json`

2. **Unity Catalog:**
   - Volume created (for VOLUME mode)
   - Schema and catalog configured

3. **CockroachDB:**
   - Changefeed configured and running
   - CDC data available in Azure or Volume

### Quick Start

1. **Upload notebook to Databricks:**
   ```bash
   # Via Databricks CLI
   databricks workspace import notebooks/test_cdc_scenario.ipynb /Users/your.name/test_cdc_scenario
   ```

2. **Update configuration in notebook (Step 2):**
   ```python
   TEST_FORMAT = "parquet"        # or "json"
   TEST_NAME = "usertable"        # your table name
   ITERATOR_MODE = ConnectorMode.VOLUME  # or AZURE_PARQUET, AZURE_JSON, DIRECT
   ```

3. **Run all cells**

### Configuration Options

#### Connector Modes

| Mode | Description | Configuration |
|------|-------------|---------------|
| `ConnectorMode.VOLUME` | Read from Unity Catalog Volume | Requires `volume_name` in pipeline config |
| `ConnectorMode.AZURE_PARQUET` | Read Parquet from Azure | Requires `azure_config` and `blob_prefix` |
| `ConnectorMode.AZURE_JSON` | Read JSON from Azure | Requires `azure_config` and `blob_prefix` |
| `ConnectorMode.DIRECT` | Direct CockroachDB connection | Requires `crdb_config` |

#### Test Scenarios

The notebook supports multiple test scenarios:

| Scenario | Format | Column Families | Use Case |
|----------|--------|----------------|----------|
| `simple_test` | Parquet/JSON | No | Basic CDC testing |
| `usertable` | Parquet/JSON | No | Standard table CDC |
| `usertable_with_split` | Parquet/JSON | Yes | Column family testing |

### Notebook Structure

**Step 1: Setup and Configuration**
- Import modules
- Load configuration files
- Initialize Spark session

**Step 2: Configure Test Scenario**
- Set test format (parquet/json)
- Set table name
- Configure connector mode

**Step 3: Auto-detect Schema**
- Detect primary keys from CockroachDB
- Detect column families
- Configure CDC mode

**Step 4: Load and Process CDC Data**
- Initialize connector
- Read CDC events
- Transform data
- Write to Delta Lake

**Step 5: Verification**
- Compare source vs target
- Verify data consistency
- Display results

### Expected Output

#### Successful Run

```
================================================================================
✅ TEST COMPLETED SUCCESSFULLY
================================================================================

📊 CDC Event Summary:
   Total events: 100
   INSERT: 50
   UPDATE: 30
   DELETE: 20

📊 Data Verification:
   Source rows: 30
   Target rows (deduplicated): 30
   Primary keys: ['ycsb_key']
   Column families: ['primary', 'family1', 'family2']

✅ All consistency checks passed
```

#### Failed Run (with diagnostics)

```
================================================================================
⚠️  ISSUES DETECTED
================================================================================

❌ Row count mismatch:
   Source: 30 rows
   Target: 28 rows
   Missing keys: [16, 23]

📊 Detailed Analysis:
   - Key 16: Missing in target
   - Key 23: Missing in target

🔍 Troubleshooting recommendations:
   1. Check changefeed status: SHOW CHANGEFEED JOBS;
   2. Verify Azure storage path
   3. Check for file processing errors
```

### Troubleshooting

#### "Configuration files not found"

**Error:**
```
FileNotFoundError: .env/cockroachdb_credentials.json not found
```

**Solution:**
1. Create `.env/` directory in repo root
2. Copy example configs:
   ```bash
   cp config_examples/*.example.json .env/
   ```
3. Update with your credentials

#### "Cannot connect to CockroachDB"

**Error:**
```
pg8000.exceptions.DatabaseError: Connection refused
```

**Solution:**
1. Verify host/port in credentials file
2. Check network connectivity
3. Ensure CockroachDB cluster is running
4. Test connection: `psql "connection_url"`

#### "No CDC files found"

**Error:**
```
ValueError: No CDC files found in volume directories
```

**Solution:**
1. Check changefeed is running: `SHOW CHANGEFEED JOBS;`
2. Verify file path configuration
3. Wait for changefeed to emit files (may take 10-30 seconds)
4. Check Azure storage directly

#### "Primary key detection failed"

**Error:**
```
RuntimeError: Could not detect primary keys
```

**Solution:**
1. Verify CockroachDB credentials are correct
2. Check table exists: `SHOW TABLES;`
3. Verify table has primary key: `SHOW CREATE TABLE tablename;`

### Advanced Usage

#### Testing Column Families

```python
# Step 2 configuration
TEST_NAME = "usertable_with_split"
TEST_FORMAT = "parquet"

# The notebook will automatically:
# - Detect column families from CockroachDB
# - Merge fragments
# - Verify complete rows
```

#### Testing Both Formats

Run the notebook twice with different formats:

**Run 1: Parquet**
```python
TEST_FORMAT = "parquet"
ITERATOR_MODE = ConnectorMode.AZURE_PARQUET
```

**Run 2: JSON**
```python
TEST_FORMAT = "json"
ITERATOR_MODE = ConnectorMode.AZURE_JSON
```

#### Custom Table Names

```python
# Test your own table
TEST_NAME = "my_custom_table"
TEST_FORMAT = "parquet"

# Notebook will auto-detect schema from CockroachDB
```

### Performance Tips

1. **Use Parquet for large datasets** (10x smaller than JSON)
2. **Use VOLUME mode for testing** (faster than Azure)
3. **Enable caching** for repeated runs:
   ```python
   target_df.cache()
   ```
4. **Adjust Spark parallelism**:
   ```python
   spark.conf.set("spark.sql.shuffle.partitions", "200")
   ```

### Integration with CI/CD

The notebook can be automated using Databricks Jobs:

```bash
databricks jobs create --json '{
  "name": "CDC Test",
  "notebook_task": {
    "notebook_path": "/Users/your.name/test_cdc_scenario",
    "base_parameters": {
      "TEST_FORMAT": "parquet",
      "TEST_NAME": "usertable",
      "ITERATOR_MODE": "volume"
    }
  },
  "new_cluster": {...}
}'
```

### Related Documentation

- [Configuration Guide](../config_examples/README.md)
- [Quick Start](../QUICKSTART.md)
- [Blog Post](../docs/stream-changefeed-to-databricks-azure.md)

### Support

If you encounter issues not covered here:
1. Check [GitHub Issues](https://github.com/rsleedbx/crdb_to_dbx/issues)
2. Review [Troubleshooting Guide](../README.md#troubleshooting)
3. Open a new issue with:
   - Error message
   - Configuration (redact credentials)
   - Notebook cell output
