# Quick Start Guide

Get started with CockroachDB CDC to Databricks in 5 minutes.

## Prerequisites

- Databricks workspace with Unity Catalog enabled
- CockroachDB cluster (22.1+)
- Python 3.8+

## Step 1: Clone and Install

```bash
git clone https://github.com/rsleedbx/crdb_to_dbx.git
cd crdb_to_dbx
pip install -e .
```

## Step 2: Configure Credentials

### CockroachDB Credentials

```bash
mkdir -p .env
cp config_examples/cockroachdb_credentials.example.json .env/cockroachdb_credentials.json
```

Edit `.env/cockroachdb_credentials.json`:
```json
{
  "host": "your-cluster.cockroachlabs.cloud",
  "port": 26257,
  "database": "defaultdb",
  "user": "your_username",
  "password": "your_password"
}
```

### Pipeline Configuration

```bash
cp config_examples/volume_mode.json .env/cockroachdb_pipelines.json
```

Edit `.env/cockroachdb_pipelines.json`:
```json
{
  "catalog": "main",
  "schema": "your_schema",
  "volume_name": "cdc_files"
}
```

## Step 3: Create Unity Catalog Volume

In Databricks SQL or notebook:

```sql
CREATE VOLUME IF NOT EXISTS main.your_schema.cdc_files;
```

## Step 4: Set Up CockroachDB Changefeed

In CockroachDB SQL shell:

```sql
-- Create a test table
CREATE TABLE usertable (
    ycsb_key INT PRIMARY KEY,
    field0 TEXT,
    field1 TEXT,
    field2 TEXT
);

-- Insert test data
INSERT INTO usertable (ycsb_key, field0, field1, field2)
VALUES 
    (1, 'value_1_0', 'value_1_1', 'value_1_2'),
    (2, 'value_2_0', 'value_2_1', 'value_2_2');

-- Create changefeed to Azure Blob Storage
CREATE CHANGEFEED FOR TABLE usertable
INTO 'azure://changefeed-events/parquet/defaultdb/public/usertable/?AZURE_ACCOUNT_NAME={storage_account}&AZURE_ACCOUNT_KEY={account_key}'
WITH 
    format='parquet',
    updated,
    resolved='10s';
```

## Step 5: Run the Notebook

1. **Upload to Databricks:**
   - Upload `notebooks/test_cdc_scenario.ipynb` to your Databricks workspace

2. **Configure the notebook:**
   Update these variables in Step 2:
   ```python
   TEST_FORMAT = "parquet"
   TEST_NAME = "usertable"
   ITERATOR_MODE = ConnectorMode.VOLUME
   ```

3. **Run all cells**

## Verification

The notebook will:
1. ✅ Auto-detect primary keys from CockroachDB
2. ✅ Load CDC events from your configured source
3. ✅ Transform and merge to Delta Lake
4. ✅ Verify data consistency

## Expected Output

```
================================================================================
CONFIGURATION SETUP
================================================================================

✅ Configuration loaded!

================================================================================
TEST CONFIGURATION
================================================================================

Test format: parquet
Test name: usertable
Iterator mode: volume

================================================================================
✅ TEST COMPLETED SUCCESSFULLY
================================================================================

📊 Summary:
   Source records: 2
   Target records: 2
   Primary keys detected: ['ycsb_key']
   ✅ Data consistency verified
```

## Next Steps

### Test Different Modes

**Azure Parquet Mode (Production):**
```bash
cp config_examples/azure_parquet_mode.json .env/cockroachdb_pipelines.json
```
Update notebook: `ITERATOR_MODE = ConnectorMode.AZURE_PARQUET`

**Azure JSON Mode (Debugging):**
```bash
cp config_examples/azure_json_mode.json .env/cockroachdb_pipelines.json
```
Update notebook: `ITERATOR_MODE = ConnectorMode.AZURE_JSON`

### Test CDC Operations

In CockroachDB, run:
```sql
-- UPDATE
UPDATE usertable SET field0 = 'updated_value' WHERE ycsb_key = 1;

-- DELETE
DELETE FROM usertable WHERE ycsb_key = 2;

-- INSERT
INSERT INTO usertable VALUES (3, 'value_3_0', 'value_3_1', 'value_3_2');
```

Re-run the notebook to see CDC events processed.

## Troubleshooting

### "Configuration files not found"
- Ensure `.env/cockroachdb_credentials.json` and `.env/cockroachdb_pipelines.json` exist
- Check file paths are correct

### "Cannot connect to CockroachDB"
- Verify credentials in `.env/cockroachdb_credentials.json`
- Test connection: `psql "postgresql://user:password@host:26257/defaultdb?sslmode=verify-full"`

### "Volume not found"
- Create volume: `CREATE VOLUME main.your_schema.cdc_files;`
- Verify permissions: `SHOW GRANTS ON VOLUME main.your_schema.cdc_files;`

### "No CDC files found"
- Check changefeed is running: `SHOW CHANGEFEED JOBS;`
- Verify Azure storage path matches configuration
- Wait a few seconds for changefeed to emit files

## Support

- 📖 [Full Documentation](README.md)
- 🐛 [Report Issues](https://github.com/rsleedbx/crdb_to_dbx/issues)
- 💬 [Discussions](https://github.com/rsleedbx/crdb_to_dbx/discussions)

## What's Next?

- [Configuration Guide](config_examples/README.md) - Explore all connector modes
- [Blog Post](docs/stream-changefeed-to-databricks-azure.md) - Learn CDC concepts
- [Notebook Guide](notebooks/README.md) - Advanced notebook usage
