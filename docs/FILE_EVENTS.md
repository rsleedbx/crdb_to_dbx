# Managed File Events for CockroachDB CDC

*This document covers Databricks Auto Loader managed file events in the context of CockroachDB CDC pipelines. It is intended for the `file-events` branch and should be validated with testing before merging.*

---

## What are managed file events?

**Managed file events** (enabled via `cloudFiles.useManagedFileEvents`) is Auto Loader’s event-driven file discovery mode. Instead of repeatedly listing the directory, Auto Loader uses a **file-events cache** populated by cloud storage notifications.

- **Cloud storage** (e.g. Azure Event Grid for ADLS Gen2) publishes notifications when files are created or updated.
- **Databricks file events** subscribes to those notifications and caches file metadata for the external location.
- **Auto Loader** reads from that cache (and the stream checkpoint) to discover new files incrementally.

Source: [Auto Loader with file events overview](https://docs.databricks.com/en/ingestion/cloud-object-storage/auto-loader/file-events-explained.html).

---

## How file events help CockroachDB CDC

| Concern | With directory listing | With file events |
|--------|------------------------|-------------------|
| **Finding new Parquet files** | Auto Loader lists the path on each run; cost and latency grow with directory size. | New files are discovered via the cache as they land; no repeated full listing. |
| **Discovering .RESOLVED files** | Listing is used to see when a `.RESOLVED` file appears for watermarking. | The same cache makes new `.RESOLVED` (and Parquet) files visible quickly. |
| **Scale** | Large directories (e.g. many partitions or long-running changefeeds) make listing slow. | File events support much higher file throughput (see [Databricks docs](https://docs.databricks.com/en/ingestion/cloud-object-storage/auto-loader/file-events-explained.html)). |

File events do **not** change the meaning of “wait for RESOLVED”: the connector still uses `.RESOLVED` for watermarks and filters by `__crdb__updated <= resolved_ts`. File events only make **discovery** of those files faster and more scalable.

---

## Pros of using file events

- **Performance**: Subsequent runs avoid directory listing; discovery is from the cache. Databricks documents significantly better performance and scalability (e.g. ingestion of large numbers of files per hour).  
  [Auto Loader with file events overview](https://docs.databricks.com/en/ingestion/cloud-object-storage/auto-loader/file-events-explained.html)

- **Fewer cloud knobs**: Databricks can set up and manage notification resources (e.g. Event Grid, queues) when the storage credential has the right permissions.  
  [Auto Loader with file events overview](https://docs.databricks.com/en/ingestion/cloud-object-storage/auto-loader/file-events-explained.html)

- **One queue per external location**: A single notification queue serves all streams from that location, reducing the risk of hitting per-stream or per-bucket limits.  
  [Auto Loader with file events overview](https://docs.databricks.com/en/ingestion/cloud-object-storage/auto-loader/file-events-explained.html)

- **Resilience to missed events**: Databricks runs periodic full directory listings on the external location when file events are enabled, so missed or late notifications can be corrected.  
  [Auto Loader with file events overview – FAQs](https://docs.databricks.com/en/ingestion/cloud-object-storage/auto-loader/file-events-explained.html)

- **Volumes**: Databricks recommends using external volumes for optimal file discovery when using cloud storage.  
  [Auto Loader with file events overview – Best practices](https://docs.databricks.com/en/ingestion/cloud-object-storage/auto-loader/file-events-explained.html)

---

## Cons and limitations

- **7-day read position**: If a stream does not run for **more than 7 days**, the stored read position in the file-events cache becomes invalid and Auto Loader must perform a full directory listing again. For infrequent or ad-hoc pipelines, the benefit is smaller and you have an extra failure mode.  
  [Auto Loader with file events overview](https://docs.databricks.com/en/ingestion/cloud-object-storage/auto-loader/file-events-explained.html)

- **Azure permissions**: Enabling file events on an external location backed by ADLS Gen2 requires the storage credential (e.g. managed identity) to have roles such as **Storage Account Contributor** and **EventGrid EventSubscription Contributor**. Many users do not have permission to assign these; only cloud/tenant admins often do.

- **First run**: The first run with file events still does a full directory listing to establish a valid read position in the cache and store it in the checkpoint.  
  [Auto Loader with file events overview](https://docs.databricks.com/en/ingestion/cloud-object-storage/auto-loader/file-events-explained.html)

- **When directory listing is still used**: Auto Loader falls back to full directory listing when, for example: starting a new stream, migrating from directory listing or legacy notifications, changing the load path, not running for more than 7 days, or when the external location is changed in a way that invalidates the read position (e.g. file events turned off and on again).  
  [Auto Loader with file events overview](https://docs.databricks.com/en/ingestion/cloud-object-storage/auto-loader/file-events-explained.html)

- **Path rewrites**: Path rewrites are not supported with file events.  
  [Auto Loader with file events overview – Limitations](https://docs.databricks.com/en/ingestion/cloud-object-storage/auto-loader/file-events-explained.html)

- **More moving parts**: Event Grid, queues, and the file-events service add operational surface. Misconfiguration (e.g. permissions, queue) can lead to missed files until the periodic full listing runs. Debugging is harder than with “credential can list path” alone.

- **Platform and location support**: Support for external volumes, Serverless, and specific clouds can change. Check the current [Databricks file events documentation](https://docs.databricks.com/en/ingestion/cloud-object-storage/auto-loader/file-events-explained.html) and [file event limitations](https://docs.databricks.com/en/connect/unity-catalog/cloud-storage/manage-external-locations.html#file-event-limitations) for your environment.

---

## Why this repo does not enable file events by default

- **Permissions**: Enabling file events requires broad Azure RBAC (e.g. Storage Account Contributor, EventGrid at resource group scope). The setup script is written so it works with minimal permissions by default; file events are opt-in.

- **Compatibility**: Directory listing works with any credential that can read the container and does not depend on Event Grid or the 7-day rule. Defaulting to directory listing keeps the connector usable in the widest set of environments.

- **Explicit opt-in**: Enabling file events (uncommenting role assignment and using `ENABLE_FILE_EVENTS=1` in the script) makes it clear that extra permissions and behavior (e.g. 7-day requirement) are accepted.

---

## Enabling file events in this repo

On the **file-events** branch:

1. **Azure**: Uncomment `assign_managed_identity_roles` in `scripts/01_azure_storage.sh` (requires Owner or User Access Administrator on the storage account and resource group).
2. **Script**: Run the script with `ENABLE_FILE_EVENTS=1` so the external location is created or updated with `--enable-file-events`.
3. **Databricks**: Enable file events on the external location (script does this when `ENABLE_FILE_EVENTS=1`). In pipelines, use `cloudFiles.useManagedFileEvents` per [Databricks Auto Loader options](https://docs.databricks.com/en/ingestion/cloud-object-storage/auto-loader/options.html) and do not force `cloudFiles.useNotifications` to `false` for that path if your runtime supports file events.

Verification: Use the **Test Connection** on the external location in the Databricks UI; file events read should show a green check when set up correctly.  
[Auto Loader with file events overview – FAQs](https://docs.databricks.com/en/ingestion/cloud-object-storage/auto-loader/file-events-explained.html)

---

## References (public documentation only)

- [Auto Loader with file events overview](https://docs.databricks.com/en/ingestion/cloud-object-storage/auto-loader/file-events-explained.html)
- [File event limitations (external locations)](https://docs.databricks.com/en/connect/unity-catalog/cloud-storage/manage-external-locations.html#file-event-limitations)
- [Auto Loader options](https://docs.databricks.com/en/ingestion/cloud-object-storage/auto-loader/options.html)
- [Migrate to Auto Loader with file events](https://docs.databricks.com/en/ingestion/cloud-object-storage/auto-loader/migrating-to-file-events.html) (migration and compatibility)
