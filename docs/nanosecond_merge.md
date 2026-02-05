# CDC timestamp: use raw `__crdb__updated` (HLC string)

This document summarizes the choice to **keep the raw CockroachDB CDC timestamp column `__crdb__updated`** (HLC string) for ordering, MERGE, and display. No derived bigint column; no legacy support or fallback.

## Fixes in this branch (4)

1. **Full HLC for merge, ordering, and display**  
   Use `__crdb__updated` as the single timestamp column (format `"WallTime.Logical"`). MERGE and watermark filter use full HLC string comparison; display as-is. No `_cdc_timestamp_nanos`; target must have `__crdb__updated` or we fail.

2. **RESOLVED filename: single format, fail on error**  
   Accept only CockroachDB’s 33-digit RESOLVED filename format. Invalid or unexpected filenames raise `ValueError`; no silent skip or fallback. Ensures watermarks are correct and code stays simple.

3. **Checkpoint path: default in config, fail if missing**  
   Checkpoint base path is no longer resolved via `DESCRIBE SCHEMA`; the default is built in `cockroachdb_config.process_config` as `/Volumes/{destination_catalog}/{destination_schema}/checkpoints`. In autoload, if `config.cdc_config.checkpoint_base_path` is missing or empty, we raise. Config is the single place that defines where checkpoints go.

4. **MERGE clause order: whenMatchedDelete first**  
   Delta MERGE uses first-matching-clause semantics. Use **whenMatchedDelete** before **whenMatchedUpdate**, then **whenNotMatchedInsert**. Otherwise a matched row with `_cdc_operation = 'DELETE'` can be updated with the DELETE payload instead of removed (target keeps the row with `_cdc_operation = 'DELETE'`). Order in code: `whenMatchedDelete(condition="source._cdc_operation = 'DELETE'")` → `whenMatchedUpdate(...)` → `whenNotMatchedInsert(...)`.

---

## Overview

CockroachDB changefeeds emit an HLC (Hybrid Logical Clock) timestamp per row in the column `__crdb__updated`. We:

1. **Keep** `__crdb__updated` as-is (string, format `"WallTime.Logical"`, e.g. `"1770067697320026017.0000000002"`).
2. **Use** it for ordering, MERGE conditions, and deduplication; lexicographic order matches HLC order.
3. **Display** in SQL/BI: use `__crdb__updated` as-is (full HLC string) to avoid confusion; do not drop the logical part.

No separate `_cdc_timestamp_nanos` column; no ALTER for existing tables. New implementation only.

---

## CockroachDB timestamp formats

| Source | Format | Example |
|--------|--------|---------|
| **Parquet / JSON row** `__crdb__updated` | `hlc.Timestamp.AsOfSystemTime()` → `"WallTime.Logical"` | `"1770067697320026017.0000000002"` |
| **RESOLVED filename** | `cloudStorageFormatTime(ts)` → 33 digits only | `YYYYMMDDHHMMSS` (14) + 9-digit nanos + 10-digit logical |

Both represent the same HLC type. Row `__crdb__updated` is the canonical HLC string; RESOLVED filenames use a single encoding (33 digits). For watermark filtering we convert the RESOLVED filename to the same `"WallTime.Logical"` string and compare with `__crdb__updated` (full HLC string comparison; lexicographic order = HLC order).

**Source verification:** CockroachDB repo: `pkg/ccl/changefeedccl/parquet.go` (row uses `updated.AsOfSystemTime()`), `pkg/util/hlc/timestamp.go` (`AsOfSystemTime()` → `"%d.%010d", WallTime, Logical`), `pkg/ccl/changefeedccl/sink_cloudstorage.go` (`cloudStorageFormatTime()` — single format, 33 chars).

---

## Design choices

### 1. Keep `__crdb__updated` (no derived column)

- **Choice:** Do not add `_cdc_timestamp_nanos`; do not drop `__crdb__updated`. Transform only adds `_cdc_operation` and drops `__crdb__event_type`.
- **Reason:** Full HLC precision in one column; string comparison gives correct ordering; no extra column to maintain.

### 2. MERGE and ordering

- **Choice:** MERGE condition: `target.__crdb__updated IS NULL OR source.__crdb__updated > target.__crdb__updated`. Order by `__crdb__updated` (e.g. `.desc()` for latest).
- **Clause order:** Use **whenMatchedDelete** first, then **whenMatchedUpdate**, then **whenNotMatchedInsert**. Delta applies the first matching clause; putting DELETE first ensures matched rows with `_cdc_operation = 'DELETE'` are removed, not updated with the DELETE payload.
- **Reason:** Lexicographic order of `"WallTime.Logical"` (zero-padded logical) matches HLC order.

### 3. Display

- **Choice:** In SQL or BI, display `__crdb__updated` as-is (full HLC string). Do not drop the logical part (would cause confusion).
- **Reason:** Same column used for merge and display; full HLC preserves ordering semantics.

### 4. Watermark filter (full HLC)

- **Choice:** RESOLVED watermark is an HLC string in the same format as `__crdb__updated` (`"WallTime.Logical"`). Filter: `__crdb__updated <= watermark_str` (string comparison).
- **Parsing:** We accept only CockroachDB’s 33-digit RESOLVED filename format (`cloudStorageFormatTime`). We convert each to `"WallTime.Logical"` and use the max. Any other filename format raises `ValueError` (fail-fast).
- **Reason:** Both sides are full HLC strings; lexicographic order = HLC order; single format keeps implementation correct and simple.

### 5. No legacy / no ALTER

- **Choice:** Target table must have `__crdb__updated`; we fail if missing. No ALTER to add columns.
- **Reason:** New implementation only; no backward compatibility with old schemas.

### 6. RESOLVED filename: single format, fail on error

- **Choice:** Support only CockroachDB’s 33-digit RESOLVED filename format (`YYYYMMDDHHMMSS` + 9 nanos + 10 logical). Invalid or unexpected filenames raise `ValueError`; no silent skip or fallback.
- **Reason:** CockroachDB’s cloud storage sink uses a single format (`cloudStorageFormatTime()` in `sink_cloudstorage.go`); supporting multiple formats added complexity and risk. Fail-fast avoids silent wrong watermarks.
- **Pros:** Simpler code; matches upstream; bad data fails loudly.
- **Cons:** Any non–CockroachDB RESOLVED naming (e.g. custom 18–19 digit) is rejected; operators must fix filenames or use CockroachDB’s format.

---

## Pros

- **Full HLC precision** (wall + logical) in one column; no loss from dropping logical.
- **Single source of truth:** One column for merge, order, and display (display via expression).
- **Simpler transform:** No derived column; keep raw CDC column.
- **Works in DLT and notebooks:** Transform remains lazy.

---

## Cons

- **Display:** Use `__crdb__updated` as-is (full HLC); do not drop logical in display to avoid confusion.
- **String type:** Some systems prefer a numeric column; we keep string for full HLC and correct ordering.

---

## Summary

- **Column:** `__crdb__updated` (string, HLC format `WallTime.Logical`). Used for ordering, MERGE, and display (show as-is, full HLC).
- **MERGE:** Full HLC string comparison: `source.__crdb__updated > target.__crdb__updated`; lexicographic order = HLC order. Clause order: **whenMatchedDelete** → **whenMatchedUpdate** → **whenNotMatchedInsert** (so DELETEs remove rows, not update them).
- **Watermark:** RESOLVED filenames parsed as 33-digit CockroachDB format only; converted to `"WallTime.Logical"`; filter `__crdb__updated <= watermark_str`. Invalid RESOLVED filenames raise.
- **No legacy:** Target must have `__crdb__updated`; fail if missing.
