"""
CockroachDB to Databricks CDC Connector

This package provides utilities for streaming CockroachDB changefeeds to Databricks
using multiple cloud storage providers.
"""

# Azure Blob Storage utilities
from .cockroachdb_azure import (
    check_azure_files,
    wait_for_changefeed_files as wait_for_changefeed_files_azure
)

# Unity Catalog Volume utilities
from .cockroachdb_uc_volume import (
    check_volume_files,
    wait_for_changefeed_files as wait_for_changefeed_files_volume
)

# Auto Loader ingestion functions
from .cockroachdb_autoload import (
    ingest_cdc_append_only,
    ingest_cdc_with_merge,
    ingest_cdc_append_only_multi_family,
    ingest_cdc_with_merge_multi_family,
    merge_column_family_fragments
)

__all__ = [
    # Azure utilities
    'check_azure_files',
    'wait_for_changefeed_files_azure',
    
    # Volume utilities
    'check_volume_files',
    'wait_for_changefeed_files_volume',
    
    # Ingestion functions
    'ingest_cdc_append_only',
    'ingest_cdc_with_merge',
    'ingest_cdc_append_only_multi_family',
    'ingest_cdc_with_merge_multi_family',
    'merge_column_family_fragments',
]

__version__ = '0.1.0'
