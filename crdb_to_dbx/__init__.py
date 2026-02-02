"""
CockroachDB to Databricks CDC Connector

A lightweight connector for streaming CockroachDB changefeeds to Databricks Delta Lake.
"""

from .connector import (
    ConnectorMode,
    LakeflowConnect,
    load_crdb_config,
    load_and_merge_cdc_to_delta,
    parse_test_scenario,
    cleanup_test_checkpoint,
)

__version__ = "1.0.0"
__all__ = [
    "ConnectorMode",
    "LakeflowConnect",
    "load_crdb_config",
    "load_and_merge_cdc_to_delta",
    "parse_test_scenario",
    "cleanup_test_checkpoint",
]
