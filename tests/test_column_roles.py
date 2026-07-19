"""Column-role metadata: pivot role capture, grow-only config persistence, and sink integration.

The ETL pivot records each created column's role (vector, text, or scalar, keyed by its source
map) and the sink persists the roles into the dataset's ``lance-etl.columns`` config entry.
These tests pin the role capture in ``pivot_map_columns``, the grow-only merge semantics of
``merge_column_roles``, the format-2.1 bootstrap, and the end-to-end sink write via
``apply_merge``.
"""

from __future__ import annotations

from pathlib import Path

import lance
import pyarrow as pa

from lance_etl.column_roles import (
    COLUMN_ROLES_KEY,
    load_column_roles,
    merge_column_roles,
)
from lance_etl.etl import ETLConfig, apply_merge, dataset_uri, pivot_map_columns
from lance_etl.telemetry import Telemetry, TelemetryConfig

ROUTING_KEY: tuple[str, str, str] = ("org1", "tenant1", "ns1")


def make_map_group(keys: list[str]) -> pa.Table:
    """Build one routing group carrying all three source map columns.

    Args:
        keys: Record ids for the rows.

    Returns:
        A routed ETL group with ``vectors``, ``texts``, and ``metadata`` maps.
    """
    count: int = len(keys)
    vector_type: pa.DataType = pa.map_(pa.string(), pa.list_(pa.float32()))
    string_map: pa.DataType = pa.map_(pa.string(), pa.string())
    return pa.table(
        {
            "record_id": pa.array(keys, pa.string()),
            "org_id": pa.array([ROUTING_KEY[0]] * count),
            "tenant_id": pa.array([ROUTING_KEY[1]] * count),
            "namespace": pa.array([ROUTING_KEY[2]] * count),
            "op": pa.array(["insert"] * count),
            "vectors": pa.array([[("embedding", [1.0] * 8)]] * count, vector_type),
            "texts": pa.array([[("body", "hello world")]] * count, string_map),
            "metadata": pa.array([[("category", "a")]] * count, string_map),
        }
    )


def test_pivot_reports_roles_by_source_map(telemetry_config: TelemetryConfig) -> None:
    """Each pivoted column's role is its source map: vectors, texts, and metadata respectively."""
    config: ETLConfig = ETLConfig(base_uri="/tmp/unused", telemetry=telemetry_config)
    pivoted, counts, roles = pivot_map_columns(
        make_map_group(["a"]).select([c for c in make_map_group(["a"]).column_names if c != "op"]), config
    )
    assert roles == {"embedding": "vector", "body": "text", "category": "scalar"}
    assert "embedding" in pivoted.column_names
    assert counts == {}


def test_merge_column_roles_grow_only_and_idempotent(tmp_path: Path) -> None:
    """Merging roles adds new entries, never reassigns stored ones, and replays converge."""
    uri: str = str(tmp_path / "roles.lance")
    lance.write_dataset(pa.table({"x": pa.array([1])}), uri, data_storage_version="2.1")

    merge_column_roles(uri, {"embedding": "vector"}, None, retries=3, backoff_seconds=0.01)
    merge_column_roles(uri, {"embedding": "scalar", "body": "text"}, None, retries=3, backoff_seconds=0.01)
    merge_column_roles(uri, {"body": "text"}, None, retries=3, backoff_seconds=0.01)

    stored: dict[str, str] = load_column_roles(lance.dataset(uri))
    assert stored == {"embedding": "vector", "body": "text"}


def test_load_column_roles_absent_and_malformed(tmp_path: Path) -> None:
    """A dataset without the key or with a malformed value yields an empty mapping."""
    uri: str = str(tmp_path / "empty.lance")
    dataset: lance.LanceDataset = lance.write_dataset(pa.table({"x": pa.array([1])}), uri)
    assert load_column_roles(dataset) == {}
    dataset.update_config({COLUMN_ROLES_KEY: "not json"})
    assert load_column_roles(lance.dataset(uri)) == {}


def test_apply_merge_persists_roles_and_format(
    tmp_path: Path, telemetry_config: TelemetryConfig, telemetry: Telemetry
) -> None:
    """The sink bootstraps with format 2.1 and records every pivoted column's role."""
    config: ETLConfig = ETLConfig(base_uri=str(tmp_path), telemetry=telemetry_config)
    upserted, deleted = apply_merge(config, telemetry, ROUTING_KEY, make_map_group(["a", "b"]))
    assert (upserted, deleted) == (2, 0)

    dataset: lance.LanceDataset = lance.dataset(dataset_uri(config, *ROUTING_KEY))
    assert dataset.data_storage_version == "2.1"
    assert load_column_roles(dataset) == {"embedding": "vector", "body": "text", "category": "scalar"}
