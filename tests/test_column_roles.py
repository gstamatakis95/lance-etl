"""Column-role metadata loading and grow-only persistence."""

from __future__ import annotations

from pathlib import Path

import lance
import pyarrow as pa

from lance_etl.column_roles import (
    COLUMN_ROLES_KEY,
    load_column_roles,
    merge_column_roles,
)


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
