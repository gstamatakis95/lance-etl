"""Unit tests for the size-aware IVF partition policy and the vector row floor."""

from __future__ import annotations

from pathlib import Path

import lance
import pytest
from conftest import make_vector_table, write_fragmented_dataset

from lance_etl.indexing import (
    MAX_IVF_PARTITIONS,
    MIN_IVF_PARTITIONS,
    IndexJobConfig,
    VectorIndexHandler,
    degrade_num_partitions,
    derive_num_partitions,
    index_dataset_locally,
)
from lance_etl.telemetry import TelemetryConfig


def test_derive_clamps_to_floor() -> None:
    """Tiny datasets clamp to the 16-partition floor."""
    assert derive_num_partitions(rows=4, configured=None) == MIN_IVF_PARTITIONS
    assert derive_num_partitions(rows=0, configured=None) == MIN_IVF_PARTITIONS


def test_derive_clamps_to_cap() -> None:
    """Huge datasets clamp to the 4096-partition cap."""
    assert derive_num_partitions(rows=1_000_000_000, configured=None) == MAX_IVF_PARTITIONS


def test_derive_uses_sqrt_in_between() -> None:
    """Mid-sized datasets use round(sqrt(rows))."""
    assert derive_num_partitions(rows=1_000_000, configured=None) == 1000
    assert derive_num_partitions(rows=10_000, configured=None) == 100


def test_derive_configured_takes_precedence() -> None:
    """An explicit configuration overrides the derived value, even outside the clamp."""
    assert derive_num_partitions(rows=1_000_000, configured=8) == 8
    assert derive_num_partitions(rows=10, configured=10_000) == 10_000


def test_degrade_caps_at_supportable_rows() -> None:
    """The planned count degrades to rows // sample_rate when training data is short."""
    assert degrade_num_partitions(planned=256, rows=1024, sample_rate=256) == 4
    assert degrade_num_partitions(planned=4, rows=1024, sample_rate=256) == 4
    assert degrade_num_partitions(planned=2, rows=1024, sample_rate=256) == 2


def test_degrade_never_below_one() -> None:
    """The degraded count is at least 1 even with no trainable rows."""
    assert degrade_num_partitions(planned=16, rows=0, sample_rate=256) == 1


def test_vector_skip_reason_below_row_floor(tmp_path: Path) -> None:
    """The vector handler skips datasets below vector_min_rows."""
    uri: str = str(tmp_path / "small.lance")
    write_fragmented_dataset(uri, make_vector_table(rows=100, dim=8), max_rows_per_file=100)
    config: IndexJobConfig = IndexJobConfig(
        telemetry=TelemetryConfig(),
        vector_column="vector",
        vector_min_rows=50_000,
    )
    handler: VectorIndexHandler = VectorIndexHandler(config, "vector", "vector_idx")
    dataset: lance.LanceDataset = lance.dataset(uri)
    reason: str | None = handler.skip_reason(dataset)
    assert reason is not None
    assert "vector_min_rows" in reason


def test_vector_skip_reason_at_or_above_floor(tmp_path: Path) -> None:
    """The vector handler proceeds when the dataset meets the row floor."""
    uri: str = str(tmp_path / "ok.lance")
    write_fragmented_dataset(uri, make_vector_table(rows=100, dim=8), max_rows_per_file=100)
    config: IndexJobConfig = IndexJobConfig(
        telemetry=TelemetryConfig(),
        vector_column="vector",
        vector_min_rows=100,
    )
    handler: VectorIndexHandler = VectorIndexHandler(config, "vector", "vector_idx")
    assert handler.skip_reason(lance.dataset(uri)) is None


def test_index_dataset_locally_skips_vector_below_floor(tmp_path: Path) -> None:
    """The tier-A executor task records the skip and builds no vector index."""
    uri: str = str(tmp_path / "tier_a.lance")
    write_fragmented_dataset(uri, make_vector_table(rows=200, dim=8), max_rows_per_file=100)
    config: IndexJobConfig = IndexJobConfig(
        telemetry=TelemetryConfig(),
        vector_column="vector",
        vector_min_rows=50_000,
        scalar_columns=["id"],
    )
    result: dict[str, object] = index_dataset_locally(uri, config)
    assert result["tier"] == "small"
    by_index: dict[str, dict[str, object]] = {item["index"]: item for item in result["indexes"]}
    assert "skipped" in by_index["vector_idx"]
    dataset: lance.LanceDataset = lance.dataset(uri)
    names: list[str] = [item["name"] for item in dataset.list_indices()]
    assert "vector_idx" not in names
    assert "id_idx" in names


def test_validate_rejects_unsupported_num_bits(tmp_path: Path) -> None:
    """IVF_RQ validation rejects num_bits other than 1."""
    uri: str = str(tmp_path / "bits.lance")
    write_fragmented_dataset(uri, make_vector_table(rows=64, dim=8), max_rows_per_file=64)
    config: IndexJobConfig = IndexJobConfig(telemetry=TelemetryConfig(), vector_column="vector", num_bits=4)
    handler: VectorIndexHandler = VectorIndexHandler(config, "vector", "vector_idx")
    with pytest.raises(ValueError, match="num_bits"):
        handler.validate(lance.dataset(uri))
