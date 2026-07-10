"""Unit tests for the size-aware IVF partition policy and the vector row floor."""

from __future__ import annotations

from pathlib import Path

import lance
from conftest import make_vector_table, write_fragmented_dataset

from lance_etl.indexing import (
    IndexJobConfig,
    VectorIndexHandler,
    degrade_num_partitions,
    derive_num_partitions,
    plan_dataset_indexes,
)
from lance_etl.indexing.config import MAX_IVF_PARTITIONS, MIN_IVF_PARTITIONS, TARGET_ROWS_PER_IVF_PARTITION
from lance_etl.telemetry import Telemetry, TelemetryConfig


def test_derive_clamps_to_floor() -> None:
    """Tiny datasets clamp to the MIN_IVF_PARTITIONS floor."""
    assert derive_num_partitions(rows=4, configured=None) == MIN_IVF_PARTITIONS
    assert derive_num_partitions(rows=0, configured=None) == MIN_IVF_PARTITIONS


def test_derive_clamps_to_cap() -> None:
    """Huge datasets clamp to MAX_IVF_PARTITIONS."""
    assert derive_num_partitions(rows=1_000_000_000, configured=None) == MAX_IVF_PARTITIONS


def test_derive_uses_target_rows_per_partition_in_between() -> None:
    """Mid-sized datasets use rows // TARGET_ROWS_PER_IVF_PARTITION."""
    assert derive_num_partitions(rows=1_000_000, configured=None) == 1_000_000 // TARGET_ROWS_PER_IVF_PARTITION
    assert derive_num_partitions(rows=10_000, configured=None) == MIN_IVF_PARTITIONS


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
        vector_columns=["vector"],
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
        vector_columns=["vector"],
        vector_min_rows=100,
    )
    handler: VectorIndexHandler = VectorIndexHandler(config, "vector", "vector_idx")
    assert handler.skip_reason(lance.dataset(uri)) is None


def test_plan_skips_vector_below_floor(tmp_path: Path, telemetry: Telemetry) -> None:
    """The plan phase records the skip for a below-floor vector index and builds no shards for it."""
    uri: str = str(tmp_path / "floor.lance")
    write_fragmented_dataset(uri, make_vector_table(rows=200, dim=8), max_rows_per_file=100)
    config: IndexJobConfig = IndexJobConfig(
        telemetry=TelemetryConfig(),
        vector_columns=["vector"],
        vector_min_rows=50_000,
        scalar_columns=["id"],
    )
    plan: dict[str, object] = plan_dataset_indexes(uri, config, telemetry)
    by_index: dict[str, dict[str, object]] = {item["index"]: item for item in plan["done"]}
    assert "skipped" in by_index["vector_idx"]
    assert all(spec["index_name"] != "vector_idx" for spec in plan["specs"])
    assert any(spec["index_name"] == "id_idx" for spec in plan["specs"])
