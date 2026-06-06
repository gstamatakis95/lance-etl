"""Tests for deferred index remap during compaction, pure Lance.

Pins the frag-reuse-index lifecycle on the small tier: deferral records a ``__lance_frag_reuse`` system index on a plain
dataset without stable row ids, and reads stay correct through lazy remapping with no extra call. Also pins the tier
scoping of ``defer_index_remap`` in the option builders.
"""

from __future__ import annotations

from pathlib import Path

import lance
import pyarrow as pa

from lance_etl.compaction import CompactionConfig, compact_small_dataset
from lance_etl.telemetry import Telemetry, TelemetryConfig

ROWS: int = 6000
ROWS_PER_FRAGMENT: int = 1000


def write_indexed_dataset(tmp_path: Path) -> str:
    """Write a plain fragmented dataset with a BTREE index and deletions.

    No stable row ids are enabled, matching the lance reference coverage for deferred remap on default datasets.

    Args:
        tmp_path: Pytest-provided temporary directory.

    Returns:
        The dataset URI.
    """
    uri: str = str(tmp_path / "fri.lance")
    table: pa.Table = pa.table(
        {
            "id": pa.array(range(ROWS), pa.int64()),
            "payload": pa.array([f"row{i}" for i in range(ROWS)]),
        }
    )
    dataset: lance.LanceDataset = lance.write_dataset(table, uri, max_rows_per_file=ROWS_PER_FRAGMENT)
    dataset.create_scalar_index("id", "BTREE")
    dataset.delete("id < 500")
    return uri


def fri_config(telemetry_config: TelemetryConfig) -> CompactionConfig:
    """Build the compaction configuration used by the FRI tests.

    Args:
        telemetry_config: The test telemetry configuration.

    Returns:
        A small-tier configuration with deferred index remap enabled.
    """
    return CompactionConfig(
        telemetry=telemetry_config,
        target_rows_per_fragment=2000,
        defer_index_remap=True,
        num_threads=1,
        run_cleanup=False,
        commit_backoff_seconds=0.0,
    )


def test_small_tier_defer_index_remap_records_frag_reuse_index(
    tmp_path: Path, telemetry_config: TelemetryConfig, telemetry: Telemetry
) -> None:
    """Deferred remap on the small tier produces a frag-reuse system index."""
    uri: str = write_indexed_dataset(tmp_path)
    result: dict[str, object] = compact_small_dataset(uri, fri_config(telemetry_config), telemetry)
    assert result["fragments_removed"] > 0
    names: list[str] = [description.name for description in lance.dataset(uri).describe_indices()]
    assert any(name == "__lance_frag_reuse" for name in names)


def test_reads_stay_correct_after_deferred_remap(
    tmp_path: Path, telemetry_config: TelemetryConfig, telemetry: Telemetry
) -> None:
    """Lazy FRI remapping keeps indexed reads correct with no explicit step."""
    uri: str = write_indexed_dataset(tmp_path)
    compact_small_dataset(uri, fri_config(telemetry_config), telemetry)
    dataset: lance.LanceDataset = lance.dataset(uri)
    result: pa.Table = dataset.to_table(filter="id = 700")
    assert result.num_rows == 1
    assert result["payload"].to_pylist() == ["row700"]
    assert dataset.to_table(filter="id = 100").num_rows == 0
    assert dataset.count_rows() == ROWS - 500


def test_defer_index_remap_scoped_to_execute_options(telemetry_config: TelemetryConfig) -> None:
    """The deferral flag reaches execute options but is dropped at plan time.

    The distributed commit binding compacts with default options, so the flag is only honored where
    ``Compaction.execute`` parses the full option set.
    """
    config: CompactionConfig = fri_config(telemetry_config)
    assert config.execute_options()["defer_index_remap"] is True
    assert "defer_index_remap" not in config.plan_options()
