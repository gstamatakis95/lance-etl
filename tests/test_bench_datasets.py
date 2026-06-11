"""Unit tests for the bench dataset adapter seam: registry resolution, SIFT specifics, and the bigann adapter."""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pytest

from bench.bigann_io import write_u8bin
from bench.config import SIFT_BASE_COUNT, SIFT_DIM, SIFT_FILE_NAMES, BenchConfig, build_parser
from bench.corpus import build_vocabulary, row_text
from bench.datasets import (
    BigannAdapter,
    DatasetAdapter,
    Sift1mAdapter,
    adapter_for,
)


def config_for(argv: list[str]) -> BenchConfig:
    """Parse an argument vector into a BenchConfig.

    Args:
        argv: The argument vector after the program name.

    Returns:
        The parsed configuration.
    """
    return BenchConfig.from_args(build_parser().parse_args(argv))


class TestRegistry:
    """The adapter registry resolves --dataset names."""

    def test_sift1m_is_default(self) -> None:
        """A bare command resolves the SIFT1M adapter."""
        adapter: DatasetAdapter = adapter_for(config_for(["prepare"]))
        assert isinstance(adapter, Sift1mAdapter)
        assert adapter.name == "sift1m"

    def test_bigann_is_registered(self) -> None:
        """The built-in bigann adapter resolves by name."""
        adapter: DatasetAdapter = adapter_for(config_for(["prepare", "--dataset", "bigann"]))
        assert isinstance(adapter, BigannAdapter)

    def test_unknown_dataset_raises(self) -> None:
        """An unregistered name raises a ValueError listing what is registered."""
        with pytest.raises(ValueError, match="unknown dataset 'nope'.*sift1m"):
            adapter_for(config_for(["prepare", "--dataset", "nope"]))

    def test_bigann_limit_bound_from_config(self) -> None:
        """adapter_for binds the configured --limit onto the returned BigannAdapter."""
        resolved: DatasetAdapter = adapter_for(config_for(["prepare", "--dataset", "bigann", "--limit", "50000"]))
        assert isinstance(resolved, BigannAdapter)
        assert resolved.limit == 50_000


class TestSift1mAdapter:
    """The SIFT1M adapter carries the published corpus facts."""

    def test_properties(self) -> None:
        """Dimension, count, metric, and ground-truth labels match SIFT1M."""
        adapter: Sift1mAdapter = Sift1mAdapter()
        assert adapter.dimension == SIFT_DIM
        assert adapter.base_count == SIFT_BASE_COUNT
        assert adapter.metric == "L2"
        assert adapter.gt_depth == 100
        assert adapter.ground_truth_source == "ivecs"

    def test_default_text_hook_delegates_to_corpus(self) -> None:
        """The default text hook reproduces the cluster-seeded corpus exactly."""
        clusters, common = build_vocabulary(4, 10, 5, seed=7)
        adapter: Sift1mAdapter = Sift1mAdapter()
        assert adapter.text_for_row(clusters, common, 2, 1234, 7, 8) == row_text(
            clusters, common, 2, 1234, 7, cluster_terms=8
        )

    def test_checksum_manifest_round_trip(self, tmp_path: Path) -> None:
        """Recorded checksums verify until a corpus file changes."""
        adapter: Sift1mAdapter = Sift1mAdapter()
        for name in SIFT_FILE_NAMES:
            (tmp_path / name).write_bytes(name.encode())
        digests: dict[str, str] = adapter.record_checksums(tmp_path)
        assert set(digests) == set(SIFT_FILE_NAMES)
        assert adapter.verify_recorded_checksums(tmp_path) is True
        (tmp_path / SIFT_FILE_NAMES[0]).write_bytes(b"tampered")
        assert adapter.verify_recorded_checksums(tmp_path) is False


class TestBigannAdapter:
    """The bigann adapter reads u8bin files and supports limit-scoped IO."""

    def make_fixture(self, workspace: Path, limit: int, dim: int, seed: int) -> BigannAdapter:
        """Write tiny u8bin files and return a BigannAdapter bound to the limit.

        Args:
            workspace: The workspace directory.
            limit: Number of base vectors.
            dim: Vector dimension.
            seed: RNG seed.

        Returns:
            A BigannAdapter with the given limit.
        """
        adapter: BigannAdapter = BigannAdapter(limit=limit)
        rng: np.random.Generator = np.random.default_rng(seed)
        base: np.ndarray = rng.integers(0, 256, size=(limit, dim), dtype=np.uint8).astype(np.float32)
        queries: np.ndarray = rng.integers(0, 256, size=(10, dim), dtype=np.uint8).astype(np.float32)
        write_u8bin(adapter.base_path(workspace), base)
        write_u8bin(adapter.query_path(workspace), queries)
        return adapter

    def test_slices_match_full_matrix(self, tmp_path: Path) -> None:
        """Any slice equals the corresponding rows of the full base matrix."""
        adapter: BigannAdapter = self.make_fixture(tmp_path, limit=100, dim=128, seed=3)
        full: np.ndarray = adapter.base_vectors(tmp_path)
        assert full.shape == (100, 128)
        assert full.dtype == np.float32
        np.testing.assert_array_equal(adapter.base_vector_slice(tmp_path, 40, 25), full[40:65])
        np.testing.assert_array_equal(adapter.base_vectors(tmp_path, limit=10), full[:10])

    def test_download_short_circuits_when_files_present(self, tmp_path: Path) -> None:
        """Download returns skipped when both base and query u8bin files already exist."""
        adapter: BigannAdapter = self.make_fixture(tmp_path, limit=50, dim=128, seed=7)
        payload: dict[str, object] = adapter.download(tmp_path)
        assert payload["skipped"] is True

    def test_no_gt_for_non_million_limit(self, tmp_path: Path) -> None:
        """Ground truth returns None when the limit is not a published million-prefix size."""
        adapter: BigannAdapter = BigannAdapter(limit=2_000)
        assert adapter.ground_truth(tmp_path) is None

    def test_text_hook_delegates_to_corpus(self) -> None:
        """The text hook delegates to the cluster corpus generator."""
        clusters, common = build_vocabulary(4, 10, 5, seed=7)
        adapter: BigannAdapter = BigannAdapter()
        result: str = adapter.text_for_row(clusters, common, 2, 1234, 7, 8)
        assert result == row_text(clusters, common, 2, 1234, 7, cluster_terms=8)
