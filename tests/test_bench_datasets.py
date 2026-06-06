"""Unit tests for the bench dataset adapter seam: registry resolution, SIFT specifics, and the synthetic adapter."""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pytest

from bench.config import SIFT_BASE_COUNT, SIFT_DIM, SIFT_FILE_NAMES, BenchConfig, build_parser
from bench.corpus import build_vocabulary, row_text
from bench.datasets import (
    DATASET_ADAPTERS,
    DatasetAdapter,
    Sift1mAdapter,
    SyntheticAdapter,
    adapter_for,
    register_adapter,
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

    def test_synthetic_is_registered(self) -> None:
        """The built-in synthetic adapter resolves by name."""
        adapter: DatasetAdapter = adapter_for(config_for(["prepare", "--dataset", "synthetic"]))
        assert isinstance(adapter, SyntheticAdapter)

    def test_unknown_dataset_raises(self) -> None:
        """An unregistered name raises a ValueError listing what is registered."""
        with pytest.raises(ValueError, match="unknown dataset 'nope'.*sift1m"):
            adapter_for(config_for(["prepare", "--dataset", "nope"]))

    def test_register_and_resolve_fake(self) -> None:
        """A freshly registered in-memory fake adapter resolves through --dataset."""
        fake: SyntheticAdapter = SyntheticAdapter(dataset_name="fake-tiny", vector_dimension=8, base_rows=64)
        register_adapter(fake)
        try:
            assert adapter_for(config_for(["prepare", "--dataset", "fake-tiny"])) is fake
        finally:
            DATASET_ADAPTERS.pop("fake-tiny", None)


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
        """The default text hook reproduces the synthetic cluster-seeded corpus exactly."""
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


class TestSyntheticAdapter:
    """The synthetic adapter is deterministic, consistent across slicing, and download-free."""

    def test_slices_match_full_matrix(self, tmp_path: Path) -> None:
        """Any slice equals the corresponding rows of the full matrix."""
        adapter: SyntheticAdapter = SyntheticAdapter(vector_dimension=8, base_rows=100, seed=3)
        full: np.ndarray = adapter.base_vectors(tmp_path)
        assert full.shape == (100, 8)
        assert full.dtype == np.float32
        np.testing.assert_array_equal(adapter.base_vector_slice(tmp_path, 40, 25), full[40:65])
        np.testing.assert_array_equal(adapter.base_vectors(tmp_path, limit=10), full[:10])

    def test_deterministic_across_instances(self, tmp_path: Path) -> None:
        """Two instances with the same seed generate identical corpora."""
        first: SyntheticAdapter = SyntheticAdapter(vector_dimension=8, base_rows=50, query_rows=5, seed=9)
        second: SyntheticAdapter = SyntheticAdapter(vector_dimension=8, base_rows=50, query_rows=5, seed=9)
        np.testing.assert_array_equal(first.base_vectors(tmp_path), second.base_vectors(tmp_path))
        np.testing.assert_array_equal(first.query_vectors(tmp_path), second.query_vectors(tmp_path))

    def test_no_published_ground_truth_and_no_download(self, tmp_path: Path) -> None:
        """Ground truth is None (brute force) and download is a recorded no-op."""
        adapter: SyntheticAdapter = SyntheticAdapter()
        assert adapter.ground_truth(tmp_path) is None
        payload: dict[str, object] = adapter.download(tmp_path)
        assert payload["skipped"] is True

    def test_gt_depth_clamped_to_corpus(self) -> None:
        """The brute-force depth never exceeds the corpus size."""
        assert SyntheticAdapter(base_rows=30).gt_depth == 30
        assert SyntheticAdapter(base_rows=5_000).gt_depth == 100
