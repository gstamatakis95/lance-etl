"""Pure unit tests for the clustered-rewrite assignment and bucketing helpers.

Covers the nearest-centroid assigner for every distance type (including a zero vector under cosine),
its agreement with Lance's own IVF centroids on a small real index, and the histogram-to-bucket
packing including oversized-partition salting and the null tail slot.
"""

from __future__ import annotations

from pathlib import Path

import lance
import numpy as np
import pyarrow as pa
import pytest
from conftest import make_vector_table, write_fragmented_dataset

from lance_etl.indexing import IndexJobConfig, bootstrap_vector_index
from lance_etl.maintenance.cluster import (
    assign_partition_ids,
    centroids_to_matrix,
    decode_centroids,
    derive_buckets,
    encode_centroids,
    partition_ids_for_batch,
)
from lance_etl.telemetry import Telemetry, TelemetryConfig


def fsl_from_rows(rows: list[list[float]]) -> pa.FixedSizeListArray:
    """Build a fixed-size-list float32 array from a list of equal-length rows.

    Args:
        rows: The vector rows, each of the same dimension.

    Returns:
        The fixed-size-list array carrying the rows.
    """
    dim: int = len(rows[0])
    flat: pa.Array = pa.array([value for row in rows for value in row], pa.float32())
    return pa.FixedSizeListArray.from_arrays(flat, dim)


def naive_nearest(vector: np.ndarray, centroids: np.ndarray, distance_type: str) -> float:
    """Return the best (nearest or most-similar) score of a vector over centroids naively.

    Args:
        vector: A single ``(dimension,)`` vector.
        centroids: The ``(num_partitions, dimension)`` centroid matrix.
        distance_type: One of ``l2``, ``cosine``, or ``dot``.

    Returns:
        The minimum L2 distance, the minimum cosine distance, or the maximum dot product.
    """
    if distance_type == "l2":
        return float(np.min(np.linalg.norm(centroids - vector, axis=1)))
    if distance_type == "cosine":
        vnorm: float = max(float(np.linalg.norm(vector)), 1e-12)
        cnorms: np.ndarray = np.maximum(np.linalg.norm(centroids, axis=1), 1e-12)
        sims: np.ndarray = (centroids @ vector) / (cnorms * vnorm)
        return float(np.min(1.0 - sims))
    return float(np.max(centroids @ vector))


def score_of_choice(vector: np.ndarray, centroid: np.ndarray, distance_type: str) -> float:
    """Return the score of a vector against the single centroid the assigner chose.

    Args:
        vector: A single ``(dimension,)`` vector.
        centroid: The chosen ``(dimension,)`` centroid.
        distance_type: One of ``l2``, ``cosine``, or ``dot``.

    Returns:
        The L2 distance, cosine distance, or dot product for the chosen centroid.
    """
    if distance_type == "l2":
        return float(np.linalg.norm(centroid - vector))
    if distance_type == "cosine":
        vnorm: float = max(float(np.linalg.norm(vector)), 1e-12)
        cnorm: float = max(float(np.linalg.norm(centroid)), 1e-12)
        return float(1.0 - (centroid @ vector) / (cnorm * vnorm))
    return float(centroid @ vector)


@pytest.mark.parametrize("distance_type", ["l2", "cosine", "dot"])
def test_assign_matches_naive_minimizer(distance_type: str) -> None:
    """The assigner picks a centroid whose true score ties the naive best for every metric."""
    rng: np.random.Generator = np.random.default_rng(11)
    centroids: np.ndarray = rng.standard_normal((16, 8)).astype(np.float32)
    rows: np.ndarray = rng.standard_normal((200, 8)).astype(np.float32)
    vectors: pa.FixedSizeListArray = pa.FixedSizeListArray.from_arrays(pa.array(rows.reshape(-1), pa.float32()), 8)
    pids: np.ndarray = assign_partition_ids(vectors, centroids, distance_type)
    assert pids.shape == (200,)
    assert pids.min() >= 0
    assert pids.max() < 16
    for index in range(rows.shape[0]):
        chosen: float = score_of_choice(rows[index], centroids[pids[index]], distance_type)
        target: float = naive_nearest(rows[index], centroids, distance_type)
        if distance_type == "dot":
            assert chosen >= target - 1e-4
        else:
            assert chosen <= target + 1e-4


def test_cosine_zero_vector_is_assigned_without_error() -> None:
    """A genuinely zero vector under cosine yields a valid partition id rather than NaN or a crash."""
    centroids: np.ndarray = np.array(
        [[1.0, 0.0, 0.0, 0.0], [0.0, 1.0, 0.0, 0.0], [0.0, 0.0, 1.0, 0.0]], dtype=np.float32
    )
    vectors: pa.FixedSizeListArray = fsl_from_rows([[0.0, 0.0, 0.0, 0.0], [0.9, 0.1, 0.0, 0.0]])
    pids: np.ndarray = assign_partition_ids(vectors, centroids, "cosine")
    assert pids.shape == (2,)
    assert 0 <= int(pids[0]) < 3
    assert int(pids[1]) == 0


def test_null_vector_routes_to_tail_partition() -> None:
    """A null vector is assigned the tail partition id ``num_partitions`` by the batch helper."""
    centroids: np.ndarray = np.array([[1.0, 0.0], [0.0, 1.0]], dtype=np.float32)
    vectors: pa.FixedSizeListArray = pa.array([[1.0, 0.0], [0.0, 1.0], None], pa.list_(pa.float32(), 2))
    pids: np.ndarray = partition_ids_for_batch(vectors, centroids, "l2", num_partitions=2)
    assert int(pids[0]) == 0
    assert int(pids[1]) == 1
    assert int(pids[2]) == 2


def test_encode_decode_centroids_roundtrip() -> None:
    """IPC encode then decode preserves the centroid values and the fixed-size-list type."""
    flat: pa.Array = pa.array([1.0, 2.0, 3.0, 4.0, 5.0, 6.0], pa.float32())
    centroids: pa.FixedSizeListArray = pa.FixedSizeListArray.from_arrays(flat, 3)
    restored: pa.Array = decode_centroids(encode_centroids(centroids))
    assert restored.type == centroids.type
    matrix: np.ndarray = centroids_to_matrix(restored)
    np.testing.assert_allclose(matrix, np.array([[1.0, 2.0, 3.0], [4.0, 5.0, 6.0]], dtype=np.float32))


def test_agreement_with_lance_centroids(tmp_path: Path, telemetry: Telemetry) -> None:
    """The assigner agrees with Lance's own IVF centroids on a small real index."""
    uri: str = str(tmp_path / "agreement.lance")
    write_fragmented_dataset(uri, make_vector_table(rows=512, dim=8), max_rows_per_file=128)
    config: IndexJobConfig = IndexJobConfig(
        telemetry=TelemetryConfig(),
        vector_columns=["vector"],
        num_partitions=8,
        vector_min_rows=1,
        commit_retries=5,
        commit_backoff_seconds=0.0,
    )
    bootstrap_vector_index(uri, "vector", "vector_idx", config, telemetry)
    dataset: lance.LanceDataset = lance.dataset(uri)
    centroids: np.ndarray = centroids_to_matrix(dataset.get_ivf_model("vector_idx").centroids)
    batch: pa.Table = dataset.to_table(columns=["vector"])
    vectors: pa.FixedSizeListArray = batch.column("vector").combine_chunks()
    pids: np.ndarray = assign_partition_ids(vectors, centroids, "l2")
    rows: np.ndarray = vectors.values.to_numpy(zero_copy_only=False).reshape(len(vectors), 8)
    for index in range(0, rows.shape[0], 37):
        chosen: float = float(np.linalg.norm(centroids[pids[index]] - rows[index]))
        target: float = float(np.min(np.linalg.norm(centroids - rows[index], axis=1)))
        assert chosen <= target + 1e-4


def test_derive_buckets_packs_contiguous_partitions() -> None:
    """Contiguous partitions pack into buckets bounded by the row cap, in ascending order."""
    buckets: list[tuple[int, int, int, int]] = derive_buckets([3, 3, 3], rows_per_task=6)
    assert buckets == [(0, 1, 0, 1), (2, 2, 0, 1)]


def test_derive_buckets_salts_oversized_partition() -> None:
    """A partition larger than the cap splits into evenly enumerated salted sub-buckets."""
    buckets: list[tuple[int, int, int, int]] = derive_buckets([10], rows_per_task=4)
    assert buckets == [(0, 0, 0, 3), (0, 0, 1, 3), (0, 0, 2, 3)]


def test_derive_buckets_includes_null_tail() -> None:
    """The null tail slot is packed with the rest and appears in a bucket when it carries rows."""
    buckets: list[tuple[int, int, int, int]] = derive_buckets([2, 2, 5], rows_per_task=10)
    assert buckets == [(0, 2, 0, 1)]
    covered: set[int] = set()
    for low, high, salt, num_salts in buckets:
        assert (salt, num_salts) == (0, 1)
        covered.update(range(low, high + 1))
    assert 2 in covered


def test_derive_buckets_rejects_nonpositive_cap() -> None:
    """A non-positive row cap is rejected rather than looping forever."""
    with pytest.raises(ValueError, match="rows_per_task"):
        derive_buckets([1, 2, 3], rows_per_task=0)
