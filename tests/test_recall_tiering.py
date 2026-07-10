"""Tests for the recall job's small/big size tiering.

A tiny batch of single-fragment groups must take the packed small path where one task scores many datasets, and a
multi-fragment group must take the per-fragment fan-out. The fanned-out, driver-reduced top-k must equal the
single-stream whole-dataset brute force exactly, ties included, both at the primitive level and through the full job.
All datasets are local and all span input goes through :class:`InMemorySpanSource`, so no network is touched.
"""

from __future__ import annotations

import json
from collections.abc import Callable
from pathlib import Path
from typing import Any

import lance
import numpy as np
import pyarrow as pa
import pytest

from lance_etl.recall import (
    InMemorySpanSource,
    RecallAuditJob,
    RecallJobConfig,
    RecallReport,
    RecallSample,
    SampleScore,
    brute_force_top_k_scored,
    compute_distances,
    fragment_vector_partials,
    reduce_partial_top_k,
    reduce_vector_legs,
)
from lance_etl.telemetry import TelemetryConfig

DIM: int = 8


class RecordedCall:
    """One recorded ``parallelize`` invocation, partitioned the way Spark would slice a list.

    Attributes:
        items: The items handed to ``parallelize``.
        slices: The requested partition count.
        partitions: The items split round-robin into ``slices`` partitions, each a stand-in for one task.
    """

    def __init__(self, items: list[Any], slices: int) -> None:
        """Record and partition one parallelize call.

        Args:
            items: The items handed to parallelize.
            slices: The requested partition count.
        """
        self.items: list[Any] = items
        self.slices: int = slices
        self.partitions: list[list[Any]] = [items[offset::slices] for offset in range(slices)]

    def tasks(self) -> int:
        """Count the non-empty partitions, the number of tasks Spark would launch.

        Returns:
            The number of non-empty partitions.
        """
        return sum(1 for partition in self.partitions if partition)

    def max_partition_size(self) -> int:
        """Return the largest partition size, the most datasets one task scores.

        Returns:
            The maximum partition length, or zero when there are no items.
        """
        return max((len(partition) for partition in self.partitions), default=0)


class RecordingRdd:
    """A fake RDD that maps eagerly while preserving the partition structure."""

    def __init__(self, partitions: list[list[Any]]) -> None:
        """Initialize the fake RDD.

        Args:
            partitions: The partitioned items.
        """
        self.partitions: list[list[Any]] = partitions

    def map(self, fn: Callable[[Any], Any]) -> RecordingRdd:
        """Apply a function to every item, keeping items in their partitions.

        Args:
            fn: The mapper.

        Returns:
            A new fake RDD with the mapped items.
        """
        return RecordingRdd([[fn(item) for item in partition] for partition in self.partitions])

    def collect(self) -> list[Any]:
        """Flatten the partitions back into one list.

        Returns:
            The mapped items in partition order.
        """
        return [item for partition in self.partitions for item in partition]


class RecordingSparkContext:
    """A fake SparkContext that records every parallelize call and its partitioning.

    Attributes:
        calls: The recorded parallelize calls in invocation order.
    """

    def __init__(self) -> None:
        """Initialize the recording context."""
        self.calls: list[RecordedCall] = []

    def parallelize(self, items: list[Any], slices: int) -> RecordingRdd:
        """Record the call, partition the items, and return a fake RDD.

        Args:
            items: The items to distribute.
            slices: The requested partition count.

        Returns:
            The fake RDD over the partitioned items.
        """
        materialized: list[Any] = list(items)
        partitions: int = max(1, slices)
        call: RecordedCall = RecordedCall(materialized, partitions)
        self.calls.append(call)
        return RecordingRdd(call.partitions)


class RecordingSpark:
    """A fake SparkSession exposing the recording context."""

    def __init__(self) -> None:
        """Initialize the fake session with its recording context."""
        self.sparkContext: RecordingSparkContext = RecordingSparkContext()


def make_vectors(rows: int, dim: int, seed: int) -> np.ndarray:
    """Generate a deterministic random vector matrix.

    Args:
        rows: Number of vectors.
        dim: Vector dimension.
        seed: Random seed.

    Returns:
        A ``(rows, dim)`` float32 matrix.
    """
    rng: np.random.Generator = np.random.default_rng(seed)
    return rng.random((rows, dim), dtype=np.float32)


def vectors_table(ids: list[int], vectors: np.ndarray) -> pa.Table:
    """Build a Lance-writable table with id, vector, and category columns.

    Args:
        ids: The vector ids.
        vectors: The ``(rows, dim)`` float32 matrix.

    Returns:
        The table.
    """
    flat: pa.Array = pa.array(vectors.ravel().tolist(), pa.float32())
    fsl: pa.Array = pa.FixedSizeListArray.from_arrays(flat, vectors.shape[1])
    return pa.table(
        {
            "vector_id": pa.array(ids, pa.int64()),
            "vector": fsl,
            "category": pa.array([f"cat{i % 4}" for i in ids]),
        }
    )


def oracle_top_k(ids: list[int], vectors: np.ndarray, query: np.ndarray, k: int) -> list[int]:
    """Compute the exact l2 top-k ids with a plain full-matrix argsort, as the test oracle.

    Args:
        ids: The candidate ids aligned with the vector rows.
        vectors: The candidate vectors.
        query: The query vector.
        k: The result count.

    Returns:
        The top-k ids in ascending-distance order.
    """
    distances: np.ndarray = compute_distances(vectors.astype(np.float64), query.astype(np.float64), "l2")
    order: np.ndarray = np.argsort(distances, kind="stable")[:k]
    return [ids[index] for index in order]


def make_sample(uri_index: int, version: int, query: np.ndarray, served: list[int], **overrides: Any) -> RecallSample:
    """Build a scoring-ready vector sample for a given tenant namespace.

    Args:
        uri_index: The tenant index, used to derive a distinct namespace and dataset URI.
        version: The recorded dataset version.
        query: The query vector.
        served: The served result ids in rank order.
        overrides: Field overrides applied on top of the defaults.

    Returns:
        The sample.
    """
    fields: dict[str, Any] = {
        "sample_id": f"s{uri_index}",
        "captured_at_unix_ms": 1_700_000_000_000,
        "org_id": "acme",
        "tenant_id": "tenant1",
        "namespace": f"ns{uri_index}",
        "dataset_version": version,
        "k": 10,
        "query_vector": tuple(float(value) for value in query),
        "result_ids": tuple(served),
        "result_distances": (),
    }
    fields.update(overrides)
    return RecallSample(**fields)


def span_record(sample: RecallSample) -> dict[str, Any]:
    """Render one sample as a flat recall span attribute dictionary.

    Args:
        sample: The sample to render.

    Returns:
        The flat attribute dictionary the parser consumes.
    """
    return {
        "recall.sample": "true",
        "recall.sample_id": sample.sample_id,
        "recall.captured_at_unix_ms": str(sample.captured_at_unix_ms),
        "recall.org_id": sample.org_id,
        "recall.tenant_id": sample.tenant_id,
        "recall.namespace": sample.namespace,
        "recall.dataset_version": str(sample.dataset_version),
        "recall.k": str(sample.k),
        "recall.query_vector": json.dumps(list(sample.query_vector)),
        "recall.result_ids": json.dumps(list(sample.result_ids or ())),
        "recall.result_distances": json.dumps([0.0] * len(sample.result_ids or ())),
    }


def config_for(tmp_path: Path, telemetry_config: TelemetryConfig, **overrides: Any) -> RecallJobConfig:
    """Build a recall job configuration over the test base directory.

    Args:
        tmp_path: Pytest temporary directory used as the base URI.
        telemetry_config: The test telemetry configuration.
        overrides: Configuration overrides applied on top of the defaults.

    Returns:
        The configuration.
    """
    fields: dict[str, Any] = {"base_uri": str(tmp_path), "telemetry": telemetry_config}
    fields.update(overrides)
    return RecallJobConfig(**fields)


def tied_dataset(
    tmp_path: Path, fragments: int, rows_per_fragment: int, tie_span: int
) -> tuple[str, list[int], np.ndarray]:
    """Write a multi-fragment dataset whose first rows share one vector to force cross-fragment ties.

    Args:
        tmp_path: Pytest temporary directory used as the base URI.
        fragments: Number of fragments to produce.
        rows_per_fragment: Rows per fragment file.
        tie_span: Number of leading rows forced to share a single vector.

    Returns:
        ``(uri, ids, vectors)`` for the written dataset.
    """
    rows: int = fragments * rows_per_fragment
    ids: list[int] = list(range(rows))
    vectors: np.ndarray = make_vectors(rows, DIM, 3)
    vectors[0:tie_span] = vectors[0]
    uri: str = str(tmp_path / "acme" / "tenant1" / "whale.lance")
    lance.write_dataset(vectors_table(ids, vectors), uri, max_rows_per_file=rows_per_fragment)
    return uri, ids, vectors


class TestExactness:
    """The per-fragment partial top-k reduced on the driver equals the single-stream whole-dataset brute force."""

    def test_reduce_equals_single_stream_with_ties(self, tmp_path: Path) -> None:
        """A query tying across fragment boundaries reduces to the same ids and distances as the whole scan."""
        uri, ids, vectors = tied_dataset(tmp_path, fragments=6, rows_per_fragment=10, tie_span=12)
        del ids
        dataset: lance.LanceDataset = lance.dataset(uri)
        assert len(dataset.get_fragments()) == 6
        query: np.ndarray = vectors[0].astype(np.float64)
        whole_ids, whole_dists, whole_count = brute_force_top_k_scored(
            dataset, query, 10, "l2", "vector_id", "vector", None, 8
        )
        partials: list[tuple[list[Any], list[float]]] = []
        total: int = 0
        for fragment in dataset.get_fragments():
            partial_ids, partial_dists, partial_count = brute_force_top_k_scored(
                dataset, query, 10, "l2", "vector_id", "vector", None, 8, fragments=[fragment]
            )
            partials.append((partial_ids, partial_dists))
            total += partial_count
        reduced_ids, reduced_dists = reduce_partial_top_k(partials, 10)
        assert reduced_ids == whole_ids
        assert reduced_dists == pytest.approx(whole_dists)
        assert total == whole_count

    def test_module_fanout_path_equals_single_stream(self, tmp_path: Path, telemetry_config: TelemetryConfig) -> None:
        """The production fan-out functions reduce to the same leg as the whole-dataset scan."""
        uri, ids, vectors = tied_dataset(tmp_path, fragments=5, rows_per_fragment=8, tie_span=7)
        dataset: lance.LanceDataset = lance.dataset(uri)
        query: np.ndarray = make_vectors(1, DIM, 17)[0].astype(np.float64)
        whole_ids, _, whole_count = brute_force_top_k_scored(dataset, query, 10, "l2", "vector_id", "vector", None, 8)
        config: RecallJobConfig = config_for(tmp_path, telemetry_config)
        version: int = lance.dataset(uri).version
        sample: RecallSample = make_sample(0, version, query, oracle_top_k(ids, vectors, query, 10))
        fragment_partials: list[tuple[int, dict[str, Any]]] = []
        for index in range(len(dataset.get_fragments())):
            partials: dict[str, dict[str, Any]] = fragment_vector_partials(uri, version, index, [sample], config)
            fragment_partials.append((index, partials[sample.sample_id]))
        leg: dict[str, Any] = reduce_vector_legs(sample, fragment_partials)
        assert leg["status"] == "leg"
        assert leg["ids"] == whole_ids
        assert leg["count"] == whole_count

    def test_stale_fragment_index_skips_instead_of_raising(
        self, tmp_path: Path, telemetry_config: TelemetryConfig
    ) -> None:
        """A fragment_index beyond the freshly-opened dataset's fragment count skips rather than raising.

        Stands in for a concurrent compaction shrinking the fragment count between the
        ``classify_groups`` probe (which planned this index) and this task's own dataset open: the
        dataset here genuinely has one fragment, so index 5 is out of range and must not raise
        ``IndexError``.
        """
        uri, ids, vectors = tied_dataset(tmp_path, fragments=1, rows_per_fragment=10, tie_span=3)
        version: int = lance.dataset(uri).version
        query: np.ndarray = make_vectors(1, DIM, 71)[0].astype(np.float64)
        served: list[int] = oracle_top_k(ids, vectors, query, 10)
        sample: RecallSample = make_sample(0, version, query, served, namespace="whale")
        config: RecallJobConfig = config_for(tmp_path, telemetry_config)
        partials: dict[str, dict[str, Any]] = fragment_vector_partials(uri, version, 5, [sample], config)
        assert partials[sample.sample_id] == {"status": "skip", "reason": "fragment_missing"}

    def test_stale_fragment_index_skip_propagates_through_reduce(
        self, tmp_path: Path, telemetry_config: TelemetryConfig
    ) -> None:
        """A fragment_missing partial among otherwise-valid partials still skips the whole reduced leg.

        Mirrors :meth:`reduce_vector_legs`'s existing rule that any skip among a sample's per-fragment
        partials skips the whole leg, so a mid-flight shrink degrades to an honest skip instead of a
        recall score computed from an incomplete fragment scan.
        """
        uri, ids, vectors = tied_dataset(tmp_path, fragments=2, rows_per_fragment=10, tie_span=3)
        version: int = lance.dataset(uri).version
        query: np.ndarray = make_vectors(1, DIM, 71)[0].astype(np.float64)
        served: list[int] = oracle_top_k(ids, vectors, query, 10)
        sample: RecallSample = make_sample(0, version, query, served, namespace="whale")
        config: RecallJobConfig = config_for(tmp_path, telemetry_config)
        valid_partials: dict[str, dict[str, Any]] = fragment_vector_partials(uri, version, 0, [sample], config)
        missing_partials: dict[str, dict[str, Any]] = fragment_vector_partials(uri, version, 9, [sample], config)
        fragment_partials: list[tuple[int, dict[str, Any]]] = [
            (0, valid_partials[sample.sample_id]),
            (9, missing_partials[sample.sample_id]),
        ]
        leg: dict[str, Any] = reduce_vector_legs(sample, fragment_partials)
        assert leg == {"status": "skip", "reason": "fragment_missing"}


class TestSmallTier:
    """Many tiny single-fragment groups take the packed small path: one task scores many datasets."""

    def test_many_tiny_groups_are_packed(self, tmp_path: Path, telemetry_config: TelemetryConfig) -> None:
        """Six tiny groups score on at most two packed tasks with no per-fragment fan-out, all recall 1.0."""
        records: list[dict[str, Any]] = []
        for index in range(6):
            ids: list[int] = list(range(12))
            vectors: np.ndarray = make_vectors(12, DIM, index)
            uri: str = str(tmp_path / "acme" / "tenant1" / f"ns{index}.lance")
            lance.write_dataset(vectors_table(ids, vectors), uri)
            version: int = lance.dataset(uri).version
            query: np.ndarray = make_vectors(1, DIM, 100 + index)[0].astype(np.float64)
            served: list[int] = oracle_top_k(ids, vectors, query, 10)
            records.append(span_record(make_sample(index, version, query, served)))
        config: RecallJobConfig = config_for(
            tmp_path, telemetry_config, large_group_fragment_threshold=100, small_tier_slices=2
        )
        spark: RecordingSpark = RecordingSpark()
        report: RecallReport = RecallAuditJob(config).run(spark, InMemorySpanSource(records=records), 0, 10**13)
        small_calls: list[RecordedCall] = [
            call for call in spark.sparkContext.calls if call.items and len(call.items[0]) == 3
        ]
        fanout_calls: list[RecordedCall] = [
            call for call in spark.sparkContext.calls if call.items and len(call.items[0]) == 5
        ]
        assert len(small_calls) == 1
        assert not fanout_calls
        assert small_calls[0].slices == 2
        assert small_calls[0].tasks() == 2
        assert small_calls[0].max_partition_size() > 1
        assert len(report.scores) == 6
        assert all(score.recall == pytest.approx(1.0) for score in report.scores)


class TestLargeTier:
    """A multi-fragment group fans the brute force out per fragment and stays exact through the job."""

    def test_large_group_fans_out_per_fragment(self, tmp_path: Path, telemetry_config: TelemetryConfig) -> None:
        """A six-fragment group produces one vector work item per fragment and scores recall 1.0."""
        uri, ids, vectors = tied_dataset(tmp_path, fragments=6, rows_per_fragment=10, tie_span=3)
        version: int = lance.dataset(uri).version
        query: np.ndarray = make_vectors(1, DIM, 71)[0].astype(np.float64)
        served: list[int] = oracle_top_k(ids, vectors, query, 10)
        sample: RecallSample = make_sample(0, version, query, served, namespace="whale")
        config: RecallJobConfig = config_for(tmp_path, telemetry_config, large_group_fragment_threshold=2)
        spark: RecordingSpark = RecordingSpark()
        report: RecallReport = RecallAuditJob(config).run(
            spark, InMemorySpanSource(records=[span_record(sample)]), 0, 10**13
        )
        fanout_calls: list[RecordedCall] = [
            call for call in spark.sparkContext.calls if call.items and len(call.items[0]) == 5
        ]
        small_calls: list[RecordedCall] = [
            call for call in spark.sparkContext.calls if call.items and len(call.items[0]) == 3
        ]
        assert len(fanout_calls) == 1
        assert len(fanout_calls[0].items) == 6
        assert not small_calls
        assert len(report.scores) == 1
        assert report.scores[0].recall == pytest.approx(1.0)

    def test_large_and_small_paths_score_identically(self, tmp_path: Path, telemetry_config: TelemetryConfig) -> None:
        """The same data scored via the per-fragment large path matches the whole-dataset small path exactly."""
        uri, ids, vectors = tied_dataset(tmp_path, fragments=5, rows_per_fragment=8, tie_span=11)
        version: int = lance.dataset(uri).version
        query: np.ndarray = make_vectors(1, DIM, 53)[0].astype(np.float64)
        served: list[int] = oracle_top_k(ids, vectors, query, 10)[:6] + [9000, 9001, 9002, 9003]
        sample: RecallSample = make_sample(0, version, query, served, namespace="whale")
        records: list[dict[str, Any]] = [span_record(sample)]
        small_config: RecallJobConfig = config_for(tmp_path, telemetry_config, large_group_fragment_threshold=100)
        large_config: RecallJobConfig = config_for(tmp_path, telemetry_config, large_group_fragment_threshold=2)
        small_report: RecallReport = RecallAuditJob(small_config).run(
            RecordingSpark(), InMemorySpanSource(records=records), 0, 10**13
        )
        large_report: RecallReport = RecallAuditJob(large_config).run(
            RecordingSpark(), InMemorySpanSource(records=records), 0, 10**13
        )
        small_score: SampleScore = small_report.scores[0]
        large_score: SampleScore = large_report.scores[0]
        assert small_score.recall == large_score.recall
        assert small_score.ndcg == large_score.ndcg
        assert small_score.mrr == large_score.mrr
        assert large_score.recall == pytest.approx(0.6)
