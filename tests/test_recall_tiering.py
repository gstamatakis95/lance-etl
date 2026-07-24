"""Tests for the recall job's small/big size tiering.

A tiny batch of single-fragment groups must take the packed small path where one task scores many datasets, and a
multi-fragment group must take the per-fragment fan-out. The fanned-out, executor-reduced top-k must equal the
single-stream whole-dataset brute force exactly, ties included, both at the primitive level and through the full job.
All datasets are local and all span input goes through :class:`InMemorySpanSource`, so no network is touched.
"""

from __future__ import annotations

import json
import os
import sys
from collections.abc import Callable, Iterable, Iterator
from dataclasses import dataclass, field
from datetime import timedelta
from pathlib import Path
from typing import Any

import lance
import numpy as np
import pyarrow as pa
import pytest
from pyspark.sql import SparkSession

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
from lance_etl.recall.job import MAX_PARTIAL_CANDIDATES_PER_TASK, chunk_samples_by_k
from lance_etl.telemetry import TelemetryConfig

DIM: int = 8


@pytest.fixture(scope="module")
def real_spark() -> Iterator[SparkSession]:
    """Provide a two-core local Spark session for the marked RDD integration case.

    Yields:
        Local Spark session using the locked Python interpreter.
    """
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
    session: SparkSession = (
        SparkSession.builder.master("local[2]")
        .appName("lance-etl-recall-tiering-tests")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()


@dataclass
class RecordedCall:
    """One recorded ``parallelize`` invocation, partitioned the way Spark would slice a list.

    Attributes:
        items: The items handed to ``parallelize``.
        slices: The requested partition count.
        partitions: The items split round-robin into ``slices`` partitions, each a stand-in for one task.
    """

    items: list[Any]
    slices: int

    @property
    def partitions(self) -> list[list[Any]]:
        """Split items round-robin across the requested slices.

        Returns:
            The simulated Spark partitions.
        """
        return [self.items[offset :: self.slices] for offset in range(self.slices)]

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


@dataclass
class RecordingRdd:
    """A fake RDD that maps eagerly while preserving the partition structure."""

    partitions: list[list[Any]]

    def map(self, fn: Callable[[Any], Any]) -> RecordingRdd:
        """Apply a function to every item, keeping items in their partitions.

        Args:
            fn: The mapper.

        Returns:
            A new fake RDD with the mapped items.
        """
        return RecordingRdd([[fn(item) for item in partition] for partition in self.partitions])

    def flatMap(self, fn: Callable[[Any], Iterable[Any]]) -> RecordingRdd:
        """Apply a function and flatten each result while retaining source partitions.

        Args:
            fn: The flat mapper.

        Returns:
            A new fake RDD with flattened mapped items.
        """
        return RecordingRdd([[result for item in partition for result in fn(item)] for partition in self.partitions])

    def reduceByKey(self, fn: Callable[[Any, Any], Any], slices: int) -> RecordingRdd:
        """Reduce key-value items into the requested number of partitions.

        Args:
            fn: Associative value reducer.
            slices: The requested output partition count.

        Returns:
            A fake RDD containing one reduced value per key.
        """
        reduced: dict[Any, Any] = {}
        for partition in self.partitions:
            for key, value in partition:
                reduced[key] = fn(reduced[key], value) if key in reduced else value
        partitions: int = max(1, slices)
        items: list[tuple[Any, Any]] = list(reduced.items())
        return RecordingRdd([items[offset::partitions] for offset in range(partitions)])

    def repartition(self, slices: int) -> RecordingRdd:
        """Redistribute all items across the requested partition count.

        Args:
            slices: The requested output partition count.

        Returns:
            A fake shuffled RDD.
        """
        items: list[Any] = self.collect()
        partitions: int = max(1, slices)
        return RecordingRdd([items[offset::partitions] for offset in range(partitions)])

    def collect(self) -> list[Any]:
        """Flatten the partitions back into one list.

        Returns:
            The mapped items in partition order.
        """
        return [item for partition in self.partitions for item in partition]


@dataclass
class RecordingSparkContext:
    """A fake SparkContext that records every parallelize call and its partitioning.

    Attributes:
        calls: The recorded parallelize calls in invocation order.
    """

    calls: list[RecordedCall] = field(default_factory=list)

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


@dataclass
class RecordingSpark:
    """A fake SparkSession exposing the recording context."""

    sparkContext: RecordingSparkContext = field(default_factory=RecordingSparkContext)


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
        ids: The record ids.
        vectors: The ``(rows, dim)`` float32 matrix.

    Returns:
        The table.
    """
    flat: pa.Array = pa.array(vectors.ravel().tolist(), pa.float32())
    fsl: pa.Array = pa.FixedSizeListArray.from_arrays(flat, vectors.shape[1])
    return pa.table(
        {
            "record_id": pa.array(ids, pa.int64()),
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


def test_sample_chunks_bound_aggregate_top_k_candidates() -> None:
    """Large-tier work splits before one task can retain an unbounded product of samples and k."""
    query: np.ndarray = make_vectors(1, DIM, 7)[0].astype(np.float64)
    samples: list[RecallSample] = [make_sample(index, 1, query, list(range(10_000)), k=10_000) for index in range(5)]

    chunks: list[list[RecallSample]] = chunk_samples_by_k(samples)

    assert [len(chunk) for chunk in chunks] == [2, 2, 1]
    assert all(sum(sample.k for sample in chunk) <= MAX_PARTIAL_CANDIDATES_PER_TASK for chunk in chunks)


class TestExactness:
    """The per-fragment partial top-k reduced by key equals the single-stream whole-dataset brute force."""

    def test_reduce_equals_single_stream_with_ties(self, tmp_path: Path) -> None:
        """A query tying across fragment boundaries reduces to the same ids and distances as the whole scan."""
        uri, ids, vectors = tied_dataset(tmp_path, fragments=6, rows_per_fragment=10, tie_span=12)
        del ids
        dataset: lance.LanceDataset = lance.dataset(uri)
        assert len(dataset.get_fragments()) == 6
        query: np.ndarray = vectors[0].astype(np.float64)
        whole_ids, whole_dists, whole_count = brute_force_top_k_scored(
            dataset, query, 10, "l2", "record_id", "vector", None, 8
        )
        partials: list[tuple[list[Any], list[float]]] = []
        total: int = 0
        for fragment in dataset.get_fragments():
            partial_ids, partial_dists, partial_count = brute_force_top_k_scored(
                dataset, query, 10, "l2", "record_id", "vector", None, 8, fragments=[fragment]
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
        whole_ids, _, whole_count = brute_force_top_k_scored(dataset, query, 10, "l2", "record_id", "vector", None, 8)
        config: RecallJobConfig = config_for(tmp_path, telemetry_config)
        version: int = lance.dataset(uri).version
        sample: RecallSample = make_sample(0, version, query, oracle_top_k(ids, vectors, query, 10))
        fragment_partials: list[tuple[int, dict[str, Any]]] = []
        for fragment in dataset.get_fragments():
            fragment_id: int = int(fragment.fragment_id)
            partials: dict[str, dict[str, Any]] = fragment_vector_partials(uri, version, fragment_id, [sample], config)
            fragment_partials.append((fragment_id, partials[sample.sample_id]))
        leg: dict[str, Any] = reduce_vector_legs(sample, fragment_partials)
        assert leg["status"] == "leg"
        assert leg["ids"] == whole_ids
        assert leg["count"] == whole_count

    def test_invalid_planned_fragment_skips_instead_of_raising(
        self, tmp_path: Path, telemetry_config: TelemetryConfig
    ) -> None:
        """An invalid planned fragment identifier becomes a missing-fragment skip rather than an executor failure."""
        uri, ids, vectors = tied_dataset(tmp_path, fragments=1, rows_per_fragment=10, tie_span=3)
        version: int = lance.dataset(uri).version
        query: np.ndarray = make_vectors(1, DIM, 71)[0].astype(np.float64)
        served: list[int] = oracle_top_k(ids, vectors, query, 10)
        sample: RecallSample = make_sample(0, version, query, served, namespace="whale")
        config: RecallJobConfig = config_for(tmp_path, telemetry_config)
        partials: dict[str, dict[str, Any]] = fragment_vector_partials(uri, version, 5, [sample], config)
        assert partials[sample.sample_id] == {"status": "skip", "reason": "fragment_missing"}

    def test_inventory_drift_sentinel_skips_instead_of_overflowing(
        self, tmp_path: Path, telemetry_config: TelemetryConfig
    ) -> None:
        """The negative executor-inventory sentinel never reaches pylance's unsigned fragment lookup."""
        uri, ids, vectors = tied_dataset(tmp_path, fragments=1, rows_per_fragment=10, tie_span=3)
        version: int = lance.dataset(uri).version
        query: np.ndarray = make_vectors(1, DIM, 71)[0].astype(np.float64)
        sample: RecallSample = make_sample(0, version, query, oracle_top_k(ids, vectors, query, 10))
        config: RecallJobConfig = config_for(tmp_path, telemetry_config)

        partials: dict[str, dict[str, Any]] = fragment_vector_partials(uri, version, -1, [sample], config)

        assert partials[sample.sample_id] == {"status": "skip", "reason": "version_missing"}

    def test_invalid_planned_fragment_skip_propagates_through_reduce(
        self, tmp_path: Path, telemetry_config: TelemetryConfig
    ) -> None:
        """A missing-fragment partial among otherwise-valid partials still skips the whole reduced leg."""
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

    def test_disappeared_planned_version_skips_instead_of_falling_forward(
        self, tmp_path: Path, telemetry_config: TelemetryConfig
    ) -> None:
        """A missing exact task version cannot silently score partials from the latest snapshot."""
        uri, ids, vectors = tied_dataset(tmp_path, fragments=2, rows_per_fragment=10, tie_span=3)
        recorded_version: int = lance.dataset(uri).version
        lance.write_dataset(vectors_table([len(ids)], make_vectors(1, DIM, 72)), uri, mode="append")
        lance.dataset(uri).cleanup_old_versions(older_than=timedelta(0), retain_versions=1, delete_unverified=True)
        query: np.ndarray = make_vectors(1, DIM, 71)[0].astype(np.float64)
        sample: RecallSample = make_sample(
            0,
            recorded_version,
            query,
            oracle_top_k(ids, vectors, query, 10),
            namespace="whale",
        )
        config: RecallJobConfig = config_for(tmp_path, telemetry_config)

        partials: dict[str, dict[str, Any]] = fragment_vector_partials(uri, recorded_version, 0, [sample], config)

        assert partials[sample.sample_id] == {"status": "skip", "reason": "version_missing"}


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
            call for call in spark.sparkContext.calls if call.items and len(call.items[0]) == 4
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
            call for call in spark.sparkContext.calls if call.items and len(call.items[0]) == 4
        ]
        small_calls: list[RecordedCall] = [
            call for call in spark.sparkContext.calls if call.items and len(call.items[0]) == 3
        ]
        assert len(fanout_calls) == 1
        assert len(fanout_calls[0].items) == 1
        assert fanout_calls[0].items[0][3] == 6
        assert not small_calls
        assert len(report.scores) == 1
        assert report.scores[0].recall == pytest.approx(1.0)

    @pytest.mark.integration
    def test_large_group_executor_inventory_runs_on_real_spark(
        self,
        tmp_path: Path,
        telemetry_config: TelemetryConfig,
        real_spark: SparkSession,
    ) -> None:
        """The executor inventory, repartition, and bounded reducer chain serialize through PySpark.

        Args:
            tmp_path: Temporary local dataset root.
            telemetry_config: Test telemetry configuration.
            real_spark: Two-core local Spark session.
        """
        uri, ids, vectors = tied_dataset(tmp_path, fragments=4, rows_per_fragment=8, tie_span=3)
        version: int = lance.dataset(uri).version
        query: np.ndarray = make_vectors(1, DIM, 93)[0].astype(np.float64)
        sample: RecallSample = make_sample(
            0,
            version,
            query,
            oracle_top_k(ids, vectors, query, 10),
            namespace="whale",
        )
        config: RecallJobConfig = config_for(
            tmp_path,
            telemetry_config,
            large_group_fragment_threshold=2,
            large_tier_slices=2,
        )

        report: RecallReport = RecallAuditJob(config).run(
            real_spark,
            InMemorySpanSource(records=[span_record(sample)]),
            0,
            10**13,
        )

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

    def test_missing_large_group_version_skips_without_fanout(
        self, tmp_path: Path, telemetry_config: TelemetryConfig
    ) -> None:
        """A missing captured version stays out of large-tier fragment fanout and skips."""
        uri, ids, vectors = tied_dataset(tmp_path, fragments=4, rows_per_fragment=10, tie_span=3)
        recorded_version: int = lance.dataset(uri).version
        appended_vectors: np.ndarray = make_vectors(1, DIM, 81)
        lance.write_dataset(vectors_table([len(ids)], appended_vectors), uri, mode="append")
        lance.dataset(uri).cleanup_old_versions(older_than=timedelta(0), retain_versions=1, delete_unverified=True)
        query: np.ndarray = make_vectors(1, DIM, 71)[0].astype(np.float64)
        served: list[int] = oracle_top_k(ids + [len(ids)], np.vstack([vectors, appended_vectors]), query, 10)
        sample: RecallSample = make_sample(0, recorded_version, query, served, namespace="whale")
        config: RecallJobConfig = config_for(tmp_path, telemetry_config, large_group_fragment_threshold=2)
        spark: RecordingSpark = RecordingSpark()

        report: RecallReport = RecallAuditJob(config).run(
            spark, InMemorySpanSource(records=[span_record(sample)]), 0, 10**13
        )

        fanout_calls: list[RecordedCall] = [call for call in spark.sparkContext.calls if call.items]
        assert not any(len(call.items[0]) == 4 for call in fanout_calls)
        assert report.scores[0].skip_reason == "version_missing"
        assert report.scores[0].recall is None

    def test_duplicate_sample_ids_fall_back_to_collision_free_small_tier(
        self, tmp_path: Path, telemetry_config: TelemetryConfig
    ) -> None:
        """Duplicate capture ids cannot overwrite each other's large-tier partial dictionaries."""
        uri, ids, vectors = tied_dataset(tmp_path, fragments=4, rows_per_fragment=10, tie_span=3)
        version: int = lance.dataset(uri).version
        query: np.ndarray = make_vectors(1, DIM, 71)[0].astype(np.float64)
        served: list[int] = oracle_top_k(ids, vectors, query, 10)
        samples: list[RecallSample] = [
            make_sample(index, version, query, served, sample_id="duplicate", namespace="whale") for index in range(2)
        ]
        config: RecallJobConfig = config_for(tmp_path, telemetry_config, large_group_fragment_threshold=2)
        spark: RecordingSpark = RecordingSpark()

        report: RecallReport = RecallAuditJob(config).run(
            spark, InMemorySpanSource(records=[span_record(sample) for sample in samples]), 0, 10**13
        )

        fanout_calls: list[RecordedCall] = [
            call for call in spark.sparkContext.calls if call.items and len(call.items[0]) == 4
        ]
        assert not fanout_calls
        assert len(report.scores) == 2
        assert all(score.recall == pytest.approx(1.0) for score in report.scores)
