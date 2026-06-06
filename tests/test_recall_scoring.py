"""Tests for recall brute-force scoring, version pinning, recall math, aggregation, and the job driver.

Spark is replaced with the minimal in-process fake used elsewhere in the suite since the recall job only exercises
``parallelize().map().collect()``. All span input goes through :class:`InMemorySpanSource`, so no network is touched.
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
    AggregateRow,
    InMemorySpanSource,
    RecallAuditJob,
    RecallJobConfig,
    RecallReport,
    RecallSample,
    SampleScore,
    aggregate_scores,
    brute_force_top_k,
    compute_distances,
    emit_recall_metrics,
    format_report,
    resolve_dataset,
    sample_dataset_uri,
    score_version_group,
)
from lance_etl.telemetry import Telemetry, TelemetryConfig

DIM: int = 8
ROWS: int = 100


class FakeRdd:
    """Minimal stand-in for a Spark RDD running map eagerly in process."""

    def __init__(self, items: list[object]) -> None:
        """Initialize the fake RDD.

        Args:
            items: The partitioned items.
        """
        self.items: list[object] = items

    def map(self, fn: Callable[[object], object]) -> FakeRdd:
        """Apply a function to every item eagerly.

        Args:
            fn: The mapper.

        Returns:
            A new fake RDD with the mapped items.
        """
        return FakeRdd([fn(item) for item in self.items])

    def collect(self) -> list[object]:
        """Return the items.

        Returns:
            The current items.
        """
        return list(self.items)


class FakeSparkContext:
    """Minimal stand-in for a SparkContext."""

    def parallelize(self, items: list[object], slices: int) -> FakeRdd:
        """Wrap items into a fake RDD.

        Args:
            items: The items to distribute.
            slices: Ignored partition count.

        Returns:
            The fake RDD.
        """
        del slices
        return FakeRdd(list(items))


class FakeSpark:
    """Minimal stand-in for a SparkSession."""

    def __init__(self) -> None:
        """Initialize the fake session with its fake context."""
        self.sparkContext: FakeSparkContext = FakeSparkContext()


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


def oracle_top_k(ids: list[int], vectors: np.ndarray, query: np.ndarray, k: int, distance_type: str) -> list[int]:
    """Compute the exact top-k ids with a plain full-matrix argsort, as the test oracle.

    Args:
        ids: The candidate ids aligned with the vector rows.
        vectors: The candidate vectors.
        query: The query vector.
        k: The result count.
        distance_type: The distance to rank by.

    Returns:
        The top-k ids in ascending-distance order.
    """
    distances: np.ndarray = compute_distances(vectors.astype(np.float64), query.astype(np.float64), distance_type)
    order: np.ndarray = np.argsort(distances, kind="stable")[:k]
    return [ids[index] for index in order]


def make_sample(**overrides: Any) -> RecallSample:
    """Build a scoring-ready sample with optional field overrides.

    Args:
        overrides: Field overrides applied on top of the defaults.

    Returns:
        The sample.
    """
    fields: dict[str, Any] = {
        "sample_id": "s1",
        "captured_at_unix_ms": 1_700_000_000_000,
        "org_id": "acme",
        "tenant_id": "tenant1",
        "namespace": "ns1",
        "dataset_version": 1,
        "k": 10,
        "query_vector": tuple(float(v) for v in make_vectors(1, DIM, 99)[0]),
        "result_ids": (0, 1, 2),
        "result_distances": (),
    }
    fields.update(overrides)
    return RecallSample(**fields)


@pytest.fixture
def job_config(tmp_path: Path, telemetry_config: TelemetryConfig) -> RecallJobConfig:
    """Build a recall job configuration over the test base directory.

    Args:
        tmp_path: Pytest-provided temporary directory used as the base URI.
        telemetry_config: The test telemetry configuration.

    Returns:
        A configuration with a small batch size so multi-batch merging is exercised.
    """
    return RecallJobConfig(base_uri=str(tmp_path), telemetry=telemetry_config, batch_size=16)


@pytest.fixture
def dataset_setup(tmp_path: Path) -> tuple[str, list[int], np.ndarray]:
    """Write the default sample's dataset and return its URI, ids, and vectors.

    Args:
        tmp_path: Pytest-provided temporary directory used as the base URI.

    Returns:
        ``(uri, ids, vectors)`` for the written dataset.
    """
    ids: list[int] = list(range(ROWS))
    vectors: np.ndarray = make_vectors(ROWS, DIM, 7)
    uri: str = str(tmp_path / "acme" / "tenant1" / "ns1.lance")
    lance.write_dataset(vectors_table(ids, vectors), uri)
    return uri, ids, vectors


class TestDistances:
    """Exact distance kernels match their definitions."""

    def test_l2_is_squared_euclidean(self) -> None:
        """The l2 kernel returns the squared Euclidean distance."""
        candidates: np.ndarray = np.array([[1.0, 2.0], [3.0, 4.0]])
        query: np.ndarray = np.array([1.0, 1.0])
        assert compute_distances(candidates, query, "l2").tolist() == [1.0, 13.0]

    def test_dot_is_negated_dot_product(self) -> None:
        """The dot kernel negates the dot product so smaller is closer."""
        candidates: np.ndarray = np.array([[1.0, 0.0], [2.0, 2.0]])
        query: np.ndarray = np.array([1.0, 1.0])
        assert compute_distances(candidates, query, "dot").tolist() == [-1.0, -4.0]

    def test_cosine_with_zero_norm_row(self) -> None:
        """The cosine kernel pins zero-norm rows to distance 1.0."""
        candidates: np.ndarray = np.array([[1.0, 0.0], [0.0, 0.0]])
        query: np.ndarray = np.array([1.0, 0.0])
        distances: np.ndarray = compute_distances(candidates, query, "cosine")
        assert distances[0] == pytest.approx(0.0)
        assert distances[1] == pytest.approx(1.0)

    def test_hamming_counts_differing_components(self) -> None:
        """The hamming kernel counts differing components."""
        candidates: np.ndarray = np.array([[0.0, 1.0, 1.0], [0.0, 1.0, 0.0]])
        query: np.ndarray = np.array([0.0, 1.0, 0.0])
        assert compute_distances(candidates, query, "hamming").tolist() == [1.0, 0.0]

    def test_unknown_distance_raises(self) -> None:
        """An unknown distance type raises."""
        with pytest.raises(ValueError, match="unknown distance type"):
            compute_distances(np.zeros((1, 2)), np.zeros(2), "manhattan")


class TestBruteForce:
    """The streaming brute force matches the full-matrix oracle."""

    @pytest.mark.parametrize("distance_type", ["l2", "cosine", "dot"])
    def test_matches_oracle_unfiltered(
        self, dataset_setup: tuple[str, list[int], np.ndarray], distance_type: str
    ) -> None:
        """Streaming partial top-k merging equals the oracle across batches for every metric."""
        uri, ids, vectors = dataset_setup
        dataset: lance.LanceDataset = lance.dataset(uri)
        query: np.ndarray = make_vectors(1, DIM, 21)[0].astype(np.float64)
        top_ids, count = brute_force_top_k(dataset, query, 10, distance_type, "vector_id", "vector", None, 16)
        assert count == ROWS
        assert top_ids == oracle_top_k(ids, vectors, query, 10, distance_type)

    def test_matches_oracle_with_filter(self, dataset_setup: tuple[str, list[int], np.ndarray]) -> None:
        """A generated filter narrows the candidate set and the oracle agrees on the subset."""
        uri, ids, vectors = dataset_setup
        dataset: lance.LanceDataset = lance.dataset(uri)
        query: np.ndarray = make_vectors(1, DIM, 22)[0].astype(np.float64)
        keep: list[int] = [i for i in ids if i % 4 == 1]
        top_ids, count = brute_force_top_k(dataset, query, 5, "l2", "vector_id", "vector", "(category = 'cat1')", 16)
        assert count == len(keep)
        assert top_ids == oracle_top_k(keep, vectors[keep], query, 5, "l2")

    def test_dimension_mismatch_raises(self, dataset_setup: tuple[str, list[int], np.ndarray]) -> None:
        """A query whose dimension disagrees with the dataset raises."""
        uri, ids, vectors = dataset_setup
        del ids, vectors
        dataset: lance.LanceDataset = lance.dataset(uri)
        with pytest.raises(ValueError, match="dimension"):
            brute_force_top_k(dataset, np.zeros(4), 5, "l2", "vector_id", "vector", None, 16)


class TestVersionPinning:
    """Scoring replays against the recorded dataset version and flags drift on fallback."""

    def test_pinned_version_ignores_later_appends(
        self, dataset_setup: tuple[str, list[int], np.ndarray], job_config: RecallJobConfig
    ) -> None:
        """Rows appended after the recorded version never enter the true top-k."""
        uri, ids, vectors = dataset_setup
        pinned_version: int = lance.dataset(uri).version
        closer: np.ndarray = np.tile(make_vectors(1, DIM, 21)[0], (50, 1))
        lance.write_dataset(vectors_table(list(range(ROWS, ROWS + 50)), closer), uri, mode="append")
        query: np.ndarray = make_vectors(1, DIM, 21)[0].astype(np.float64)
        dataset, drift = resolve_dataset(uri, pinned_version, None)
        assert dataset is not None
        assert drift is False
        top_ids, count = brute_force_top_k(dataset, query, 10, "l2", "vector_id", "vector", None, 16)
        assert count == ROWS
        assert all(top_id < ROWS for top_id in top_ids)
        assert top_ids == oracle_top_k(ids, vectors, query, 10, "l2")
        sample: RecallSample = make_sample(
            dataset_version=pinned_version, query_vector=tuple(query), result_ids=tuple(top_ids)
        )
        scores: list[SampleScore] = score_version_group(uri, pinned_version, [sample], job_config)
        assert scores[0].recall == pytest.approx(1.0)
        assert scores[0].version_drift is False

    def test_invalid_version_falls_back_to_latest_with_drift(
        self, dataset_setup: tuple[str, list[int], np.ndarray], job_config: RecallJobConfig
    ) -> None:
        """A cleaned-up version falls back to latest and flags the score as drifted."""
        uri, ids, vectors = dataset_setup
        query: np.ndarray = make_vectors(1, DIM, 23)[0].astype(np.float64)
        served: list[int] = oracle_top_k(ids, vectors, query, 10, "l2")
        sample: RecallSample = make_sample(dataset_version=9999, query_vector=tuple(query), result_ids=tuple(served))
        scores: list[SampleScore] = score_version_group(uri, 9999, [sample], job_config)
        assert scores[0].version_drift is True
        assert scores[0].skip_reason is None
        assert scores[0].recall == pytest.approx(1.0)

    def test_missing_dataset_skips_group(self, job_config: RecallJobConfig) -> None:
        """A nonexistent dataset skips every sample in the group with dataset_missing."""
        uri: str = sample_dataset_uri(job_config.base_uri, make_sample(org_id="ghost"))
        scores: list[SampleScore] = score_version_group(uri, 1, [make_sample(org_id="ghost")], job_config)
        assert [score.skip_reason for score in scores] == ["dataset_missing"]
        assert scores[0].recall is None


class TestRecallMath:
    """Recall@k edge cases score or skip as documented."""

    def test_k_larger_than_rows_caps_denominator(self, tmp_path: Path, job_config: RecallJobConfig) -> None:
        """Serving every row of a sub-k dataset scores a perfect 1.0."""
        ids: list[int] = list(range(10))
        vectors: np.ndarray = make_vectors(10, DIM, 5)
        uri: str = str(tmp_path / "acme" / "tenant1" / "small.lance")
        lance.write_dataset(vectors_table(ids, vectors), uri)
        sample: RecallSample = make_sample(namespace="small", k=50, result_ids=tuple(ids))
        scores: list[SampleScore] = score_version_group(uri, 1, [sample], job_config)
        assert scores[0].recall == pytest.approx(1.0)

    def test_null_result_ids_skip(
        self, dataset_setup: tuple[str, list[int], np.ndarray], job_config: RecallJobConfig
    ) -> None:
        """A null served-id capture is skipped and counted."""
        uri, ids, vectors = dataset_setup
        del ids, vectors
        scores: list[SampleScore] = score_version_group(uri, 1, [make_sample(result_ids=None)], job_config)
        assert scores[0].skip_reason == "null_result_ids"
        assert scores[0].recall is None

    def test_partial_overlap_scores_fractionally(
        self, dataset_setup: tuple[str, list[int], np.ndarray], job_config: RecallJobConfig
    ) -> None:
        """Recall is the served-versus-true overlap divided by k."""
        uri, ids, vectors = dataset_setup
        query: np.ndarray = make_vectors(1, DIM, 31)[0].astype(np.float64)
        true_ids: list[int] = oracle_top_k(ids, vectors, query, 10, "l2")
        served: tuple[int, ...] = tuple(true_ids[:5]) + tuple(range(9000, 9005))
        sample: RecallSample = make_sample(k=10, query_vector=tuple(query), result_ids=served)
        scores: list[SampleScore] = score_version_group(uri, 1, [sample], job_config)
        assert scores[0].recall == pytest.approx(0.5)

    def test_filter_translation_failure_skips(
        self, dataset_setup: tuple[str, list[int], np.ndarray], job_config: RecallJobConfig
    ) -> None:
        """A filter referencing an unknown column skips the sample."""
        uri, ids, vectors = dataset_setup
        del ids, vectors
        ast: dict[str, Any] = {"compare": {"column": "nope", "op": "eq", "value": {"int": 1}}}
        scores: list[SampleScore] = score_version_group(uri, 1, [make_sample(filter_ast=ast)], job_config)
        assert scores[0].skip_reason == "filter_translation"

    def test_empty_candidate_set_skips(
        self, dataset_setup: tuple[str, list[int], np.ndarray], job_config: RecallJobConfig
    ) -> None:
        """A filter matching nothing skips the sample instead of dividing by zero."""
        uri, ids, vectors = dataset_setup
        del ids, vectors
        ast: dict[str, Any] = {"compare": {"column": "category", "op": "eq", "value": {"string": "nope"}}}
        scores: list[SampleScore] = score_version_group(uri, 1, [make_sample(filter_ast=ast)], job_config)
        assert scores[0].skip_reason == "empty_candidate_set"

    def test_dimension_mismatch_skips_as_scan_error(
        self, dataset_setup: tuple[str, list[int], np.ndarray], job_config: RecallJobConfig
    ) -> None:
        """A wrong-dimension query skips the sample as a scan error."""
        uri, ids, vectors = dataset_setup
        del ids, vectors
        sample: RecallSample = make_sample(query_vector=(0.5, 0.5))
        scores: list[SampleScore] = score_version_group(uri, 1, [sample], job_config)
        assert scores[0].skip_reason == "scan_error"


def score(recall: float | None, **overrides: Any) -> SampleScore:
    """Build a sample score for aggregation tests.

    Args:
        recall: The measured recall, or None for a skipped sample.
        overrides: Field overrides applied on top of the defaults.

    Returns:
        The score.
    """
    fields: dict[str, Any] = {
        "sample_id": "s",
        "org_id": "acme",
        "k": 10,
        "nprobes_min": None,
        "nprobes_max": None,
        "refine_factor": None,
        "recall": recall,
        "version_drift": False,
        "skip_reason": None if recall is not None else "null_result_ids",
    }
    fields.update(overrides)
    return SampleScore(**fields)


class TestAggregationAndReport:
    """Scores aggregate into overall, RPC, and org rows, and the table renders cleanly."""

    def test_bucket_structure_and_statistics(self) -> None:
        """Rows come out as overall, RPC buckets, then org buckets with correct statistics."""
        scores: list[SampleScore] = [
            score(1.0, nprobes_min=8, nprobes_max=32, refine_factor=2),
            score(0.5, nprobes_min=8, nprobes_max=32, refine_factor=2),
            score(0.8, org_id="beta"),
            score(None, org_id="beta", version_drift=True),
        ]
        rows: list[AggregateRow] = aggregate_scores(scores)
        assert rows[0].bucket == "overall"
        assert rows[0].samples == 3
        assert rows[0].mean_recall == pytest.approx((1.0 + 0.5 + 0.8) / 3)
        assert rows[0].drift_count == 1
        assert rows[0].skip_count == 1
        labels: list[str] = [row.bucket for row in rows]
        assert "rpc nprobes=8..32 refine=2" in labels
        assert "rpc nprobes=default..default refine=unset" in labels
        assert labels[-2:] == ["org acme", "org beta"]
        rpc_row: AggregateRow = next(row for row in rows if row.bucket == "rpc nprobes=8..32 refine=2")
        assert rpc_row.is_rpc_bucket is True
        assert rpc_row.samples == 2
        assert rpc_row.mean_recall == pytest.approx(0.75)
        assert rpc_row.p50 == pytest.approx(0.75)
        assert rpc_row.p95 == pytest.approx(float(np.percentile([1.0, 0.5], 95)))
        org_row: AggregateRow = next(row for row in rows if row.bucket == "org beta")
        assert org_row.is_rpc_bucket is False
        assert org_row.samples == 1
        assert org_row.skip_count == 1
        assert org_row.drift_count == 1

    def test_empty_scores_aggregate_to_empty_overall(self) -> None:
        """No scores still produce a well-formed overall row with no statistics."""
        rows: list[AggregateRow] = aggregate_scores([])
        assert len(rows) == 1
        assert rows[0].samples == 0
        assert rows[0].mean_recall is None

    def test_format_report_renders_table_and_skip_lines(self) -> None:
        """The table carries the header, every bucket, and the skip summaries."""
        scores: list[SampleScore] = [score(1.0), score(None)]
        report: RecallReport = RecallReport(
            rows=aggregate_scores(scores), scores=scores, parse_skips={"missing:recall.k": 2}
        )
        rendered: str = format_report(report)
        assert "bucket" in rendered
        assert "overall" in rendered
        assert "org acme" in rendered
        assert "1.0000" in rendered
        assert "parse skips: missing:recall.k=2" in rendered
        assert "score skips: null_result_ids=1" in rendered

    def test_emit_metrics_only_for_rpc_buckets_without_org_tags(self, telemetry: Telemetry) -> None:
        """Gauges are emitted per RPC bucket only, tagged with RPC parameters and never the org."""
        emitted: list[tuple[str, float, list[str]]] = []

        def capture(name: str, value: float, tags: list[str] | None = None) -> None:
            """Record one gauge call.

            Args:
                name: The metric name.
                value: The gauge value.
                tags: The metric tags.
            """
            emitted.append((name, value, list(tags or [])))

        telemetry.gauge = capture
        rows: list[AggregateRow] = aggregate_scores(
            [score(1.0, nprobes_min=8, nprobes_max=32, refine_factor=2), score(0.5, org_id="beta")]
        )
        emit_recall_metrics(telemetry, rows)
        assert len(emitted) == 2
        assert all(name == "recall.measured" for name, value, tags in emitted)
        all_tags: list[str] = [tag for name, value, tags in emitted for tag in tags]
        assert "nprobes_min:8" in all_tags
        assert "nprobes_max:32" in all_tags
        assert "refine_factor:2" in all_tags
        assert "nprobes_min:default" in all_tags
        assert "refine_factor:unset" in all_tags
        assert not any(tag.startswith("org") for tag in all_tags)


class TestJobEndToEnd:
    """The full job runs from span records to a report over a fake Spark."""

    def test_run_scores_and_counts_parse_skips(
        self, dataset_setup: tuple[str, list[int], np.ndarray], job_config: RecallJobConfig
    ) -> None:
        """A perfect capture scores 1.0 overall and a malformed record is counted, not fatal."""
        uri, ids, vectors = dataset_setup
        version: int = lance.dataset(uri).version
        query: np.ndarray = make_vectors(1, DIM, 42)[0].astype(np.float64)
        served: list[int] = oracle_top_k(ids, vectors, query, 10, "l2")
        good: dict[str, Any] = {
            "recall.sample": "true",
            "recall.sample_id": "good-sample",
            "recall.captured_at_unix_ms": "1500",
            "recall.org_id": "acme",
            "recall.tenant_id": "tenant1",
            "recall.namespace": "ns1",
            "recall.dataset_version": str(version),
            "recall.k": "10",
            "recall.query_vector": json.dumps(list(query)),
            "recall.result_ids": json.dumps(served),
            "recall.result_distances": json.dumps([0.0] * 10),
        }
        malformed: dict[str, Any] = dict(good)
        del malformed["recall.sample_id"]
        outside_window: dict[str, Any] = dict(good)
        outside_window["recall.captured_at_unix_ms"] = "999999"
        source: InMemorySpanSource = InMemorySpanSource(records=[good, malformed, outside_window])
        report: RecallReport = RecallAuditJob(job_config).run(FakeSpark(), source, 1000, 2000)
        assert report.parse_skips == {"missing:recall.sample_id": 1}
        assert len(report.scores) == 1
        assert report.rows[0].bucket == "overall"
        assert report.rows[0].samples == 1
        assert report.rows[0].mean_recall == pytest.approx(1.0)
        assert "org acme" in [row.bucket for row in report.rows]
