"""Tests for nDCG and MRR grading, exact BM25 text scoring, and hybrid fusion replay.

The ranking-quality cases use hand-computable rankings so the expected nDCG and MRR are known closed-form values. The
text and hybrid end-to-end cases write a tiny local Lance dataset and drive the full job through
:class:`InMemorySpanSource`, so no network is touched. Fusion replay is exercised as a pure function against the same
values the Rust ``fusion.rs`` unit tests assert.
"""

from __future__ import annotations

import json
import math
from collections.abc import Callable
from pathlib import Path
from typing import Any

import lance
import numpy as np
import pyarrow as pa
import pytest

from lance_etl.recall import (
    FusionReplayError,
    InMemorySpanSource,
    RecallAuditJob,
    RecallJobConfig,
    RecallReport,
    SampleScore,
    TextQueryTranslationError,
    bm25_column_scores,
    bm25_top_k,
    fuse_legs,
    parse_recall_sample,
    parse_samples,
    ranking_quality,
    text_query_field_queries,
)
from lance_etl.telemetry import TelemetryConfig

DIM: int = 8


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


def text_table(ids: list[int], bodies: list[str], vectors: np.ndarray) -> pa.Table:
    """Build a Lance-writable table with id, vector, and body columns.

    Args:
        ids: The row ids.
        bodies: The text bodies aligned with the ids.
        vectors: The ``(rows, dim)`` float32 vector matrix aligned with the ids.

    Returns:
        The table.
    """
    flat: pa.Array = pa.array(vectors.ravel().tolist(), pa.float32())
    fsl: pa.Array = pa.FixedSizeListArray.from_arrays(flat, vectors.shape[1])
    return pa.table(
        {
            "vector_id": pa.array(ids, pa.int64()),
            "vector": fsl,
            "body": pa.array(bodies),
        }
    )


@pytest.fixture
def job_config(tmp_path: Path, telemetry_config: TelemetryConfig) -> RecallJobConfig:
    """Build a recall job configuration over the test base directory.

    Args:
        tmp_path: Pytest-provided temporary directory used as the base URI.
        telemetry_config: The test telemetry configuration.

    Returns:
        The configuration.
    """
    return RecallJobConfig(base_uri=str(tmp_path), telemetry=telemetry_config, batch_size=16)


class TestRankingQuality:
    """nDCG@k and MRR match closed-form values for known rankings."""

    def test_perfect_ranking_scores_all_ones(self) -> None:
        """A served order identical to the ground truth scores recall, nDCG, and MRR of 1.0."""
        recall, ndcg, mrr = ranking_quality(["a", "b", "c"], ["a", "b", "c"], 3, 3)
        assert (recall, ndcg, mrr) == (1.0, 1.0, 1.0)

    def test_reversed_ranking_known_ndcg_and_mrr(self) -> None:
        """A fully reversed order keeps recall at 1.0 but lowers nDCG and drops the top result deep."""
        recall, ndcg, mrr = ranking_quality(["a", "b", "c"], ["c", "b", "a"], 3, 3)
        dcg: float = 1 / math.log2(2) + 2 / math.log2(3) + 3 / math.log2(4)
        idcg: float = 3 / math.log2(2) + 2 / math.log2(3) + 1 / math.log2(4)
        assert recall == 1.0
        assert ndcg == pytest.approx(dcg / idcg)
        assert mrr == pytest.approx(1.0 / 3.0)

    def test_only_top_result_served_known_values(self) -> None:
        """Serving only the exact top result keeps MRR at 1.0 while recall and nDCG fall."""
        recall, ndcg, mrr = ranking_quality(["a", "b", "c"], ["a", "x", "y"], 3, 3)
        idcg: float = 3 / math.log2(2) + 2 / math.log2(3) + 1 / math.log2(4)
        assert recall == pytest.approx(1.0 / 3.0)
        assert ndcg == pytest.approx((3 / math.log2(2)) / idcg)
        assert mrr == 1.0

    def test_top_result_absent_zeroes_mrr(self) -> None:
        """When the exact top result is not served, MRR is zero even though recall is positive."""
        recall, ndcg, mrr = ranking_quality(["a", "b", "c"], ["b", "c"], 3, 3)
        assert recall == pytest.approx(2.0 / 3.0)
        assert mrr == 0.0

    def test_candidate_count_below_k_caps_denominator(self) -> None:
        """A ground truth smaller than k scores a perfect 1.0 across all three metrics."""
        recall, ndcg, mrr = ranking_quality(["a", "b"], ["a", "b"], 5, 2)
        assert (recall, ndcg, mrr) == (1.0, 1.0, 1.0)


class TestBm25:
    """Exact BM25 ranks documents by term saturation and length normalization."""

    def test_column_scores_rank_by_term_frequency(self) -> None:
        """More occurrences of a query term raise the BM25 score and non-matching docs are masked out."""
        token_lists: list[list[str]] = [["apple", "apple", "apple"], ["apple", "apple"], ["apple"], ["banana"]]
        scores, matched = bm25_column_scores(token_lists, ["apple"], "or")
        assert matched.tolist() == [True, True, True, False]
        assert scores[0] > scores[1] > scores[2] > 0.0
        assert scores[3] == 0.0

    def test_and_operator_requires_all_terms(self) -> None:
        """The and operator only matches documents containing every query term."""
        token_lists: list[list[str]] = [["apple", "pie"], ["apple"], ["pie"]]
        unused_scores, matched = bm25_column_scores(token_lists, ["apple", "pie"], "and")
        del unused_scores
        assert matched.tolist() == [True, False, False]

    def test_top_k_orders_and_counts_matches(self, tmp_path: Path) -> None:
        """The exact BM25 top-k ranks the most relevant docs first and counts only matched docs."""
        uri: str = str(tmp_path / "bm25.lance")
        bodies: list[str] = ["apple apple apple", "apple apple", "apple", "banana"]
        lance.write_dataset(text_table([0, 1, 2, 3], bodies, np.zeros((4, DIM), dtype=np.float32)), uri)
        dataset: lance.LanceDataset = lance.dataset(uri)
        ids, scores, count = bm25_top_k(dataset, [("body", ["apple"], "or", 1.0)], 10, "vector_id", None, 16)
        assert ids == [0, 1, 2]
        assert count == 3
        assert scores[0] > scores[1] > scores[2]


class TestTextQueryExtraction:
    """The text-query AST extracts validated per-column scoring clauses."""

    COLUMNS: frozenset[str] = frozenset({"body", "title", "vector_id"})

    def test_match_without_column_uses_default_columns(self) -> None:
        """A match clause without a column fans out across the default text columns."""
        node: dict[str, Any] = {"match": {"terms": "Hello World", "operator": "or"}}
        clauses = text_query_field_queries(node, ("body", "title"), self.COLUMNS)
        assert clauses == [("body", ["hello", "world"], "or", 1.0), ("title", ["hello", "world"], "or", 1.0)]

    def test_match_with_explicit_column_and_boost(self) -> None:
        """A match clause naming a column and boost is honored verbatim."""
        node: dict[str, Any] = {"match": {"terms": "apple", "column": "body", "operator": "and", "boost": 2.0}}
        clauses = text_query_field_queries(node, (), self.COLUMNS)
        assert clauses == [("body", ["apple"], "and", 2.0)]

    def test_multi_match_with_per_column_boosts(self) -> None:
        """A multi_match clause yields one validated clause per column with its boost."""
        node: dict[str, Any] = {
            "multi_match": {"terms": "apple", "columns": ["body", "title"], "boosts": [1.5, 0.5], "operator": "or"}
        }
        clauses = text_query_field_queries(node, (), self.COLUMNS)
        assert clauses == [("body", ["apple"], "or", 1.5), ("title", ["apple"], "or", 0.5)]

    @pytest.mark.parametrize(
        "node",
        [
            {"phrase": {"terms": "apple pie", "column": "body"}},
            {"match": {"terms": "apple", "column": "missing"}},
            {"match": {"terms": "apple", "column": "body; DROP"}},
            {"multi_match": {"terms": "apple", "columns": [], "operator": "or"}},
            {"multi_match": {"terms": "apple", "columns": ["body"], "boosts": [1.0, 2.0], "operator": "or"}},
            {"match": {"terms": "apple", "operator": "maybe"}},
            "not a node",
        ],
    )
    def test_invalid_or_unsupported_nodes_rejected(self, node: Any) -> None:
        """Unsupported tags, unknown or unsafe columns, and malformed shapes are rejected."""
        with pytest.raises(TextQueryTranslationError):
            text_query_field_queries(node, ("body",), self.COLUMNS)

    def test_match_without_column_and_no_defaults_rejected(self) -> None:
        """A column-less match clause with no default columns has nothing to score and is rejected."""
        with pytest.raises(TextQueryTranslationError):
            text_query_field_queries({"match": {"terms": "apple"}}, (), self.COLUMNS)


class TestFusionReplay:
    """Hybrid fusion replay matches the recorded strategy and the Rust fusion math."""

    def test_rrf_matches_formula(self) -> None:
        """Reciprocal-rank fusion accrues 1 / (k + rank) per leg and ranks the shared row first."""
        fused = fuse_legs({"rrf": {"k": 60}}, [1, 2], [0.1, 0.2], [2, 3], [5.0, 4.0], 10)
        assert fused == [2, 1, 3]

    def test_rrf_custom_constant_and_truncation(self) -> None:
        """A custom rank constant and a small k truncate to the fused best."""
        fused = fuse_legs({"rrf": {"k": 1}}, [7, 8], [0.1, 0.2], [7], [5.0], 1)
        assert fused == [7]

    def test_weighted_normalized_sum(self) -> None:
        """Weighted fusion forms vector_weight * vector_norm + (1 - vector_weight) * text_norm."""
        fused = fuse_legs({"weighted": {"vector_weight": 0.7}}, [1, 2], [0.0, 1.0], [2, 3], [3.0, 1.0], 10)
        assert fused == [1, 2, 3]

    def test_weighted_single_element_leg_contributes_full_weight(self) -> None:
        """A single-hit leg normalizes to 1.0 so its row carries the leg's full weight."""
        fused = fuse_legs({"weighted": {"vector_weight": 0.2}}, [1], [0.0], [2], [9.0], 10)
        assert fused == [2, 1]

    @pytest.mark.parametrize(
        "spec",
        [
            {"rrf": {"k": 0}},
            {"rrf": {"k": -1}},
            {"weighted": {"vector_weight": 1.5}},
            {"weighted": {"vector_weight": -0.1}},
            {"unknown": {}},
            {"rrf": {"k": 1}, "weighted": {}},
        ],
    )
    def test_invalid_specs_rejected(self, spec: Any) -> None:
        """Out-of-range parameters and unknown shapes are rejected."""
        with pytest.raises(FusionReplayError):
            fuse_legs(spec, [1], [0.0], [2], [1.0], 10)


class TestParsingNewAttributes:
    """Text and hybrid captures parse their extra attributes and skip when required ones are missing."""

    def base(self, **overrides: Any) -> dict[str, Any]:
        """Build a flat attribute dictionary without the query-type-specific fields.

        Args:
            overrides: Attribute overrides keyed by the suffix after ``recall.``.

        Returns:
            The flat attribute dictionary.
        """
        attrs: dict[str, Any] = {
            "recall.sample": "true",
            "recall.sample_id": "abc",
            "recall.captured_at_unix_ms": "1500",
            "recall.org_id": "acme",
            "recall.tenant_id": "tenant1",
            "recall.namespace": "ns1",
            "recall.dataset_version": "1",
            "recall.k": "3",
            "recall.result_ids": json.dumps([0, 1, 2]),
        }
        for key, value in overrides.items():
            full: str = f"recall.{key}"
            if value is None:
                attrs.pop(full, None)
            else:
                attrs[full] = value
        return attrs

    def test_legacy_capture_defaults_to_vector(self) -> None:
        """A capture without a query type parses as a vector query."""
        sample = parse_recall_sample(self.base(query_vector=json.dumps([0.1] * DIM)))
        assert sample.query_type == "vector"

    def test_text_capture_parses(self) -> None:
        """A text capture parses its node tree, columns, and scores without a query vector."""
        node: dict[str, Any] = {"match": {"terms": "apple", "operator": "or"}}
        sample = parse_recall_sample(
            self.base(
                query_type="text",
                text_query=json.dumps(node),
                text_columns=json.dumps(["body"]),
                result_scores=json.dumps([3.0, 2.0, 1.0]),
            )
        )
        assert sample.query_type == "text"
        assert sample.query_vector == ()
        assert sample.text_query == node
        assert sample.text_columns == ("body",)
        assert sample.result_scores == (3.0, 2.0, 1.0)

    def test_hybrid_capture_parses(self) -> None:
        """A hybrid capture parses the vector, text, and fusion attributes together."""
        node: dict[str, Any] = {"match": {"terms": "apple", "operator": "or"}}
        fusion: dict[str, Any] = {"rrf": {"k": 60}}
        sample = parse_recall_sample(
            self.base(
                query_type="hybrid",
                query_vector=json.dumps([0.1] * DIM),
                text_query=json.dumps(node),
                text_columns=json.dumps(["body"]),
                fusion=json.dumps(fusion),
            )
        )
        assert sample.query_type == "hybrid"
        assert sample.fusion == fusion
        assert len(sample.query_vector) == DIM

    @pytest.mark.parametrize(
        ("overrides", "reason"),
        [
            ({"query_type": "image"}, "invalid:recall.query_type"),
            ({"query_type": "text", "text_columns": json.dumps(["body"])}, "missing:recall.text_query"),
            (
                {"query_type": "text", "text_query": json.dumps({"match": {"terms": "a"}})},
                "missing:recall.text_columns",
            ),
            (
                {
                    "query_type": "text",
                    "text_query": json.dumps({"match": {"terms": "a"}}),
                    "text_columns": "[]",
                },
                "invalid:recall.text_columns",
            ),
            (
                {
                    "query_type": "hybrid",
                    "query_vector": json.dumps([0.1] * DIM),
                    "text_query": json.dumps({"match": {"terms": "a"}}),
                    "text_columns": json.dumps(["body"]),
                },
                "missing:recall.fusion",
            ),
            (
                {
                    "query_type": "text",
                    "text_query": json.dumps({"match": {"terms": "a"}}),
                    "text_columns": json.dumps(["body"]),
                    "result_scores": '["x"]',
                },
                "invalid:recall.result_scores",
            ),
        ],
    )
    def test_malformed_captures_skip_with_reason(self, overrides: dict[str, Any], reason: str) -> None:
        """Missing or malformed query-type-specific attributes skip with a bounded reason."""
        samples, skips = parse_samples([self.base(**overrides)])
        assert samples == []
        assert skips == {reason: 1}


class TestTextScoringEndToEnd:
    """Text samples score against the exact BM25 reference at the pinned version."""

    def write_dataset(self, base_uri: str) -> int:
        """Write the text dataset and return its version.

        Args:
            base_uri: The job base URI.

        Returns:
            The written dataset version.
        """
        uri: str = f"{base_uri}/acme/tenant1/txt.lance"
        bodies: list[str] = ["apple apple apple", "apple apple", "apple", "banana"]
        lance.write_dataset(text_table([0, 1, 2, 3], bodies, np.zeros((4, DIM), dtype=np.float32)), uri)
        return lance.dataset(uri).version

    def record(self, version: int, result_ids: list[int]) -> dict[str, Any]:
        """Build a text recall span record.

        Args:
            version: The pinned dataset version.
            result_ids: The served result ids in rank order.

        Returns:
            The flat attribute dictionary.
        """
        return {
            "recall.sample": "true",
            "recall.sample_id": "txt-1",
            "recall.captured_at_unix_ms": "1500",
            "recall.org_id": "acme",
            "recall.tenant_id": "tenant1",
            "recall.namespace": "txt",
            "recall.dataset_version": str(version),
            "recall.k": "3",
            "recall.query_type": "text",
            "recall.text_query": json.dumps({"match": {"terms": "apple", "operator": "or"}}),
            "recall.text_columns": json.dumps(["body"]),
            "recall.result_ids": json.dumps(result_ids),
            "recall.result_scores": json.dumps([3.0, 2.0, 1.0]),
        }

    def test_exact_served_order_scores_perfect(self, job_config: RecallJobConfig) -> None:
        """Serving the exact BM25 order scores recall, nDCG, and MRR of 1.0 for the text query type."""
        version: int = self.write_dataset(job_config.base_uri)
        source: InMemorySpanSource = InMemorySpanSource(records=[self.record(version, [0, 1, 2])])
        report: RecallReport = RecallAuditJob(job_config).run(FakeSpark(), source, 1000, 2000)
        overall = report.rows[0]
        assert overall.samples == 1
        assert overall.mean_recall == pytest.approx(1.0)
        assert overall.mean_ndcg == pytest.approx(1.0)
        assert overall.mean_mrr == pytest.approx(1.0)
        assert "query_type text" in [row.bucket for row in report.rows]

    def test_stale_served_order_lowers_ndcg(self, job_config: RecallJobConfig) -> None:
        """A served order disagreeing with the pinned exact ranking keeps recall high but lowers nDCG and MRR."""
        version: int = self.write_dataset(job_config.base_uri)
        source: InMemorySpanSource = InMemorySpanSource(records=[self.record(version, [2, 1, 0])])
        report: RecallReport = RecallAuditJob(job_config).run(FakeSpark(), source, 1000, 2000)
        score: SampleScore = report.scores[0]
        assert score.recall == pytest.approx(1.0)
        assert score.ndcg is not None and score.ndcg < 1.0
        assert score.mrr == pytest.approx(1.0 / 3.0)


class TestHybridScoringEndToEnd:
    """Hybrid samples fuse exact vector and exact BM25 references with the recorded strategy."""

    def write_dataset(self, base_uri: str) -> int:
        """Write the hybrid dataset and return its version.

        Args:
            base_uri: The job base URI.

        Returns:
            The written dataset version.
        """
        uri: str = f"{base_uri}/acme/tenant1/hyb.lance"
        vectors: np.ndarray = np.array(
            [[0.0] * DIM, [1.0] + [0.0] * (DIM - 1), [2.0] + [0.0] * (DIM - 1), [3.0] + [0.0] * (DIM - 1)],
            dtype=np.float32,
        )
        bodies: list[str] = ["plum", "pear", "apple apple", "apple"]
        lance.write_dataset(text_table([0, 1, 2, 3], bodies, vectors), uri)
        return lance.dataset(uri).version

    def record(self, version: int, result_ids: list[int]) -> dict[str, Any]:
        """Build a hybrid recall span record.

        Args:
            version: The pinned dataset version.
            result_ids: The served result ids in rank order.

        Returns:
            The flat attribute dictionary.
        """
        return {
            "recall.sample": "true",
            "recall.sample_id": "hyb-1",
            "recall.captured_at_unix_ms": "1500",
            "recall.org_id": "acme",
            "recall.tenant_id": "tenant1",
            "recall.namespace": "hyb",
            "recall.dataset_version": str(version),
            "recall.k": "4",
            "recall.query_type": "hybrid",
            "recall.query_vector": json.dumps([0.0] * DIM),
            "recall.text_query": json.dumps({"match": {"terms": "apple", "operator": "or"}}),
            "recall.text_columns": json.dumps(["body"]),
            "recall.fusion": json.dumps({"rrf": {"k": 60}}),
            "recall.result_ids": json.dumps(result_ids),
            "recall.result_scores": json.dumps([0.03, 0.03, 0.02, 0.02]),
        }

    def test_recorded_rrf_fusion_scores_served_order(self, job_config: RecallJobConfig) -> None:
        """The recorded RRF fusion of the exact legs grades the served order, perfect when it matches."""
        version: int = self.write_dataset(job_config.base_uri)
        source: InMemorySpanSource = InMemorySpanSource(records=[self.record(version, [2, 3, 0, 1])])
        report: RecallReport = RecallAuditJob(job_config).run(FakeSpark(), source, 1000, 2000)
        score: SampleScore = report.scores[0]
        assert score.query_type == "hybrid"
        assert score.recall == pytest.approx(1.0)
        assert score.ndcg == pytest.approx(1.0)
        assert score.mrr == pytest.approx(1.0)
        assert "query_type hybrid" in [row.bucket for row in report.rows]

    def test_wrong_top_result_drops_mrr(self, job_config: RecallJobConfig) -> None:
        """A served order that buries the fused top result keeps recall at 1.0 but lowers MRR."""
        version: int = self.write_dataset(job_config.base_uri)
        source: InMemorySpanSource = InMemorySpanSource(records=[self.record(version, [3, 0, 1, 2])])
        report: RecallReport = RecallAuditJob(job_config).run(FakeSpark(), source, 1000, 2000)
        score: SampleScore = report.scores[0]
        assert score.recall == pytest.approx(1.0)
        assert score.mrr == pytest.approx(1.0 / 4.0)
