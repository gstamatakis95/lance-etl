"""Unit tests for the bench cluster-seeded corpus: determinism, vocabulary distinctness, clustering, and routing."""

from __future__ import annotations

import numpy as np

from bench.corpus import (
    assign_clusters,
    batch_row_texts,
    build_vocabulary,
    row_text,
    tenant_for_index,
    train_centroids,
)


class TestVocabulary:
    """build_vocabulary is deterministic and produces disjoint vocabularies."""

    def test_same_seed_same_vocabulary(self) -> None:
        """Two builds with the same seed are identical."""
        first = build_vocabulary(8, 10, 5, seed=99)
        second = build_vocabulary(8, 10, 5, seed=99)
        assert first == second

    def test_different_seed_different_vocabulary(self) -> None:
        """A different seed yields a different vocabulary."""
        assert build_vocabulary(8, 10, 5, seed=1) != build_vocabulary(8, 10, 5, seed=2)

    def test_cluster_vocabularies_disjoint(self) -> None:
        """Cluster vocabularies are pairwise disjoint and disjoint from the common pool."""
        clusters, common = build_vocabulary(6, 12, 7, seed=3)
        pools: list[set[str]] = [set(words) for words in clusters] + [set(common)]
        for i in range(len(pools)):
            for j in range(i + 1, len(pools)):
                assert not pools[i] & pools[j]

    def test_sizes(self) -> None:
        """The requested vocabulary sizes are honored."""
        clusters, common = build_vocabulary(4, 9, 6, seed=8)
        assert len(clusters) == 4
        assert all(len(words) == 9 for words in clusters)
        assert len(common) == 6


class TestRowText:
    """row_text is deterministic and draws from the right vocabularies."""

    def test_same_seed_same_text(self) -> None:
        """The same (seed, cluster, index) always produces the same document."""
        clusters, common = build_vocabulary(4, 10, 5, seed=7)
        assert row_text(clusters, common, 2, 1234, seed=7) == row_text(clusters, common, 2, 1234, seed=7)

    def test_different_index_different_text(self) -> None:
        """Different rows get different documents."""
        clusters, common = build_vocabulary(4, 10, 5, seed=7)
        assert row_text(clusters, common, 2, 1, seed=7) != row_text(clusters, common, 2, 2, seed=7)

    def test_terms_come_from_cluster_and_common_pools(self) -> None:
        """Every term belongs to the row's cluster vocabulary or the common pool."""
        clusters, common = build_vocabulary(4, 10, 5, seed=7)
        terms: list[str] = row_text(clusters, common, 3, 42, seed=7, cluster_terms=6, common_terms=2).split()
        allowed: set[str] = set(clusters[3]) | set(common)
        assert all(term in allowed for term in terms)
        assert sum(term in set(clusters[3]) for term in terms) == 6


class TestBatchRowTexts:
    """batch_row_texts is the vectorized per-batch equivalent of row_text."""

    def test_same_seed_same_batch(self) -> None:
        """The same seed and batch produce the same documents."""
        clusters, common = build_vocabulary(4, 10, 5, seed=7)
        cluster_ids: np.ndarray = np.array([0, 1, 2, 3, 0, 1], dtype=np.int64)
        indices: np.ndarray = np.arange(100, 106, dtype=np.int64)
        first: list[str] = batch_row_texts(clusters, common, cluster_ids, indices, seed=7)
        second: list[str] = batch_row_texts(clusters, common, cluster_ids, indices, seed=7)
        assert first == second

    def test_different_batch_key_different_text(self) -> None:
        """A different first global index (batch key) changes the generated documents."""
        clusters, common = build_vocabulary(4, 10, 5, seed=7)
        cluster_ids: np.ndarray = np.array([0, 1, 2, 3], dtype=np.int64)
        first: list[str] = batch_row_texts(clusters, common, cluster_ids, np.arange(0, 4, dtype=np.int64), seed=7)
        second: list[str] = batch_row_texts(
            clusters, common, cluster_ids, np.arange(1000, 1004, dtype=np.int64), seed=7
        )
        assert first != second

    def test_terms_come_from_each_rows_own_cluster_and_common_pools(self) -> None:
        """Every row's cluster terms are drawn only from that row's own cluster vocabulary."""
        clusters, common = build_vocabulary(4, 10, 5, seed=11)
        cluster_ids: np.ndarray = np.array([0, 1, 2, 3, 2, 1, 0, 3], dtype=np.int64)
        indices: np.ndarray = np.arange(200, 208, dtype=np.int64)
        texts: list[str] = batch_row_texts(
            clusters, common, cluster_ids, indices, seed=11, cluster_terms=6, common_terms=2
        )
        for cluster_id, text in zip(cluster_ids, texts, strict=True):
            terms: list[str] = text.split()
            allowed: set[str] = set(clusters[int(cluster_id)]) | set(common)
            assert all(term in allowed for term in terms)
            assert sum(term in set(clusters[int(cluster_id)]) for term in terms) == 6

    def test_empty_batch_returns_empty_list(self) -> None:
        """A zero-length batch returns an empty list without constructing a generator."""
        clusters, common = build_vocabulary(4, 10, 5, seed=1)
        assert (
            batch_row_texts(clusters, common, np.array([], dtype=np.int64), np.array([], dtype=np.int64), seed=1) == []
        )


class TestTenantRouting:
    """tenant_for_index implements the round-robin split."""

    def test_round_robin_assignment(self) -> None:
        """Index i routes to tenant i mod N."""
        assert [tenant_for_index(i, 3) for i in range(7)] == [0, 1, 2, 0, 1, 2, 0]

    def test_balanced_counts(self) -> None:
        """The split is balanced to within one row."""
        assignments: list[int] = [tenant_for_index(i, 4) for i in range(10)]
        counts: list[int] = [assignments.count(t) for t in range(4)]
        assert counts == [3, 3, 2, 2]

    def test_single_tenant(self) -> None:
        """One tenant receives everything."""
        assert all(tenant_for_index(i, 1) == 0 for i in range(5))


class TestClustering:
    """Minibatch k-means separates well-separated blobs deterministically."""

    def test_blobs_assigned_consistently(self) -> None:
        """Points within one blob share a cluster and blobs differ."""
        rng: np.random.Generator = np.random.default_rng(0)
        blob_a: np.ndarray = rng.normal(loc=0.0, scale=0.1, size=(200, 8)).astype(np.float32)
        blob_b: np.ndarray = rng.normal(loc=10.0, scale=0.1, size=(200, 8)).astype(np.float32)
        sample: np.ndarray = np.concatenate([blob_a, blob_b])
        centroids: np.ndarray = train_centroids(sample, 2, seed=5)
        assigned: np.ndarray = assign_clusters(sample, centroids)
        assert len(set(assigned[:200].tolist())) == 1
        assert len(set(assigned[200:].tolist())) == 1
        assert assigned[0] != assigned[200]

    def test_training_deterministic(self) -> None:
        """The same seed trains identical centroids."""
        rng: np.random.Generator = np.random.default_rng(1)
        sample: np.ndarray = rng.normal(size=(300, 6)).astype(np.float32)
        np.testing.assert_array_equal(train_centroids(sample, 4, seed=9), train_centroids(sample, 4, seed=9))

    def test_cluster_count_clamped(self) -> None:
        """Requesting more clusters than samples clamps to the sample size."""
        sample: np.ndarray = np.eye(3, dtype=np.float32)
        assert train_centroids(sample, 10, seed=2).shape == (3, 3)
