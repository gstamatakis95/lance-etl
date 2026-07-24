"""Deterministic cluster-seeded text corpus and tenant routing for the benchmark.

Every base vector is assigned to a coarse k-means cluster (numpy minibatch k-means trained on a sample) and its text is
drawn from that cluster's private vocabulary plus a small shared common-word pool, so BM25 sees realistic term
distributions: cluster terms are discriminative, common terms are background noise.

Two document-text generators are provided. :func:`row_text` seeds one generator per row from
``(seed, cluster, global index)`` and is used by the prepare phase and by
:meth:`~bench.datasets.DatasetAdapter.text_for_row`, so the same seed always yields the identical
corpus regardless of how rows are sliced across Spark tasks. :func:`batch_row_texts` is the
vectorized ingest-path equivalent used by the production reconciler benchmark: it seeds exactly one
generator per Spark task batch (keyed by the batch's first global index) and draws every row's
terms with one vectorized random-key top-k selection per cluster group instead of constructing a
fresh generator per row, which otherwise dominates per-row ingest cost at scale. Both generators
draw each row's cluster-specific terms only from that row's own cluster vocabulary, which is what
makes the FTS hit-rate check provable. Per-row RNG-key compatibility between the two generators is
not preserved or required, only per-run determinism.
"""

from __future__ import annotations

import numpy as np

LETTERS: str = "abcdefghijklmnopqrstuvwxyz"
MIN_WORD_LENGTH: int = 5
MAX_WORD_LENGTH: int = 9


def generate_words(rng: np.random.Generator, count: int, taken: set[str]) -> list[str]:
    """Generate unique pronounceable-ish benchmark vocabulary words.

    Args:
        rng: The seeded generator to draw letters from.
        count: Number of unique words to produce.
        taken: Words already in use. Extended in place with the new words.

    Returns:
        The freshly generated words.
    """
    words: list[str] = []
    while len(words) < count:
        length: int = int(rng.integers(MIN_WORD_LENGTH, MAX_WORD_LENGTH))
        word: str = "".join(LETTERS[i] for i in rng.integers(0, len(LETTERS), size=length))
        if word not in taken:
            taken.add(word)
            words.append(word)
    return words


def build_vocabulary(
    num_clusters: int, words_per_cluster: int, common_count: int, seed: int
) -> tuple[list[list[str]], list[str]]:
    """Build per-cluster vocabularies and the shared common-word pool.

    All words across all clusters and the common pool are pairwise distinct, so each cluster vocabulary is fully
    discriminative for BM25.

    Args:
        num_clusters: Number of cluster vocabularies.
        words_per_cluster: Words per cluster vocabulary.
        common_count: Size of the shared common pool.
        seed: Seed making the vocabulary deterministic.

    Returns:
        The cluster vocabularies and the common pool.
    """
    rng: np.random.Generator = np.random.default_rng([seed, 1])
    taken: set[str] = set()
    clusters: list[list[str]] = [generate_words(rng, words_per_cluster, taken) for _ in range(num_clusters)]
    common: list[str] = generate_words(rng, common_count, taken)
    return clusters, common


def row_text(
    cluster_vocab: list[list[str]],
    common_vocab: list[str],
    cluster_id: int,
    global_index: int,
    seed: int,
    cluster_terms: int = 8,
    common_terms: int = 2,
) -> str:
    """Build the deterministic cluster-seeded document for one vector.

    Args:
        cluster_vocab: Per-cluster vocabularies.
        common_vocab: Shared common-word pool.
        cluster_id: The vector's coarse cluster.
        global_index: The vector's global row index.
        seed: The corpus seed.
        cluster_terms: Cluster-specific words per document.
        common_terms: Common words per document.

    Returns:
        The space-joined document text.
    """
    rng: np.random.Generator = np.random.default_rng([seed, 2, int(cluster_id), int(global_index)])
    vocabulary: list[str] = cluster_vocab[int(cluster_id)]
    picked: list[str] = list(rng.choice(vocabulary, size=min(cluster_terms, len(vocabulary)), replace=False))
    picked.extend(rng.choice(common_vocab, size=min(common_terms, len(common_vocab)), replace=False))
    return " ".join(picked)


def batch_row_texts(
    cluster_vocab: list[list[str]],
    common_vocab: list[str],
    clusters: np.ndarray,
    indices: np.ndarray,
    seed: int,
    cluster_terms: int = 8,
    common_terms: int = 2,
) -> list[str]:
    """Build deterministic cluster-seeded documents for a whole ingest batch in one vectorized pass.

    Exactly one seeded generator is created for the whole batch, keyed by the corpus seed and the
    batch's first global index (``indices[0]``), which is stable across repeated runs of the same
    seed because the ingest phase always slices the corpus into the same contiguous ordinal
    windows. This replaces constructing a fresh :func:`numpy.random.default_rng` per row, which is
    the dominant per-row cost of ingest at scale.

    Rows are grouped by cluster so each row's cluster-specific terms are still drawn exclusively
    from that row's own cluster vocabulary, preserving the invariant that makes the FTS hit-rate
    check provable. Per-row sampling without replacement is vectorized with the random-key top-k
    trick: one uniform random key is drawn per ``(row, candidate word)`` pair within a group, and
    the ``cluster_terms`` (respectively ``common_terms``) smallest keys per row select that row's
    words, which is equivalent in distribution to independent per-row ``rng.choice(replace=False)``
    calls. Per-row RNG-key compatibility with :func:`row_text` is not preserved or required, only
    per-run determinism is.

    Args:
        cluster_vocab: Per-cluster vocabularies.
        common_vocab: Shared common-word pool.
        clusters: Per-row coarse cluster assignment for the batch.
        indices: Per-row global row index for the batch, the same length as ``clusters``.
        seed: The corpus seed.
        cluster_terms: Cluster-specific words per document.
        common_terms: Common words per document.

    Returns:
        One space-joined document per row, in input order.
    """
    batch_size: int = len(clusters)
    texts: list[str] = [""] * batch_size
    if batch_size == 0:
        return texts
    rng: np.random.Generator = np.random.default_rng([seed, 2, int(indices[0])])
    common_pool: np.ndarray = np.asarray(common_vocab)
    common_size: int = min(common_terms, len(common_vocab))
    common_keys: np.ndarray = rng.random((batch_size, len(common_vocab)))
    common_order: np.ndarray = np.argsort(common_keys, axis=1)[:, :common_size]
    common_words: np.ndarray = common_pool[common_order]
    for cluster_id in np.unique(clusters):
        rows: np.ndarray = np.nonzero(clusters == cluster_id)[0]
        vocabulary: np.ndarray = np.asarray(cluster_vocab[int(cluster_id)])
        group_size: int = min(cluster_terms, len(vocabulary))
        cluster_keys: np.ndarray = rng.random((len(rows), len(vocabulary)))
        cluster_order: np.ndarray = np.argsort(cluster_keys, axis=1)[:, :group_size]
        cluster_words: np.ndarray = vocabulary[cluster_order]
        for position, row in enumerate(rows):
            texts[row] = " ".join([*cluster_words[position], *common_words[row]])
    return texts


def tenant_for_index(global_index: int, tenants: int) -> int:
    """Return the tenant a vector is routed to under the round-robin split.

    Args:
        global_index: The vector's global row index.
        tenants: Total tenant count.

    Returns:
        The zero-based tenant number.
    """
    return global_index % tenants


def nearest_centroids(block: np.ndarray, centroids: np.ndarray) -> np.ndarray:
    """Assign each row of a block to its nearest centroid by L2 distance.

    Args:
        block: A ``(rows, dim)`` float array.
        centroids: A ``(clusters, dim)`` float array.

    Returns:
        An int32 array of cluster assignments.
    """
    block32: np.ndarray = np.asarray(block, dtype=np.float32)
    centroid_norms: np.ndarray = np.sum(centroids * centroids, axis=1)
    scores: np.ndarray = block32 @ centroids.T * -2.0 + centroid_norms[None, :]
    return np.argmin(scores, axis=1).astype(np.int32)


def train_centroids(
    sample: np.ndarray, num_clusters: int, seed: int, iterations: int = 25, batch_size: int = 4_096
) -> np.ndarray:
    """Train coarse cluster centroids with numpy minibatch k-means.

    Args:
        sample: A ``(rows, dim)`` sample of the corpus vectors.
        num_clusters: Desired centroid count. Clamped to the sample size.
        seed: Seed for initialization and minibatch sampling.
        iterations: Minibatch update rounds.
        batch_size: Rows per minibatch.

    Returns:
        A float32 ``(clusters, dim)`` centroid array.
    """
    sample32: np.ndarray = np.asarray(sample, dtype=np.float32)
    clusters: int = min(num_clusters, len(sample32))
    rng: np.random.Generator = np.random.default_rng([seed, 3])
    initial: np.ndarray = rng.choice(len(sample32), size=clusters, replace=False)
    centroids: np.ndarray = sample32[initial].copy()
    counts: np.ndarray = np.ones(clusters, dtype=np.float64)
    for _ in range(iterations):
        picked: np.ndarray = rng.integers(0, len(sample32), size=min(batch_size, len(sample32)))
        block: np.ndarray = sample32[picked]
        assigned: np.ndarray = nearest_centroids(block, centroids)
        for cluster in np.unique(assigned):
            members: np.ndarray = block[assigned == cluster]
            counts[cluster] += len(members)
            rate: float = len(members) / counts[cluster]
            centroids[cluster] = (1.0 - rate) * centroids[cluster] + rate * members.mean(axis=0)
    return centroids


def assign_clusters(vectors: np.ndarray, centroids: np.ndarray, chunk: int = 100_000) -> np.ndarray:
    """Assign every vector to its nearest centroid, processing in chunks.

    Args:
        vectors: A ``(rows, dim)`` float array.
        centroids: A ``(clusters, dim)`` float32 centroid array.
        chunk: Rows per distance-computation chunk.

    Returns:
        An int32 array of cluster assignments.
    """
    parts: list[np.ndarray] = []
    for start in range(0, len(vectors), chunk):
        parts.append(nearest_centroids(vectors[start : start + chunk], centroids))
    if not parts:
        return np.empty(0, dtype=np.int32)
    return np.concatenate(parts)
