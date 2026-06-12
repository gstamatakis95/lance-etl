"""Build the Iceberg source table, cluster-seeded text corpus, and ground-truth artifacts.

The base vectors are written into a local Iceberg table with exactly the schema the project's ETL
expects: the routing columns (``org_id``, ``tenant_id``, ``namespace``), the merge key
``vector_id``, the operation column ``op``, the timestamp column ``updated_at`` (used as both the
last-write-wins collapse column and the window-pushdown column), the low-cardinality concrete
``category`` column (the bitmap index target, flows through the ETL untouched), and the three map
columns ``vectors`` / ``texts`` / ``metadata``. The embedding rides in the ``vectors`` map under
the key ``"vector"`` and the cluster-seeded document rides in the ``texts`` map under
the key ``"text"``. The ETL dynamically pivots every map key into a concrete column per dataset
group on the executor, so the ``vector`` and ``text`` keys become concrete columns automatically.
The ``vector`` column type override in the ingest config ensures the inferred FSL dimension is
always correct. The ``metadata`` map carries the cluster id under ``"cluster"`` and is pivoted
into a concrete ``cluster`` string column in the dataset.

Row generation runs inside Spark executors via ``mapInArrow``: each task reads its own row slice straight through the
dataset adapter, assigns clusters against the driver-trained centroids, and emits Arrow batches. ``updated_at``
spreads rows deterministically over one base day (minute ``index % 1440``) so the ingest phase can slice the day
into ``--batches`` windows through the ETL's real ``--window-start`` / ``--window-end`` pushdown flags.

Ground truth: the dataset's published ground truth is used verbatim for the canonical full single-tenant run when the
adapter provides one. Any ``--limit`` subset, multi-tenant split, or adapter without published truth triggers an exact
batched numpy brute-force recomputation per tenant so the benchmark stays self-consistent.

All corpus access goes through the :class:`bench.datasets.DatasetAdapter` resolved from ``--dataset``: the driver reads
the queries, the k-means training sample, and the ground truth from the adapter, and each Spark executor task reads its
own base-vector slice through the pickled adapter.
"""

from __future__ import annotations

import json
import logging
import time
from collections.abc import Iterator
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import numpy as np
import pyarrow as pa
from pyspark.sql import functions as F

from bench.config import NAMESPACE, TENANT_ID, BenchConfig
from bench.corpus import assign_clusters, build_vocabulary, train_centroids
from bench.datasets import DatasetAdapter, adapter_for
from bench.groundtruth import brute_force_topk
from bench.results import ensure_dir, read_json, save_phase, utc_now, write_json
from bench.spark_session import build_spark

logger: logging.Logger = logging.getLogger(__name__)

BASE_DAY: datetime = datetime(2024, 1, 1, tzinfo=UTC)
BASE_DAY_EPOCH_US: int = int(BASE_DAY.timestamp()) * 1_000_000
MINUTES_PER_DAY: int = 1_440
MICROS_PER_MINUTE: int = 60_000_000
CATEGORY_CARDINALITY: int = 16
KMEANS_SAMPLE_ROWS: int = 100_000
SPARK_ROW_DDL: str = (
    "org_id string, tenant_id string, namespace string, vector_id string, op string, updated_at_us long, "
    "category string, vectors map<string,array<float>>, texts map<string,string>, "
    "metadata map<string,string>"
)

SPARK_ROW_DDL_NO_TEXT: str = (
    "org_id string, tenant_id string, namespace string, vector_id string, op string, updated_at_us long, "
    "category string, vectors map<string,array<float>>, "
    "metadata map<string,string>"
)


def arrow_row_schema(no_text: bool = False) -> pa.Schema:
    """Return the Arrow schema matching :data:`SPARK_ROW_DDL` or :data:`SPARK_ROW_DDL_NO_TEXT`.

    Args:
        no_text: When True, omit the ``texts`` column from the schema.

    Returns:
        The schema of the batches emitted by the generator tasks.
    """
    fields: list[tuple[str, pa.DataType]] = [
        ("org_id", pa.string()),
        ("tenant_id", pa.string()),
        ("namespace", pa.string()),
        ("vector_id", pa.string()),
        ("op", pa.string()),
        ("updated_at_us", pa.int64()),
        ("category", pa.string()),
        ("vectors", pa.map_(pa.string(), pa.list_(pa.float32()))),
    ]
    if not no_text:
        fields.append(("texts", pa.map_(pa.string(), pa.string())))
    fields.append(("metadata", pa.map_(pa.string(), pa.string())))
    return pa.schema(fields)


def updated_at_micros(global_index: np.ndarray) -> np.ndarray:
    """Return the deterministic ``updated_at`` epoch microseconds per row.

    Rows are spread round-robin over the minutes of one base day, so every batch window selected by the ingest
    phase contains rows for every tenant.

    Args:
        global_index: Global row indices.

    Returns:
        Epoch microseconds as int64.
    """
    minutes: np.ndarray = (global_index % MINUTES_PER_DAY).astype(np.int64)
    return BASE_DAY_EPOCH_US + minutes * MICROS_PER_MINUTE


def single_entry_map(keys: list[str], items: pa.Array) -> pa.MapArray:
    """Build a map array with exactly one entry per row.

    Args:
        keys: One key per row.
        items: One value per row.

    Returns:
        The map array.
    """
    offsets: pa.Array = pa.array(np.arange(len(keys) + 1, dtype=np.int32))
    return pa.MapArray.from_arrays(offsets, pa.array(keys, pa.string()), items)


def slice_record_batch(
    start: int,
    count: int,
    adapter: DatasetAdapter,
    corpus_root: Path,
    centroids: np.ndarray,
    cluster_vocab: list[list[str]],
    common_vocab: list[str],
    tenants: int,
    seed: int,
    words_per_text: int,
    no_text: bool = False,
) -> pa.RecordBatch:
    """Build the Arrow batch for one contiguous slice of base vectors.

    Runs inside a Spark executor task: reads its own slice through the pickled dataset adapter, assigns clusters, and
    generates the deterministic per-row text and routing columns. When ``no_text`` is True the ``texts`` column is
    omitted entirely, keeping the schema consistent with :func:`arrow_row_schema` called with ``no_text=True``.

    Args:
        start: First global row index of the slice.
        count: Rows in the slice.
        adapter: The dataset adapter, pickled into the task closure.
        corpus_root: The shared corpus cache directory, readable from the executor.
        centroids: Broadcast-by-closure k-means centroids.
        cluster_vocab: Per-cluster vocabularies.
        common_vocab: Shared common-word pool.
        tenants: Round-robin tenant count.
        seed: Corpus seed.
        words_per_text: Cluster-specific words per document.
        no_text: When True, omit the ``texts`` column from the returned batch.

    Returns:
        One record batch conforming to :func:`arrow_row_schema` with the same ``no_text`` setting.
    """
    vectors: np.ndarray = adapter.base_vector_slice(corpus_root, start, count)
    clusters: np.ndarray = assign_clusters(vectors, centroids)
    indices: np.ndarray = np.arange(start, start + count, dtype=np.int64)
    org_ids: list[str] = [f"org{int(i) % tenants}" for i in indices]
    flat_offsets: pa.Array = pa.array(np.arange(count + 1, dtype=np.int32) * vectors.shape[1])
    vector_items: pa.ListArray = pa.ListArray.from_arrays(flat_offsets, pa.array(vectors.ravel(), pa.float32()))
    arrays: list[pa.Array] = [
        pa.array(org_ids, pa.string()),
        pa.array([TENANT_ID] * count, pa.string()),
        pa.array([NAMESPACE] * count, pa.string()),
        pa.array([str(int(i)) for i in indices], pa.string()),
        pa.array(["insert"] * count, pa.string()),
        pa.array(updated_at_micros(indices)),
        pa.array([f"cat{int(c) % CATEGORY_CARDINALITY}" for c in clusters], pa.string()),
        single_entry_map(["vector"] * count, vector_items),
    ]
    if not no_text:
        texts: list[str] = [
            adapter.text_for_row(cluster_vocab, common_vocab, int(c), int(i), seed, words_per_text)
            for c, i in zip(clusters, indices, strict=True)
        ]
        arrays.append(single_entry_map(["text"] * count, pa.array(texts, pa.string())))
    arrays.append(single_entry_map(["cluster"] * count, pa.array([str(int(c)) for c in clusters], pa.string())))
    return pa.RecordBatch.from_arrays(arrays, schema=arrow_row_schema(no_text=no_text))


def write_iceberg_table(
    config: BenchConfig, adapter: DatasetAdapter, centroids: np.ndarray, vocab: tuple[list[list[str]], list[str]]
) -> float:
    """Write the corpus source rows into the local Iceberg table via Spark executors.

    When ``config.no_text`` is True the ``texts`` column is omitted from both the Arrow schema and the
    Spark DDL string, so the ETL downstream never sees a text map to pivot.

    Args:
        config: Benchmark configuration.
        adapter: The dataset adapter providing executor-side base-vector slices.
        centroids: Trained cluster centroids.
        vocab: The cluster vocabularies and common pool.

    Returns:
        The wall time of the write in seconds.
    """
    spark = build_spark(config, "bench-prepare")
    corpus_root: Path = config.corpus_root
    cluster_vocab, common_vocab = vocab
    tenants: int = config.tenants
    seed: int = config.seed
    words_per_text: int = config.words_per_text
    no_text: bool = config.no_text
    slices: list[tuple[int, int]] = [
        (start, min(config.rows_per_slice, config.limit - start))
        for start in range(0, config.limit, config.rows_per_slice)
    ]

    def generate(batches: Iterator[pa.RecordBatch]) -> Iterator[pa.RecordBatch]:
        """Generate the row batches for the slice specs assigned to this task.

        Args:
            batches: Arrow batches of ``(start, count)`` slice specs.

        Yields:
            One row batch per slice spec.
        """
        for batch in batches:
            starts: list[int] = batch.column("start").to_pylist()
            counts: list[int] = batch.column("count").to_pylist()
            for start, count in zip(starts, counts, strict=True):
                yield slice_record_batch(
                    int(start),
                    int(count),
                    adapter,
                    corpus_root,
                    centroids,
                    cluster_vocab,
                    common_vocab,
                    tenants,
                    seed,
                    words_per_text,
                    no_text,
                )

    spark_ddl: str = SPARK_ROW_DDL_NO_TEXT if no_text else SPARK_ROW_DDL
    started: float = time.perf_counter()
    try:
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {config.catalog}.db")
        specs = spark.createDataFrame(slices, "start long, count long").repartition(len(slices))
        rows = specs.mapInArrow(generate, schema=spark_ddl)
        rows = rows.withColumn("updated_at", F.timestamp_micros(F.col("updated_at_us"))).drop("updated_at_us")
        rows.writeTo(config.table()).using("iceberg").createOrReplace()
        elapsed: float = time.perf_counter() - started
    finally:
        spark.stop()
    return elapsed


def tenant_ground_truth(
    config: BenchConfig, adapter: DatasetAdapter, queries: np.ndarray
) -> tuple[dict[str, np.ndarray], str]:
    """Compute or load the per-tenant ground truth as global vector ids.

    The adapter's published ground truth is used verbatim for the canonical full single-tenant run. Any subset,
    multi-tenant split, or adapter without published truth triggers an exact brute-force recomputation per tenant.

    Args:
        config: Benchmark configuration.
        adapter: The dataset adapter.
        queries: The full query matrix.

    Returns:
        One ``(num_queries, depth)`` int64 array per org id, and the manifest source label.
    """
    if config.limit == adapter.base_count and config.tenants == 1:
        published: np.ndarray | None = adapter.ground_truth(config.corpus_root)
        if published is not None:
            return {"org0": published}, adapter.ground_truth_source
    base: np.ndarray = adapter.base_vectors(config.corpus_root, limit=config.limit)
    result: dict[str, np.ndarray] = {}
    for tenant in range(config.tenants):
        ids: np.ndarray = np.arange(tenant, config.limit, config.tenants, dtype=np.int64)
        logger.info("brute-force ground truth for org%d over %d vectors", tenant, len(ids))
        result[f"org{tenant}"] = brute_force_topk(base[ids], ids, queries, adapter.gt_depth)
    return result, "brute_force"


def compute_cluster_artifact(config: BenchConfig, adapter: DatasetAdapter, centroids: np.ndarray) -> np.ndarray:
    """Assign every base vector in scope to its cluster for FTS scoring.

    Args:
        config: Benchmark configuration.
        adapter: The dataset adapter.
        centroids: Trained cluster centroids.

    Returns:
        An int32 array of cluster ids indexed by global vector id.
    """
    parts: list[np.ndarray] = []
    for start in range(0, config.limit, KMEANS_SAMPLE_ROWS):
        count: int = min(KMEANS_SAMPLE_ROWS, config.limit - start)
        parts.append(assign_clusters(adapter.base_vector_slice(config.corpus_root, start, count), centroids))
    return np.concatenate(parts)


def run_prepare(config: BenchConfig) -> dict[str, Any]:
    """Build every prepared artifact and the Iceberg source table.

    Skips the work when a manifest for the same corpus shape already exists, unless ``--force`` is set.

    Args:
        config: Benchmark configuration.

    Returns:
        The phase result document.
    """
    prepared: Path = ensure_dir(config.prepared_dir())
    manifest_path: Path = prepared / "manifest.json"
    if manifest_path.exists() and not config.force:
        manifest: dict[str, Any] = read_json(manifest_path)
        return save_phase(config, "prepare", {"skipped": True, "manifest": manifest})

    adapter: DatasetAdapter = adapter_for(config)
    queries: np.ndarray = adapter.query_vectors(config.corpus_root)
    sample: np.ndarray = adapter.base_vectors(config.corpus_root, limit=min(config.limit, KMEANS_SAMPLE_ROWS))
    centroids: np.ndarray = train_centroids(sample, config.num_clusters, config.seed)

    if config.no_text:
        vocab: tuple[list[list[str]], list[str]] = ([], [])
    else:
        vocab = build_vocabulary(len(centroids), config.words_per_cluster, config.common_words, config.seed)

    write_seconds: float = write_iceberg_table(config, adapter, centroids, vocab)
    clusters: np.ndarray = compute_cluster_artifact(config, adapter, centroids)
    ground_truth, ground_truth_source = tenant_ground_truth(config, adapter, queries)

    np.save(prepared / "queries.npy", queries)
    np.save(prepared / "centroids.npy", centroids)
    np.save(prepared / "clusters.npy", clusters)
    np.savez(prepared / "ground_truth.npz", **ground_truth)
    if not config.no_text:
        (prepared / "vocab.json").write_text(json.dumps({"clusters": vocab[0], "common": vocab[1]}), encoding="utf-8")
    manifest = {
        "created_at": utc_now(),
        "limit": config.limit,
        "tenants": config.tenants,
        "seed": config.seed,
        "num_clusters": len(centroids),
        "no_text": config.no_text,
        "table": config.table(),
        "iceberg_write_seconds": round(write_seconds, 3),
        "ground_truth_source": ground_truth_source,
    }
    write_json(manifest_path, manifest)
    return save_phase(config, "prepare", {"skipped": False, "manifest": manifest})
