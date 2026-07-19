"""Build the Iceberg source table, cluster-seeded text corpus, and ground-truth artifacts.

The base vectors are written into a local Iceberg table with exactly the schema the project's ETL
expects: the routing columns (``org_id``, ``tenant_id``, ``namespace``), the merge key
``record_id``, the operation column ``op``, the timestamp column ``updated_at`` (used as both the
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
brute-force recomputation fanned out over Spark: each executor task loads only its own base-vector slice, computes
per-tenant partial top-k bounded by the ground-truth depth against the broadcast-by-closure queries, and the driver
reduces the partials into exact global top-k ids. Cluster assignment for FTS scoring rides the same fan-out, so the
driver never loads the base matrix.

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
from bench.groundtruth import brute_force_topk_scored, merge_topk_partials
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
    "org_id string, tenant_id string, namespace string, record_id string, op string, updated_at_us long, "
    "category string, vectors map<string,array<float>>, texts map<string,string>, "
    "metadata map<string,string>"
)

SPARK_ROW_DDL_NO_TEXT: str = (
    "org_id string, tenant_id string, namespace string, record_id string, op string, updated_at_us long, "
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
        ("record_id", pa.string()),
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


def published_ground_truth(config: BenchConfig, adapter: DatasetAdapter) -> np.ndarray | None:
    """Load the adapter's published ground truth when it applies to this run.

    The published truth is usable verbatim only for the canonical full single-tenant run. Any ``--limit`` subset,
    multi-tenant split, or adapter without published truth returns ``None`` so the caller recomputes it exactly.

    Args:
        config: Benchmark configuration.
        adapter: The dataset adapter.

    Returns:
        The published ``(num_queries, depth)`` int64 ground truth, or ``None`` when it does not apply.
    """
    if config.limit == adapter.base_count and config.tenants == 1:
        return adapter.ground_truth(config.corpus_root)
    return None


def slice_prepare_artifacts(
    start: int,
    count: int,
    adapter: DatasetAdapter,
    corpus_root: Path,
    centroids: np.ndarray,
    queries: np.ndarray | None,
    tenants: int,
    depth: int,
) -> dict[str, Any]:
    """Compute one base-vector slice's cluster assignments and per-tenant ground-truth partials.

    Runs inside a Spark executor task: loads only its own slice through the pickled adapter, assigns clusters against
    the broadcast-by-closure centroids, and (when ``queries`` is set) computes an exact partial top-k per tenant
    bounded by ``depth``, sized for the driver-side reduce in :func:`merge_topk_partials`.

    Args:
        start: First global row index of the slice.
        count: Rows in the slice.
        adapter: The dataset adapter, pickled into the task closure.
        corpus_root: The shared corpus cache directory, readable from the executor.
        centroids: Broadcast-by-closure k-means centroids.
        queries: Broadcast-by-closure query matrix, or ``None`` when published ground truth is used.
        tenants: Round-robin tenant count.
        depth: Ground-truth neighbors per query.

    Returns:
        The slice ``start``, its int32 ``clusters``, and per-org ``(ids, distances)`` ``partials``.
    """
    vectors: np.ndarray = adapter.base_vector_slice(corpus_root, start, count)
    clusters: np.ndarray = assign_clusters(vectors, centroids)
    partials: dict[str, tuple[np.ndarray, np.ndarray]] = {}
    if queries is not None:
        ids: np.ndarray = np.arange(start, start + count, dtype=np.int64)
        for tenant in range(tenants):
            mask: np.ndarray = ids % tenants == tenant
            if mask.any():
                partials[f"org{tenant}"] = brute_force_topk_scored(vectors[mask], ids[mask], queries, depth)
    return {"start": start, "clusters": clusters, "partials": partials}


def fan_out_prepare_artifacts(
    config: BenchConfig, adapter: DatasetAdapter, centroids: np.ndarray, queries: np.ndarray | None
) -> tuple[np.ndarray, dict[str, np.ndarray]]:
    """Compute the cluster artifact and brute-force ground truth in one Spark fan-out.

    Each executor task handles one base-vector slice via :func:`slice_prepare_artifacts`. The driver concatenates the
    cluster slices in ascending start order and reduces the per-tenant partial top-k results into exact global ids —
    the same partial-top-k reduce shape the recall job's large tier uses. The driver never loads the base matrix.

    Args:
        config: Benchmark configuration.
        adapter: The dataset adapter.
        centroids: Trained cluster centroids.
        queries: The query matrix, or ``None`` to skip ground-truth computation.

    Returns:
        The int32 cluster ids indexed by global record id, and one ``(num_queries, depth)`` int64 array per org id
        (empty when ``queries`` is ``None``).
    """
    corpus_root: Path = config.corpus_root
    tenants: int = config.tenants
    depth: int = adapter.gt_depth
    slices: list[tuple[int, int]] = [
        (start, min(config.rows_per_slice, config.limit - start))
        for start in range(0, config.limit, config.rows_per_slice)
    ]

    def compute_partition(items: Any) -> Any:
        """Compute the slice artifacts assigned to this executor task.

        Args:
            items: The ``(start, count)`` slice specs for this partition.

        Yields:
            One slice-artifact record per spec.
        """
        for start, count in items:
            yield slice_prepare_artifacts(start, count, adapter, corpus_root, centroids, queries, tenants, depth)

    logger.info("prepare artifacts fan-out: %d slices, ground truth %s", len(slices), queries is not None)
    spark = build_spark(config, "bench-prepare-artifacts")
    try:
        results: list[dict[str, Any]] = (
            spark.sparkContext.parallelize(slices, len(slices)).mapPartitions(compute_partition).collect()
        )
    finally:
        spark.stop()

    results.sort(key=lambda record: record["start"])
    clusters: np.ndarray = np.concatenate([record["clusters"] for record in results])
    ground_truth: dict[str, np.ndarray] = {}
    if queries is not None:
        for tenant in range(tenants):
            org: str = f"org{tenant}"
            partials: list[tuple[np.ndarray, np.ndarray]] = [
                record["partials"][org] for record in results if org in record["partials"]
            ]
            if not partials:
                logger.warning(
                    "no base rows for %s (limit %d, tenants %d); skipping its ground truth", org, config.limit, tenants
                )
                continue
            ground_truth[org] = merge_topk_partials(partials, depth)
    return clusters, ground_truth


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
    published: np.ndarray | None = published_ground_truth(config, adapter)
    clusters, ground_truth = fan_out_prepare_artifacts(
        config, adapter, centroids, None if published is not None else queries
    )
    if published is not None:
        ground_truth = {"org0": published}
        ground_truth_source: str = adapter.ground_truth_source
    else:
        ground_truth_source = "brute_force"

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
