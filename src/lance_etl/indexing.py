"""Distributed vector, scalar, and full-text indexing for Lance datasets.

Each index type is built by its own handler so the orchestration stays uniform while the per-type specifics live in one
place:

- :class:`VectorIndexHandler` builds an IVF_RQ index. The driver trains IVF centroids and mints one shared RaBitQ
  rotation per dataset with ``lance.lance.indices.build_rq_model``, persists both to a sidecar, and broadcasts both so
  every shard encodes with the same centroids and rotation. Segments are merged before commit.
- :class:`BTreeIndexHandler` and :class:`BitmapIndexHandler` build scalar indices through the same segment API as the
  vector handler: per-shard ``create_index_uncommitted`` followed by a driver ``commit_existing_index_segments``. Bitmap
  segments are merged into one segment first; btree segments are committed unmerged.
- :class:`FtsIndexHandler` builds a full-text (BM25) inverted index. Inverted indices use the distributed metadata-merge
  path: each shard builds its fragments under one shared index id, the driver merges the per-fragment metadata, and the
  index is published with a create-index commit.

Vector and scalar handlers index only fragments not already covered by the existing segments; pass ``rebuild`` to
reindex every fragment. Only the FTS handler rebuilds the whole index each run. Every commit retries conflicts with
exponential backoff so it coexists with concurrent ingestion and compaction.

:class:`LanceIndexer.run` orchestrates many datasets in two tiers. Small datasets (fragment count below a configurable
threshold) are batched into a single Spark job where each executor task indexes one whole dataset end-to-end with plain
``create_index`` / ``create_scalar_index``. Large datasets keep the per-dataset segment fan-out, driven concurrently
from the driver with a thread pool and Spark FAIR scheduler pools. IVF partition counts follow a size-aware policy:
``clamp(round(sqrt(rows)), 16, 4096)`` unless configured, degraded when the dataset cannot supply enough training rows,
and the vector index is skipped entirely below a configurable row floor where flat KNN is sufficient.

Requires pylance and the Datadog Agent on the executors; artifact IO uses pyarrow's filesystem layer.
"""

from __future__ import annotations

import base64
import json
import logging
import math
import uuid
from collections.abc import Callable, Iterator
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

import lance
import pyarrow as pa
from lance.dataset import Index
from lance.indices import IndicesBuilder
from lance.lance import indices as native_indices
from pyspark.sql import SparkSession

from lance_etl.cloud_storage import object_exists, read_object, resolve_filesystem, write_object
from lance_etl.telemetry import Telemetry, TelemetryConfig, commit_with_retries

logger: logging.Logger = logging.getLogger(__name__)

METRIC_TO_DISTANCE: dict[str, str] = {"l2": "l2", "cosine": "cosine", "dot": "dot"}
MIN_IVF_PARTITIONS: int = 16
MAX_IVF_PARTITIONS: int = 4096
FTS_OPTIONAL_PARAMS: tuple[str, ...] = (
    "base_tokenizer",
    "language",
    "lower_case",
    "stem",
    "remove_stop_words",
    "ascii_folding",
)


@dataclass
class IndexJobConfig:
    """Configuration for :class:`LanceIndexer`.

    Attributes:
        telemetry: Telemetry configuration.
        storage_options: Object-store options forwarded to pylance.
        vector_column: Vector column to index with IVF_RQ, if any.
        num_partitions: IVF partitions; derived as ``clamp(round(sqrt(rows)), 16, 4096)`` when unset.
        num_bits: RaBitQ bits per dimension; IVF_RQ uses 1.
        vector_min_rows: Skip the vector index below this row count; flat KNN serves small datasets.
        metric: Distance metric, such as ``L2``, ``cosine``, or ``dot``.
        distance_type: IVF training distance; derived from ``metric`` if unset.
        train_sample_rate: Rows sampled per partition when training the IVF.
        train_max_iters: Maximum k-means iterations when training the IVF.
        vector_index_name: Vector index name; defaults to ``{vector_column}_idx``.
        scalar_columns: Columns to index with btree.
        bitmap_columns: Columns to index with bitmap.
        text_columns: Columns to index with a full-text inverted index.
        fts_with_position: Store token positions for phrase queries in FTS.
        fts_base_tokenizer: FTS base tokenizer name, if not the default.
        fts_language: FTS stemming and stop-word language, if any.
        fts_lower_case: Lowercase FTS tokens when set.
        fts_stem: Apply FTS stemming when set.
        fts_remove_stop_words: Remove FTS stop words when set.
        fts_ascii_folding: Apply FTS ASCII folding when set.
        num_shards: Number of parallel builders per dataset.
        rebuild: Reindex every fragment instead of only uncovered ones.
        reuse_artifacts: Reuse the dataset's persisted IVF_RQ artifacts.
        commit_retries: Retry budget for commit conflicts.
        commit_backoff_seconds: Base backoff between commit retries.
        small_dataset_fragment_threshold: Datasets with fewer fragments are indexed whole on one executor.
        small_tier_slices: Spark partition count for the batched small-dataset job and classification job.
        driver_concurrency: Concurrent large-dataset submissions from the driver thread pool.
        scheduler_pool: Spark FAIR scheduler pool name set for large-dataset jobs.
    """

    telemetry: TelemetryConfig
    storage_options: dict[str, Any] | None = None
    vector_column: str | None = None
    num_partitions: int | None = None
    num_bits: int = 1
    vector_min_rows: int = 50_000
    metric: str = "L2"
    distance_type: str | None = None
    train_sample_rate: int = 256
    train_max_iters: int = 50
    vector_index_name: str | None = None
    scalar_columns: list[str] = field(default_factory=list)
    bitmap_columns: list[str] = field(default_factory=list)
    text_columns: list[str] = field(default_factory=list)
    fts_with_position: bool = False
    fts_base_tokenizer: str | None = None
    fts_language: str | None = None
    fts_lower_case: bool | None = None
    fts_stem: bool | None = None
    fts_remove_stop_words: bool | None = None
    fts_ascii_folding: bool | None = None
    num_shards: int = 64
    rebuild: bool = False
    reuse_artifacts: bool = True
    commit_retries: int = 20
    commit_backoff_seconds: float = 0.5
    small_dataset_fragment_threshold: int = 32
    small_tier_slices: int = 256
    driver_concurrency: int = 8
    scheduler_pool: str = "lance-indexing"

    def resolved_vector_index_name(self) -> str:
        """Return the vector index name, defaulting to ``{column}_idx``.

        Returns:
            The configured or derived vector index name.
        """
        if self.vector_index_name is not None:
            return self.vector_index_name
        return f"{self.vector_column}_idx"

    def resolved_distance_type(self) -> str:
        """Return the IVF training distance derived from the metric if unset.

        Returns:
            A Lance distance type string.
        """
        if self.distance_type is not None:
            return self.distance_type
        return METRIC_TO_DISTANCE.get(self.metric.lower(), "l2")

    def fts_params(self) -> dict[str, Any]:
        """Build the inverted-index parameters, omitting unset options.

        Returns:
            Keyword arguments for an ``INVERTED`` index build.
        """
        params: dict[str, Any] = {"with_position": self.fts_with_position}
        values: dict[str, object | None] = {
            "base_tokenizer": self.fts_base_tokenizer,
            "language": self.fts_language,
            "lower_case": self.fts_lower_case,
            "stem": self.fts_stem,
            "remove_stop_words": self.fts_remove_stop_words,
            "ascii_folding": self.fts_ascii_folding,
        }
        for name in FTS_OPTIONAL_PARAMS:
            if values[name] is not None:
                params[name] = values[name]
        return params


def scalar_index_name(column: str) -> str:
    """Return the btree index name for a scalar column.

    Args:
        column: The scalar column name.

    Returns:
        The derived index name.
    """
    return f"{column}_idx"


def bitmap_index_name(column: str) -> str:
    """Return the bitmap index name for a column.

    Args:
        column: The column name.

    Returns:
        The derived index name.
    """
    return f"{column}_bitmap_idx"


def fts_index_name(column: str) -> str:
    """Return the full-text index name for a text column.

    Args:
        column: The text column name.

    Returns:
        The derived index name.
    """
    return f"{column}_fts_idx"


def derive_num_partitions(rows: int, configured: int | None) -> int:
    """Return the IVF partition count for a dataset size.

    Follows the size-aware policy ``clamp(round(sqrt(rows)), 16, 4096)`` unless an explicit partition count was
    configured.

    Args:
        rows: The dataset row count.
        configured: An explicit partition count, taking precedence when set.

    Returns:
        The planned IVF partition count.
    """
    if configured is not None:
        return configured
    return min(MAX_IVF_PARTITIONS, max(MIN_IVF_PARTITIONS, round(math.sqrt(rows))))


def degrade_num_partitions(planned: int, rows: int, sample_rate: int) -> int:
    """Lower the partition count when training rows are insufficient.

    ``train_ivf`` samples ``num_partitions * sample_rate`` rows; when the dataset cannot supply that many, the partition
    count is degraded to what the available rows can train.

    Args:
        planned: The planned IVF partition count.
        rows: The dataset row count.
        sample_rate: Rows sampled per partition during IVF training.

    Returns:
        A partition count trainable from the available rows, at least 1.
    """
    supportable: int = rows // sample_rate
    return max(1, min(planned, supportable))


def artifact_directory(uri: str, column: str) -> str:
    """Return the per-dataset artifact sidecar location for a column.

    Args:
        uri: Dataset URI.
        column: Vector column the artifacts belong to.

    Returns:
        A sidecar directory beside the dataset, scoped to the column.
    """
    return f"{uri.rstrip('/')}.artifacts/{column}"


def centroids_to_ipc(centroids: pa.Array) -> bytes:
    """Serialize IVF centroids to an Arrow IPC stream.

    Args:
        centroids: The fixed-size-list centroid array.

    Returns:
        The IPC stream bytes.
    """
    table: pa.Table = pa.table({"centroids": centroids})
    sink: pa.BufferOutputStream = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, table.schema) as writer:
        writer.write_table(table)
    return sink.getvalue().to_pybytes()


def centroids_from_ipc(data: bytes) -> pa.Array:
    """Deserialize IVF centroids from an Arrow IPC stream.

    Args:
        data: The IPC stream bytes.

    Returns:
        The fixed-size-list centroid array.
    """
    reader = pa.ipc.open_stream(pa.BufferReader(data))
    return reader.read_all().column("centroids").combine_chunks()


def split_evenly(values: list[int], shards: int) -> list[list[int]]:
    """Split ids into balanced shards by round-robin assignment.

    Args:
        values: The fragment ids to split.
        shards: The desired number of shards.

    Returns:
        A list of non-empty shards.
    """
    count: int = max(1, min(shards, len(values)))
    groups: list[list[int]] = [values[index::count] for index in range(count)]
    return [group for group in groups if group]


def serialize_segment(segment: Index) -> str:
    """Serialize uncommitted segment metadata to a JSON document.

    Args:
        segment: The segment metadata returned by an uncommitted build.

    Returns:
        A JSON string carrying everything the commit needs.

    Raises:
        ValueError: If the segment is missing the index details required to commit it.
    """
    if segment.index_details is None:
        raise ValueError(f"segment {segment.uuid} is missing index details")
    type_url, detail_bytes = segment.index_details
    payload: dict[str, Any] = {
        "uuid": segment.uuid,
        "name": segment.name,
        "fields": list(segment.fields),
        "dataset_version": segment.dataset_version,
        "fragment_ids": sorted(segment.fragment_ids),
        "index_version": segment.index_version,
        "index_details_type_url": type_url,
        "index_details_b64": base64.b64encode(detail_bytes).decode("ascii"),
    }
    return json.dumps(payload)


def deserialize_segment(document: str) -> Index:
    """Reconstruct segment metadata from its JSON document.

    Args:
        document: The JSON string produced by :func:`serialize_segment`.

    Returns:
        An index-metadata object the commit accepts.
    """
    payload: dict[str, Any] = json.loads(document)
    details: tuple[str, bytes] = (
        payload["index_details_type_url"],
        base64.b64decode(payload["index_details_b64"]),
    )
    return Index(
        uuid=payload["uuid"],
        name=payload["name"],
        fields=payload["fields"],
        dataset_version=payload["dataset_version"],
        fragment_ids=set(payload["fragment_ids"]),
        index_version=payload["index_version"],
        index_details=details,
    )


def commit_segments(
    uri: str,
    segment_documents: list[str],
    column: str,
    index_name: str,
    merge: bool,
    config: IndexJobConfig,
    telemetry: Telemetry,
) -> None:
    """Commit built segments, retrying conflicts to coexist with writers.

    Args:
        uri: Dataset URI.
        segment_documents: Serialized segments returned by the executors.
        column: The indexed column.
        index_name: The index name to publish under.
        merge: Whether to merge segments before committing, used for IVF_RQ and BITMAP.
        config: Indexing configuration.
        telemetry: Driver telemetry facade.

    Raises:
        OSError | RuntimeError: If commits keep conflicting past the retry budget.
    """
    segments: list[Index] = [deserialize_segment(document) for document in segment_documents]
    tags: list[str] = [f"index:{index_name}"]

    def action() -> None:
        """Merge if needed and commit the segments at the latest version."""
        dataset: lance.LanceDataset = lance.dataset(uri, storage_options=config.storage_options)
        if merge and len(segments) > 1:
            merged = dataset.merge_existing_index_segments(segments)
            dataset.commit_existing_index_segments(index_name, column, [merged])
        else:
            dataset.commit_existing_index_segments(index_name, column, segments)
        telemetry.incr("index.committed", tags=tags)

    commit_with_retries(
        action,
        config.commit_retries,
        config.commit_backoff_seconds,
        lambda: telemetry.incr("index.commit_conflict", tags=tags),
    )


def lance_field_id(dataset: lance.LanceDataset, column: str) -> int:
    """Return the Lance field id for a top-level column.

    Uses the internal Lance schema rather than the Arrow positional index so the field id remains stable across schema
    evolution. ``dataset._ds`` is the only way to access the Lance schema from Python; access is wrapped here to contain
    the private-attribute usage.

    Args:
        dataset: The dataset to inspect.
        column: The column name to look up.

    Returns:
        The Lance field id for the column.

    Raises:
        ValueError: If the column is not present in the Lance schema.
    """
    field = dataset._ds.lance_schema.field_case_insensitive(column)
    if field is None:
        raise ValueError(f"column {column!r} not found in Lance schema")
    return field.id()


class IndexHandler:
    """Base handler that builds one index over a dataset's fragments.

    The default :meth:`build` implements the segment-API flow shared by the vector handler: split target fragments into
    shards, build one uncommitted segment per shard across executors, and commit the collected segments. Subclasses
    override the build steps or the whole flow.
    """

    def __init__(self, config: IndexJobConfig, column: str, index_name: str) -> None:
        """Initialize the handler.

        Args:
            config: Indexing configuration.
            column: The column to index.
            index_name: The index name to publish under.
        """
        self.config: IndexJobConfig = config
        self.column: str = column
        self.index_name: str = index_name

    def index_type(self) -> str:
        """Return the Lance index type string.

        Returns:
            The index type, such as ``BTREE``.
        """
        raise NotImplementedError

    def merges(self) -> bool:
        """Report whether segments are merged before commit.

        Returns:
            ``True`` to merge segments into one before committing.
        """
        return False

    def validate(self, dataset: lance.LanceDataset) -> None:
        """Validate that the dataset supports this index.

        Subclasses override this to raise ``ValueError`` when the dataset does not satisfy the index's prerequisites.
        The base implementation accepts any dataset.

        Args:
            dataset: The dataset to validate against.
        """
        del dataset

    def skip_reason(self, dataset: lance.LanceDataset) -> str | None:
        """Return why this index should be skipped for the dataset, if at all.

        Subclasses override this to opt out of indexing, for example when the dataset is too small to benefit. The base
        implementation never skips.

        Args:
            dataset: The dataset to inspect.

        Returns:
            A human-readable reason to skip, or ``None`` to proceed.
        """
        del dataset
        return None

    def extra_stats(self) -> dict[str, Any]:
        """Return handler-specific fields to merge into the result.

        Returns:
            Additional statistics, empty by default.
        """
        return {}

    def covered_fragments(self, dataset: lance.LanceDataset) -> set[int]:
        """Return fragments already covered by this index.

        Args:
            dataset: The dataset to inspect.

        Returns:
            The set of covered fragment ids.
        """
        covered: set[int] = set()
        for description in dataset.describe_indices():
            if description.name == self.index_name and self.column in description.field_names:
                for segment in description.segments:
                    covered.update(segment.fragment_ids)
        return covered

    def target_fragments(self, dataset: lance.LanceDataset) -> list[int]:
        """Return fragments to index.

        Args:
            dataset: The dataset to inspect.

        Returns:
            Every fragment when rebuilding, otherwise only uncovered fragments.
        """
        all_ids: list[int] = [fragment.fragment_id for fragment in dataset.get_fragments()]
        if self.config.rebuild:
            return all_ids
        covered: set[int] = self.covered_fragments(dataset)
        return [fragment_id for fragment_id in all_ids if fragment_id not in covered]

    def prepare(self, dataset: lance.LanceDataset, uri: str, telemetry: Telemetry) -> object | None:
        """Build artifacts to broadcast to the segment builders.

        Subclasses override this to train or load artifacts that are broadcast to each executor shard. The base
        implementation requires no artifacts.

        Args:
            dataset: The dataset being indexed.
            uri: Dataset URI.
            telemetry: Driver telemetry facade.

        Returns:
            A broadcastable artifact, or ``None`` when none is needed.
        """
        del dataset, uri, telemetry
        return None

    def build_segment(self, dataset: lance.LanceDataset, fragment_ids: list[int], artifacts: object | None) -> Index:
        """Build one uncommitted segment over a shard of fragments.

        Args:
            dataset: A dataset handle pinned to the build version.
            fragment_ids: The fragment ids for this shard.
            artifacts: The broadcast artifact, or ``None``.

        Returns:
            The uncommitted segment metadata.
        """
        raise NotImplementedError

    def build(self, spark: SparkSession, uri: str, telemetry: Telemetry) -> dict[str, Any]:
        """Build and commit this index across executors.

        Args:
            spark: Active Spark session.
            uri: Dataset URI.
            telemetry: Driver telemetry facade.

        Returns:
            A statistics dictionary for the index.
        """
        config: IndexJobConfig = self.config
        dataset: lance.LanceDataset = lance.dataset(uri, storage_options=config.storage_options)
        reason: str | None = self.skip_reason(dataset)
        if reason is not None:
            telemetry.incr("index.skipped", tags=[f"index:{self.index_name}"])
            return {"column": self.column, "index": self.index_name, "segments": 0, "fragments": 0, "skipped": reason}
        self.validate(dataset)
        targets: list[int] = self.target_fragments(dataset)
        if not targets:
            return {"column": self.column, "index": self.index_name, "segments": 0, "fragments": 0}

        artifacts: object | None = self.prepare(dataset, uri, telemetry)
        version: int = dataset.version
        groups: list[list[int]] = split_evenly(targets, config.num_shards)
        spark_context = spark.sparkContext
        broadcast_artifacts = spark_context.broadcast(artifacts) if artifacts is not None else None
        build_segment: Callable[[lance.LanceDataset, list[int], object | None], Index] = self.build_segment
        storage_options: dict[str, Any] | None = config.storage_options
        index_type: str = self.index_type()

        def build_partition(group_iterator: Iterator[list[int]]) -> Iterator[str]:
            """Build one segment per shard on an executor.

            Args:
                group_iterator: Fragment-id shards assigned to this task.

            Yields:
                The serialized segment for each shard.
            """
            telemetry_local: Telemetry = Telemetry.create(config.telemetry)
            local_artifacts: object | None = broadcast_artifacts.value if broadcast_artifacts is not None else None
            tags: list[str] = [f"index_type:{index_type}"]
            with telemetry_local.span("lance.indexing.build_segment"):
                for group in group_iterator:
                    shard_dataset: lance.LanceDataset = lance.dataset(
                        uri, version=version, storage_options=storage_options
                    )
                    with telemetry_local.timed("segment.build_ms", tags=tags):
                        segment = build_segment(shard_dataset, list(group), local_artifacts)
                    telemetry_local.incr("segment.built", tags=tags)
                    yield serialize_segment(segment)

        with telemetry.timed("index.build_ms", tags=[f"index:{self.index_name}"]):
            segment_documents: list[str] = (
                spark_context.parallelize(groups, len(groups)).mapPartitions(build_partition).collect()
            )
        with telemetry.timed("index.commit_ms", tags=[f"index:{self.index_name}"]):
            commit_segments(uri, segment_documents, self.column, self.index_name, self.merges(), config, telemetry)
        result: dict[str, Any] = {
            "column": self.column,
            "index": self.index_name,
            "segments": len(segment_documents),
            "fragments": len(targets),
        }
        result.update(self.extra_stats())
        return result


class VectorIndexHandler(IndexHandler):
    """Builds an IVF_RQ vector index, training or reusing per-dataset artifacts."""

    def __init__(self, config: IndexJobConfig, column: str, index_name: str) -> None:
        """Initialize the handler.

        Args:
            config: Indexing configuration.
            column: The vector column to index.
            index_name: The index name to publish under.
        """
        super().__init__(config, column, index_name)
        self.reused_artifacts: bool = False
        self.num_partitions_used: int | None = None

    def index_type(self) -> str:
        """Return the vector index type.

        Returns:
            The string ``IVF_RQ``.
        """
        return "IVF_RQ"

    def merges(self) -> bool:
        """Report that IVF_RQ segments are merged before commit.

        Returns:
            Always ``True``.
        """
        return True

    def extra_stats(self) -> dict[str, Any]:
        """Return artifact reuse and the partition count actually used.

        When ``reused_artifacts`` is true, the sidecar supplied the centroids, ``num_partitions``, and the
        ``rabitq_model`` rotation string.

        Returns:
            A mapping with the artifact reuse flag and IVF partition count.
        """
        return {"reused_artifacts": self.reused_artifacts, "num_partitions": self.num_partitions_used}

    def skip_reason(self, dataset: lance.LanceDataset) -> str | None:
        """Skip the vector index when the dataset is below the row floor.

        Args:
            dataset: The dataset to inspect.

        Returns:
            The skip reason for small datasets, or ``None`` to proceed.
        """
        rows: int = dataset.count_rows()
        if rows < self.config.vector_min_rows:
            return f"{rows} rows below vector_min_rows={self.config.vector_min_rows}; flat KNN suffices"
        return None

    def dimension(self, dataset: lance.LanceDataset) -> int:
        """Return the vector dimension of the indexed column.

        Args:
            dataset: The dataset to inspect.

        Returns:
            The fixed vector dimension.
        """
        return IndicesBuilder(dataset, self.column).dimension

    def validate(self, dataset: lance.LanceDataset) -> None:
        """Validate the IVF_RQ parameters against the column.

        Args:
            dataset: The dataset to validate against.

        Raises:
            ValueError: If parameters or the dimension are unsupported.
        """
        if self.config.num_bits != 1:
            raise ValueError("IVF_RQ supports num_bits=1; higher widths are gated")
        if self.dimension(dataset) % 8 != 0:
            raise ValueError("IVF_RQ requires the vector dimension to be divisible by 8")

    def validate_manifest(self, manifest: dict[str, Any], dimension: int) -> None:
        """Check that reused artifacts match the requested configuration.

        The partition count is not compared: reused centroids define it, so the build adopts ``num_partitions`` from the
        manifest instead. The manifest also carries the ``rabitq_model`` rotation string; its presence is checked by the
        reuse branch in :meth:`prepare`, which retrains when a sidecar predates the model.

        Args:
            manifest: The stored artifact manifest.
            dimension: The vector dimension of the column.

        Raises:
            ValueError: If any pinned parameter differs from the request.
        """
        config: IndexJobConfig = self.config
        expected: dict[str, Any] = {
            "dimension": dimension,
            "metric": config.metric,
            "num_bits": config.num_bits,
        }
        for name, value in expected.items():
            if manifest.get(name) != value:
                raise ValueError(f"reused artifact {name} {manifest.get(name)!r} does not match {value!r}")

    def prepare(self, dataset: lance.LanceDataset, uri: str, telemetry: Telemetry) -> object | None:
        """Load this dataset's IVF_RQ artifacts, or train and persist them.

        Trains the IVF centroid model and mints one shared RaBitQ rotation via ``lance.lance.indices.build_rq_model``;
        both are persisted to the sidecar manifest and broadcast. The SAME ``rabitq_model`` JSON string must reach every
        executor because it pins the rotation, so per-fragment segments produce comparable binary codes and remain
        mergeable. If it were omitted, each ``create_index_uncommitted`` call would generate its own random rotation,
        which is only safe for a single non-merged segment. The partition count follows the size-aware policy and is
        degraded when the dataset cannot supply ``num_partitions * sample_rate`` training rows.

        Args:
            dataset: The dataset to train on if artifacts are absent.
            uri: Dataset URI used to locate the artifact sidecar.
            telemetry: Driver telemetry facade.

        Returns:
            The centroids IPC bytes, the RaBitQ model JSON string, num_bits,
            and the IVF partition count.
        """
        config: IndexJobConfig = self.config
        dimension: int = self.dimension(dataset)
        filesystem, base_path = resolve_filesystem(artifact_directory(uri, self.column), config.storage_options)
        manifest_path: str = f"{base_path.rstrip('/')}/manifest.json"
        centroids_path: str = f"{base_path.rstrip('/')}/ivf_centroids.arrow"

        if config.reuse_artifacts and not config.rebuild and object_exists(filesystem, manifest_path):
            manifest: dict[str, Any] = json.loads(read_object(filesystem, manifest_path))
            if "rabitq_model" in manifest:
                self.validate_manifest(manifest, dimension)
                self.reused_artifacts = True
                self.num_partitions_used = int(manifest["num_partitions"])
                telemetry.incr("artifacts.reused")
                return (
                    read_object(filesystem, centroids_path),
                    manifest["rabitq_model"],
                    config.num_bits,
                    self.num_partitions_used,
                )
            logger.info("sidecar manifest for %s has no rabitq_model; retraining artifacts", uri)

        rows: int = dataset.count_rows()
        planned: int = derive_num_partitions(rows, config.num_partitions)
        partitions: int = degrade_num_partitions(planned, rows, config.train_sample_rate)
        if partitions < planned:
            telemetry.incr("artifacts.partitions_degraded")
            logger.info("degraded num_partitions %d -> %d for %s (%d rows)", planned, partitions, uri, rows)
        with telemetry.timed("artifacts.train_ms"):
            ivf_model = IndicesBuilder(dataset, self.column).train_ivf(
                num_partitions=partitions,
                distance_type=config.resolved_distance_type(),
                sample_rate=config.train_sample_rate,
                max_iters=config.train_max_iters,
            )
            centroids_bytes: bytes = centroids_to_ipc(ivf_model.centroids)
            rabitq_model: str = native_indices.build_rq_model(dimension=dimension, num_bits=config.num_bits)
        manifest = {
            "dimension": dimension,
            "metric": config.metric,
            "num_partitions": partitions,
            "num_bits": config.num_bits,
            "distance_type": config.resolved_distance_type(),
            "rabitq_model": rabitq_model,
            "created_at": datetime.now(UTC).isoformat(),
        }
        write_object(filesystem, centroids_path, centroids_bytes)
        write_object(filesystem, manifest_path, json.dumps(manifest).encode("utf-8"))
        self.reused_artifacts = False
        self.num_partitions_used = partitions
        telemetry.incr("artifacts.trained")
        return centroids_bytes, rabitq_model, config.num_bits, partitions

    def build_segment(self, dataset: lance.LanceDataset, fragment_ids: list[int], artifacts: object | None) -> Index:
        """Build one IVF_RQ segment over a shard of fragments.

        Args:
            dataset: A dataset handle pinned to the build version.
            fragment_ids: The fragment ids for this shard.
            artifacts: The centroids bytes, the shared RaBitQ model string, num_bits, and the IVF partition count.

        Returns:
            The uncommitted segment metadata.

        Raises:
            ValueError: If ``artifacts`` is ``None``; vector segment builds require the artifact tuple produced by
                :meth:`prepare`.
        """
        if artifacts is None:
            raise ValueError("VectorIndexHandler.build_segment requires artifacts from prepare; got None")
        centroids_bytes, rabitq_model, num_bits, num_partitions = artifacts
        centroids: pa.Array = centroids_from_ipc(centroids_bytes)
        return dataset.create_index_uncommitted(
            column=self.column,
            index_type="IVF_RQ",
            name=self.index_name,
            metric=self.config.metric,
            num_partitions=num_partitions,
            num_bits=num_bits,
            ivf_centroids=centroids,
            rabitq_model=rabitq_model,
            fragment_ids=fragment_ids,
        )


class BTreeIndexHandler(IndexHandler):
    """Builds a btree scalar index through the segment API.

    Each shard calls ``create_index_uncommitted`` and the driver publishes the collected segments with
    ``commit_existing_index_segments``. BTREE segments do not support driver-side merging, so they are committed
    unmerged. Incremental fragment coverage is inherited from :class:`IndexHandler`.
    """

    def index_type(self) -> str:
        """Return the btree index type.

        Returns:
            The string ``BTREE``.
        """
        return "BTREE"

    def build_segment(self, dataset: lance.LanceDataset, fragment_ids: list[int], artifacts: object | None) -> Index:
        """Build one BTREE segment over a shard of fragments.

        ``index_uuid`` must not be passed for BTREE segment builds; Lance mints segment ids itself.

        Args:
            dataset: A dataset handle pinned to the build version.
            fragment_ids: The fragment ids for this shard.
            artifacts: Unused; btree builds need no broadcast artifact.

        Returns:
            The uncommitted segment metadata.
        """
        del artifacts
        return dataset.create_index_uncommitted(
            column=self.column,
            index_type="BTREE",
            name=self.index_name,
            fragment_ids=fragment_ids,
        )


class BitmapIndexHandler(IndexHandler):
    """Builds a bitmap scalar index through the segment API.

    Each shard calls ``create_index_uncommitted`` and the driver merges the collected segments into one with
    ``merge_existing_index_segments`` before publishing via ``commit_existing_index_segments``. Incremental fragment
    coverage is inherited from :class:`IndexHandler`.
    """

    def index_type(self) -> str:
        """Return the bitmap index type.

        Returns:
            The string ``BITMAP``.
        """
        return "BITMAP"

    def merges(self) -> bool:
        """Report that bitmap segments are merged before commit.

        Returns:
            Always ``True``.
        """
        return True

    def build_segment(self, dataset: lance.LanceDataset, fragment_ids: list[int], artifacts: object | None) -> Index:
        """Build one BITMAP segment over a shard of fragments.

        Args:
            dataset: A dataset handle pinned to the build version.
            fragment_ids: The fragment ids for this shard.
            artifacts: Unused; bitmap builds need no broadcast artifact.

        Returns:
            The uncommitted segment metadata.
        """
        del artifacts
        return dataset.create_index_uncommitted(
            column=self.column,
            index_type="BITMAP",
            name=self.index_name,
            fragment_ids=fragment_ids,
        )


class FtsIndexHandler(IndexHandler):
    """Builds a full-text BM25 inverted index via the metadata-merge path.

    Inverted indices are not built through the segment API. Each shard builds its fragments under one shared index id,
    the driver merges the per-fragment metadata, and the index is published with a create-index commit. The whole index
    is rebuilt each run, so any existing index of the same name is dropped first.
    """

    def index_type(self) -> str:
        """Return the inverted index type.

        Returns:
            The string ``INVERTED``.
        """
        return "INVERTED"

    def commit_index(
        self,
        uri: str,
        dataset: lance.LanceDataset,
        index_uuid: str,
        fragment_ids: list[int],
        telemetry: Telemetry,
    ) -> None:
        """Publish the merged inverted index, retrying conflicts.

        Args:
            uri: Dataset URI.
            dataset: The dataset handle at the build version.
            index_uuid: The shared index id the shards built under.
            fragment_ids: The fragments the index covers.
            telemetry: Driver telemetry facade.

        Raises:
            OSError | RuntimeError: If commits keep conflicting past the retry budget.
        """
        config: IndexJobConfig = self.config
        field_id: int = lance_field_id(dataset, self.column)
        index_name: str = self.index_name
        fragments: set[int] = set(fragment_ids)
        storage_options: dict[str, Any] | None = config.storage_options
        tags: list[str] = ["index_type:INVERTED"]

        def action() -> None:
            """Publish the merged inverted index at the latest version."""
            current: lance.LanceDataset = lance.dataset(uri, storage_options=storage_options)
            index: Index = Index(
                uuid=index_uuid,
                name=index_name,
                fields=[field_id],
                dataset_version=current.version,
                fragment_ids=fragments,
                index_version=0,
            )
            operation = lance.LanceOperation.CreateIndex(new_indices=[index], removed_indices=[])
            lance.LanceDataset.commit(uri, operation, read_version=current.version, storage_options=storage_options)
            telemetry.incr("index.committed", tags=tags)

        commit_with_retries(
            action,
            config.commit_retries,
            config.commit_backoff_seconds,
            lambda: telemetry.incr("index.commit_conflict", tags=tags),
        )

    def build(self, spark: SparkSession, uri: str, telemetry: Telemetry) -> dict[str, Any]:
        """Build and commit the inverted index across executors.

        Args:
            spark: Active Spark session.
            uri: Dataset URI.
            telemetry: Driver telemetry facade.

        Returns:
            A statistics dictionary for the index.
        """
        config: IndexJobConfig = self.config
        dataset: lance.LanceDataset = lance.dataset(uri, storage_options=config.storage_options)
        if self.covered_fragments(dataset):
            dataset.drop_index(self.index_name)
            dataset = lance.dataset(uri, storage_options=config.storage_options)

        fragment_ids: list[int] = [fragment.fragment_id for fragment in dataset.get_fragments()]
        if not fragment_ids:
            return {"column": self.column, "index": self.index_name, "segments": 0, "fragments": 0}

        version: int = dataset.version
        index_uuid: str = str(uuid.uuid4())
        params: dict[str, Any] = config.fts_params()
        groups: list[list[int]] = split_evenly(fragment_ids, config.num_shards)
        column: str = self.column
        index_name: str = self.index_name
        storage_options: dict[str, Any] | None = config.storage_options

        def build_partition(group_iterator: Iterator[list[int]]) -> Iterator[int]:
            """Build per-fragment inverted indices under the shared id.

            Args:
                group_iterator: Fragment-id shards assigned to this task.

            Yields:
                The count of fragments this task built.
            """
            telemetry_local: Telemetry = Telemetry.create(config.telemetry)
            built: int = 0
            with telemetry_local.span("lance.indexing.build_fts_segment"):
                for group in group_iterator:
                    shard_dataset: lance.LanceDataset = lance.dataset(
                        uri, version=version, storage_options=storage_options
                    )
                    for fragment_id in group:
                        with telemetry_local.timed("segment.build_ms", tags=["index_type:INVERTED"]):
                            shard_dataset.create_scalar_index(
                                column=column,
                                index_type="INVERTED",
                                name=index_name,
                                replace=False,
                                index_uuid=index_uuid,
                                fragment_ids=[fragment_id],
                                **params,
                            )
                        built += 1
                        telemetry_local.incr("segment.built", tags=["index_type:INVERTED"])
            yield built

        with telemetry.timed("index.build_ms", tags=[f"index:{index_name}"]):
            counts: list[int] = (
                spark.sparkContext.parallelize(groups, len(groups)).mapPartitions(build_partition).collect()
            )
        with telemetry.timed("index.merge_ms", tags=[f"index:{index_name}"]):
            dataset.merge_index_metadata(index_uuid, index_type="INVERTED")
        with telemetry.timed("index.commit_ms", tags=[f"index:{index_name}"]):
            self.commit_index(uri, dataset, index_uuid, fragment_ids, telemetry)
        return {
            "column": self.column,
            "index": index_name,
            "segments": sum(counts),
            "fragments": len(fragment_ids),
        }


def index_dataset_locally(uri: str, config: IndexJobConfig) -> dict[str, Any]:
    """Build every configured index for one small dataset on one executor.

    This is the small-dataset tier: no segment fan-out, just plain ``create_index`` / ``create_scalar_index`` calls
    that build and commit each index end-to-end. Single-process ``create_index`` needs no shared RaBitQ model — a lone
    non-merged segment may use its own random rotation. The vector index follows the same size-aware policy as the
    distributed path and is skipped below the configured row floor.

    Args:
        uri: Dataset URI.
        config: Indexing configuration.

    Returns:
        A statistics dictionary matching the large-tier shape.
    """
    telemetry: Telemetry = Telemetry.create(config.telemetry)
    dataset: lance.LanceDataset = lance.dataset(uri, storage_options=config.storage_options)
    fragments: int = len(dataset.get_fragments())
    indexes: list[dict[str, Any]] = []
    with telemetry.span("lance.indexing.local_dataset"):
        if config.vector_column is not None:
            index_name: str = config.resolved_vector_index_name()
            rows: int = dataset.count_rows()
            if rows < config.vector_min_rows:
                telemetry.incr("index.skipped", tags=[f"index:{index_name}"])
                reason: str = f"{rows} rows below vector_min_rows={config.vector_min_rows}; flat KNN suffices"
                indexes.append(
                    {
                        "column": config.vector_column,
                        "index": index_name,
                        "segments": 0,
                        "fragments": 0,
                        "skipped": reason,
                    }
                )
            else:
                planned: int = derive_num_partitions(rows, config.num_partitions)
                partitions: int = degrade_num_partitions(planned, rows, config.train_sample_rate)
                with telemetry.timed("index.build_ms", tags=[f"index:{index_name}"]):
                    dataset.create_index(
                        config.vector_column,
                        "IVF_RQ",
                        name=index_name,
                        metric=config.metric,
                        replace=True,
                        num_partitions=partitions,
                        num_bits=config.num_bits,
                    )
                telemetry.incr("index.committed", tags=[f"index:{index_name}"])
                indexes.append(
                    {
                        "column": config.vector_column,
                        "index": index_name,
                        "segments": 1,
                        "fragments": fragments,
                        "num_partitions": partitions,
                    }
                )
        scalar_targets: list[tuple[str, str, str, dict[str, Any]]] = [
            *((column, "BTREE", scalar_index_name(column), {}) for column in config.scalar_columns),
            *((column, "BITMAP", bitmap_index_name(column), {}) for column in config.bitmap_columns),
            *((column, "INVERTED", fts_index_name(column), config.fts_params()) for column in config.text_columns),
        ]
        for column, index_type, name, params in scalar_targets:
            with telemetry.timed("index.build_ms", tags=[f"index:{name}"]):
                dataset.create_scalar_index(column, index_type, name=name, replace=True, **params)
            telemetry.incr("index.committed", tags=[f"index:{name}"])
            indexes.append({"column": column, "index": name, "segments": 1, "fragments": fragments})
    return {"uri": uri, "indexes": indexes, "tier": "small"}


class LanceIndexer:
    """Builds the configured indices on Lance datasets via per-type handlers."""

    def __init__(self, config: IndexJobConfig) -> None:
        """Initialize the indexer.

        Args:
            config: Indexing configuration.
        """
        self.config: IndexJobConfig = config

    def handlers(self) -> list[IndexHandler]:
        """Build the index handlers selected by the configuration.

        Returns:
            One handler per configured index.
        """
        config: IndexJobConfig = self.config
        result: list[IndexHandler] = []
        if config.vector_column is not None:
            result.append(VectorIndexHandler(config, config.vector_column, config.resolved_vector_index_name()))
        for column in config.scalar_columns:
            result.append(BTreeIndexHandler(config, column, scalar_index_name(column)))
        for column in config.bitmap_columns:
            result.append(BitmapIndexHandler(config, column, bitmap_index_name(column)))
        for column in config.text_columns:
            result.append(FtsIndexHandler(config, column, fts_index_name(column)))
        return result

    def build(self, spark: SparkSession, uri: str, telemetry: Telemetry) -> dict[str, Any]:
        """Build every configured index for one dataset.

        Args:
            spark: Active Spark session.
            uri: Dataset URI.
            telemetry: Driver telemetry facade.

        Returns:
            A statistics dictionary for the dataset's indices.
        """
        indexes: list[dict[str, Any]] = []
        for handler in self.handlers():
            with telemetry.span("lance.indexing.index") as index_span:
                index_span.set_tag("index", handler.index_name)
                index_span.set_tag("index_type", handler.index_type())
                indexes.append(handler.build(spark, uri, telemetry))
        return {"uri": uri, "indexes": indexes}

    def classify(self, spark: SparkSession, dataset_uris: list[str]) -> tuple[list[str], list[str]]:
        """Split datasets into the small and large tiers by fragment count.

        Fragment counts are gathered with one distributed job so the driver never opens datasets itself.

        Args:
            spark: Active Spark session.
            dataset_uris: Datasets to classify.

        Returns:
            The small-tier URIs and the large-tier URIs.
        """
        config: IndexJobConfig = self.config
        storage_options: dict[str, Any] | None = config.storage_options
        threshold: int = config.small_dataset_fragment_threshold

        def fragment_count(uri: str) -> tuple[str, int]:
            """Count one dataset's fragments on an executor.

            Args:
                uri: Dataset URI.

            Returns:
                The URI paired with its fragment count.
            """
            return uri, len(lance.dataset(uri, storage_options=storage_options).get_fragments())

        slices: int = max(1, min(config.small_tier_slices, len(dataset_uris)))
        counts: list[tuple[str, int]] = (
            spark.sparkContext.parallelize(dataset_uris, slices).map(fragment_count).collect()
        )
        small: list[str] = [uri for uri, count in counts if count < threshold]
        large: list[str] = [uri for uri, count in counts if count >= threshold]
        return small, large

    def run_small_tier(self, spark: SparkSession, uris: list[str], telemetry: Telemetry) -> list[dict[str, Any]]:
        """Index many small datasets in one batched Spark job.

        Each executor task indexes one whole dataset end-to-end with plain non-distributed index builds; the driver only
        collects statistics.

        Args:
            spark: Active Spark session.
            uris: Small-tier dataset URIs.
            telemetry: Driver telemetry facade.

        Returns:
            One statistics dictionary per dataset.
        """
        config: IndexJobConfig = self.config

        def index_one(uri: str) -> dict[str, Any]:
            """Index one whole dataset on an executor.

            Args:
                uri: Dataset URI.

            Returns:
                The dataset's statistics dictionary.
            """
            return index_dataset_locally(uri, config)

        slices: int = max(1, min(config.small_tier_slices, len(uris)))
        with telemetry.timed("tier.small_ms"):
            results: list[dict[str, Any]] = spark.sparkContext.parallelize(uris, slices).map(index_one).collect()
        telemetry.gauge("tier.small_datasets", len(results))
        return results

    def run_large_tier(self, spark: SparkSession, uris: list[str], telemetry: Telemetry) -> list[dict[str, Any]]:
        """Index large datasets concurrently with the segment fan-out.

        Each dataset keeps its distributed per-segment build, but multiple datasets are driven concurrently from a
        driver thread pool. Every submission is tagged with the configured Spark FAIR scheduler pool so concurrent jobs
        share the cluster fairly; ``spark.scheduler.mode=FAIR`` must be set on the session for the pools to take effect.

        Args:
            spark: Active Spark session.
            uris: Large-tier dataset URIs.
            telemetry: Driver telemetry facade.

        Returns:
            One statistics dictionary per dataset, in input order.
        """
        config: IndexJobConfig = self.config

        def index_one(uri: str) -> dict[str, Any]:
            """Drive one dataset's distributed build from a worker thread.

            Args:
                uri: Dataset URI.

            Returns:
                The dataset's statistics dictionary.
            """
            spark.sparkContext.setLocalProperty("spark.scheduler.pool", config.scheduler_pool)
            try:
                with telemetry.timed("dataset.total_ms", tags=[f"uri:{uri}"]):
                    stats: dict[str, Any] = self.build(spark, uri, telemetry)
            except Exception:
                telemetry.error(f"indexing failed for {uri}", tags=[f"uri:{uri}"])
                raise
            finally:
                spark.sparkContext.setLocalProperty("spark.scheduler.pool", None)
            segment_total: int = sum(int(item["segments"]) for item in stats["indexes"])
            telemetry.gauge("dataset.segments", segment_total, tags=[f"uri:{uri}"])
            logger.info("indexed %s: %d indices, %d segments", uri, len(stats["indexes"]), segment_total)
            stats["tier"] = "large"
            return stats

        workers: int = max(1, min(config.driver_concurrency, len(uris)))
        with telemetry.timed("tier.large_ms"), ThreadPoolExecutor(max_workers=workers) as pool:
            results: list[dict[str, Any]] = list(pool.map(index_one, uris))
        telemetry.gauge("tier.large_datasets", len(results))
        return results

    def run(self, spark: SparkSession, dataset_uris: list[str]) -> list[dict[str, Any]]:
        """Index every dataset with two-tier orchestration.

        Datasets are classified by fragment count: small datasets are batched into one Spark job where each executor
        task indexes a whole dataset, and large datasets keep the distributed segment fan-out, driven concurrently from
        the driver. Any failure propagates and fails the run.

        Args:
            spark: Active Spark session.
            dataset_uris: Datasets to index.

        Returns:
            One statistics dictionary per dataset, in input order.
        """
        driver_telemetry: Telemetry = Telemetry.create(self.config.telemetry)
        with driver_telemetry.span("lance.indexing.run") as run_span:
            run_span.set_tag("dataset_count", len(dataset_uris))
            if not dataset_uris:
                return []
            small, large = self.classify(spark, dataset_uris)
            run_span.set_tag("small_datasets", len(small))
            run_span.set_tag("large_datasets", len(large))
            stats_by_uri: dict[str, dict[str, Any]] = {}
            if small:
                for stats in self.run_small_tier(spark, small, driver_telemetry):
                    stats_by_uri[stats["uri"]] = stats
            if large:
                for stats in self.run_large_tier(spark, large, driver_telemetry):
                    stats_by_uri[stats["uri"]] = stats
            results: list[dict[str, Any]] = [stats_by_uri[uri] for uri in dataset_uris]
            driver_telemetry.gauge("run.datasets", len(results))
            logger.info("indexing run: %d datasets (%d small, %d large)", len(results), len(small), len(large))
            return results
