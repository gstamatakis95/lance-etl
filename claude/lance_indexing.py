"""Distributed vector, scalar, and full-text indexing for Lance datasets.

Each index type is built by its own handler so the orchestration stays uniform
while the per-type specifics live in one place:

- :class:`VectorIndexHandler` builds an IVF_RQ index. IVF centroids and the
  RaBitQ rotation are trained once per dataset, persisted to a sidecar, and
  reused so new fragments encode with the same centroids and rotation as the
  existing segments. Segments are merged before commit.
- :class:`BTreeIndexHandler` and :class:`BitmapIndexHandler` build scalar
  indices through the same segment API; their segments are committed disjointly,
  which lets a growing dataset be indexed by adding segments for new fragments.
- :class:`FtsIndexHandler` builds a full-text (BM25) inverted index. Inverted
  indices use the distributed metadata-merge path rather than the segment API:
  each shard builds its fragments under one shared index id, the driver merges
  the per-fragment metadata, and the index is published with a create-index
  commit. This path rebuilds the whole index, so it is not incremental.

Vector, btree, and bitmap handlers index only fragments not already covered by
default and commit so existing disjoint segments are preserved; pass ``rebuild``
to reindex every fragment. Every commit retries conflicts with exponential
backoff so it coexists with concurrent ingestion and compaction; any other
failure propagates so the job fails fast. Requires pylance and the Datadog Agent
on the executors; artifact IO uses pyarrow's filesystem layer.
"""

from __future__ import annotations

import base64
import json
import logging
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, Iterator, List, Optional, Set, Tuple

import lance
import pyarrow as pa
from lance.dataset import Index
from lance.indices import IndicesBuilder
from lance.lance import indices as native_indices
from pyspark.sql import SparkSession

from cloud_storage import object_exists, read_object, resolve_filesystem, write_object
from telemetry import Telemetry, TelemetryConfig, commit_with_retries

logger: logging.Logger = logging.getLogger(__name__)

METRIC_TO_DISTANCE: Dict[str, str] = {"l2": "l2", "cosine": "cosine", "dot": "dot"}
FTS_OPTIONAL_PARAMS: Tuple[str, ...] = (
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
        num_partitions: IVF partitions; required when a vector column is set.
        num_bits: RaBitQ bits per dimension; IVF_RQ uses 1.
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
    """

    telemetry: TelemetryConfig
    storage_options: Optional[Dict[str, Any]] = None
    vector_column: Optional[str] = None
    num_partitions: Optional[int] = None
    num_bits: int = 1
    metric: str = "L2"
    distance_type: Optional[str] = None
    train_sample_rate: int = 256
    train_max_iters: int = 50
    vector_index_name: Optional[str] = None
    scalar_columns: List[str] = field(default_factory=list)
    bitmap_columns: List[str] = field(default_factory=list)
    text_columns: List[str] = field(default_factory=list)
    fts_with_position: bool = False
    fts_base_tokenizer: Optional[str] = None
    fts_language: Optional[str] = None
    fts_lower_case: Optional[bool] = None
    fts_stem: Optional[bool] = None
    fts_remove_stop_words: Optional[bool] = None
    fts_ascii_folding: Optional[bool] = None
    num_shards: int = 64
    rebuild: bool = False
    reuse_artifacts: bool = True
    commit_retries: int = 20
    commit_backoff_seconds: float = 0.5

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

    def fts_params(self) -> Dict[str, Any]:
        """Build the inverted-index parameters, omitting unset options.

        Returns:
            Keyword arguments for an ``INVERTED`` index build.
        """
        params: Dict[str, Any] = {"with_position": self.fts_with_position}
        values: Dict[str, Optional[object]] = {
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


def split_evenly(values: List[int], shards: int) -> List[List[int]]:
    """Split ids into balanced shards by round-robin assignment.

    Args:
        values: The fragment ids to split.
        shards: The desired number of shards.

    Returns:
        A list of non-empty shards.
    """
    count: int = max(1, min(shards, len(values)))
    groups: List[List[int]] = [values[index::count] for index in range(count)]
    return [group for group in groups if group]


def serialize_segment(segment: Index) -> str:
    """Serialize uncommitted segment metadata to a JSON document.

    Args:
        segment: The segment metadata returned by an uncommitted build.

    Returns:
        A JSON string carrying everything the commit needs.

    Raises:
        ValueError: If the segment is missing the index details required to
            commit it.
    """
    if segment.index_details is None:
        raise ValueError(f"segment {segment.uuid} is missing index details")
    type_url, detail_bytes = segment.index_details
    payload: Dict[str, Any] = {
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
    payload: Dict[str, Any] = json.loads(document)
    details: Tuple[str, bytes] = (
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
    segment_documents: List[str],
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
        merge: Whether to merge segments before committing, used for IVF_RQ.
        config: Indexing configuration.
        telemetry: Driver telemetry facade.

    Raises:
        RuntimeError: If commits keep conflicting past the retry budget.
    """
    segments: List[Index] = [deserialize_segment(document) for document in segment_documents]
    tags: List[str] = [f"index:{index_name}"]

    def action() -> None:
        """Merge if needed and commit the segments at the latest version."""
        dataset: "lance.LanceDataset" = lance.dataset(uri, storage_options=config.storage_options)
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


class IndexHandler:
    """Base handler that builds one index over a dataset's fragments.

    The default :meth:`build` implements the segment-API flow shared by the
    vector and scalar handlers: split target fragments into shards, build one
    uncommitted segment per shard across executors, and commit the collected
    segments. Subclasses override the build steps or the whole flow.
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

    def validate(self, dataset: "lance.LanceDataset") -> None:
        """Validate that the dataset supports this index.

        Args:
            dataset: The dataset to validate against.
        """
        return None

    def extra_stats(self) -> Dict[str, Any]:
        """Return handler-specific fields to merge into the result.

        Returns:
            Additional statistics, empty by default.
        """
        return {}

    def covered_fragments(self, dataset: "lance.LanceDataset") -> Set[int]:
        """Return fragments already covered by this index.

        Args:
            dataset: The dataset to inspect.

        Returns:
            The set of covered fragment ids.
        """
        covered: Set[int] = set()
        for description in dataset.describe_indices():
            if description.name == self.index_name and self.column in description.field_names:
                for segment in description.segments:
                    covered.update(segment.fragment_ids)
        return covered

    def target_fragments(self, dataset: "lance.LanceDataset") -> List[int]:
        """Return fragments to index.

        Args:
            dataset: The dataset to inspect.

        Returns:
            Every fragment when rebuilding, otherwise only uncovered fragments.
        """
        all_ids: List[int] = [fragment.fragment_id for fragment in dataset.get_fragments()]
        if self.config.rebuild:
            return all_ids
        covered: Set[int] = self.covered_fragments(dataset)
        return [fragment_id for fragment_id in all_ids if fragment_id not in covered]

    def prepare(
        self, dataset: "lance.LanceDataset", uri: str, telemetry: Telemetry
    ) -> Optional[object]:
        """Build artifacts to broadcast to the segment builders.

        Args:
            dataset: The dataset being indexed.
            uri: Dataset URI.
            telemetry: Driver telemetry facade.

        Returns:
            A broadcastable artifact, or ``None`` when none is needed.
        """
        return None

    def build_segment(
        self, dataset: "lance.LanceDataset", fragment_ids: List[int], artifacts: Optional[object]
    ) -> Index:
        """Build one uncommitted segment over a shard of fragments.

        Args:
            dataset: A dataset handle pinned to the build version.
            fragment_ids: The fragment ids for this shard.
            artifacts: The broadcast artifact, or ``None``.

        Returns:
            The uncommitted segment metadata.
        """
        raise NotImplementedError

    def build(self, spark: SparkSession, uri: str, telemetry: Telemetry) -> Dict[str, Any]:
        """Build and commit this index across executors.

        Args:
            spark: Active Spark session.
            uri: Dataset URI.
            telemetry: Driver telemetry facade.

        Returns:
            A statistics dictionary for the index.
        """
        config: IndexJobConfig = self.config
        dataset: "lance.LanceDataset" = lance.dataset(uri, storage_options=config.storage_options)
        self.validate(dataset)
        targets: List[int] = self.target_fragments(dataset)
        if not targets:
            return {"column": self.column, "index": self.index_name, "segments": 0, "fragments": 0}

        artifacts: Optional[object] = self.prepare(dataset, uri, telemetry)
        version: int = dataset.version
        groups: List[List[int]] = split_evenly(targets, config.num_shards)
        spark_context = spark.sparkContext
        broadcast_artifacts = spark_context.broadcast(artifacts) if artifacts is not None else None
        handler: "IndexHandler" = self
        storage_options: Optional[Dict[str, Any]] = config.storage_options
        index_type: str = self.index_type()

        def build_partition(group_iterator: Iterator[List[int]]) -> Iterator[str]:
            """Build one segment per shard on an executor.

            Args:
                group_iterator: Fragment-id shards assigned to this task.

            Yields:
                The serialized segment for each shard.
            """
            telemetry_local: Telemetry = Telemetry.create(config.telemetry)
            local_artifacts: Optional[object] = (
                broadcast_artifacts.value if broadcast_artifacts is not None else None
            )
            tags: List[str] = [f"index_type:{index_type}"]
            with telemetry_local.span("lance.indexing.build_segment"):
                for group in group_iterator:
                    shard_dataset: "lance.LanceDataset" = lance.dataset(
                        uri, version=version, storage_options=storage_options
                    )
                    with telemetry_local.timed("segment.build_ms", tags=tags):
                        segment = handler.build_segment(shard_dataset, list(group), local_artifacts)
                    telemetry_local.incr("segment.built", tags=tags)
                    yield serialize_segment(segment)

        with telemetry.timed("index.build_ms", tags=[f"index:{self.index_name}"]):
            segment_documents: List[str] = (
                spark_context.parallelize(groups, len(groups)).mapPartitions(build_partition).collect()
            )
        with telemetry.timed("index.commit_ms", tags=[f"index:{self.index_name}"]):
            commit_segments(
                uri, segment_documents, self.column, self.index_name, self.merges(), config, telemetry
            )
        result: Dict[str, Any] = {
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

    def extra_stats(self) -> Dict[str, Any]:
        """Return whether artifacts were reused.

        Returns:
            A mapping with the artifact reuse flag.
        """
        return {"reused_artifacts": self.reused_artifacts}

    def dimension(self, dataset: "lance.LanceDataset") -> int:
        """Return the vector dimension of the indexed column.

        Args:
            dataset: The dataset to inspect.

        Returns:
            The fixed vector dimension.
        """
        return IndicesBuilder(dataset, self.column).dimension

    def validate(self, dataset: "lance.LanceDataset") -> None:
        """Validate the IVF_RQ parameters against the column.

        Args:
            dataset: The dataset to validate against.

        Raises:
            ValueError: If parameters or the dimension are unsupported.
        """
        if self.config.num_partitions is None:
            raise ValueError("num_partitions is required to build the IVF_RQ index")
        if self.config.num_bits != 1:
            raise ValueError("IVF_RQ supports num_bits=1; higher widths are gated")
        if self.dimension(dataset) % 8 != 0:
            raise ValueError("IVF_RQ requires the vector dimension to be divisible by 8")

    def validate_manifest(self, manifest: Dict[str, Any], dimension: int) -> None:
        """Check that reused artifacts match the requested configuration.

        Args:
            manifest: The stored artifact manifest.
            dimension: The vector dimension of the column.

        Raises:
            ValueError: If any pinned parameter differs from the request.
        """
        config: IndexJobConfig = self.config
        expected: Dict[str, Any] = {
            "dimension": dimension,
            "metric": config.metric,
            "num_partitions": config.num_partitions,
            "num_bits": config.num_bits,
        }
        for name, value in expected.items():
            if manifest.get(name) != value:
                raise ValueError(
                    f"reused artifact {name} {manifest.get(name)!r} does not match {value!r}"
                )

    def prepare(
        self, dataset: "lance.LanceDataset", uri: str, telemetry: Telemetry
    ) -> Optional[object]:
        """Load this dataset's IVF_RQ artifacts, or train and persist them.

        Args:
            dataset: The dataset to train on if artifacts are absent.
            uri: Dataset URI used to locate the artifact sidecar.
            telemetry: Driver telemetry facade.

        Returns:
            The centroids IPC bytes paired with the RaBitQ model string.
        """
        config: IndexJobConfig = self.config
        dimension: int = self.dimension(dataset)
        filesystem, base_path = resolve_filesystem(
            artifact_directory(uri, self.column), config.storage_options
        )
        manifest_path: str = f"{base_path.rstrip('/')}/manifest.json"
        centroids_path: str = f"{base_path.rstrip('/')}/ivf_centroids.arrow"

        if config.reuse_artifacts and not config.rebuild and object_exists(filesystem, manifest_path):
            manifest: Dict[str, Any] = json.loads(read_object(filesystem, manifest_path))
            self.validate_manifest(manifest, dimension)
            self.reused_artifacts = True
            telemetry.incr("artifacts.reused")
            return read_object(filesystem, centroids_path), manifest["rabitq_model"]

        with telemetry.timed("artifacts.train_ms"):
            ivf_model = IndicesBuilder(dataset, self.column).train_ivf(
                num_partitions=config.num_partitions,
                distance_type=config.resolved_distance_type(),
                sample_rate=config.train_sample_rate,
                max_iters=config.train_max_iters,
            )
            centroids_bytes: bytes = centroids_to_ipc(ivf_model.centroids)
            rabitq_model: str = native_indices.build_rq_model(
                dimension=dimension, num_bits=config.num_bits
            )
        manifest = {
            "dimension": dimension,
            "metric": config.metric,
            "num_partitions": config.num_partitions,
            "num_bits": config.num_bits,
            "distance_type": config.resolved_distance_type(),
            "rabitq_model": rabitq_model,
            "created_at": datetime.now(timezone.utc).isoformat(),
        }
        write_object(filesystem, centroids_path, centroids_bytes)
        write_object(filesystem, manifest_path, json.dumps(manifest).encode("utf-8"))
        self.reused_artifacts = False
        telemetry.incr("artifacts.trained")
        return centroids_bytes, rabitq_model

    def build_segment(
        self, dataset: "lance.LanceDataset", fragment_ids: List[int], artifacts: Optional[object]
    ) -> Index:
        """Build one IVF_RQ segment over a shard of fragments.

        Args:
            dataset: A dataset handle pinned to the build version.
            fragment_ids: The fragment ids for this shard.
            artifacts: The centroids bytes paired with the RaBitQ model.

        Returns:
            The uncommitted segment metadata.
        """
        centroids_bytes, rabitq_model = artifacts
        centroids: pa.Array = centroids_from_ipc(centroids_bytes)
        return dataset.create_index_uncommitted(
            column=self.column,
            index_type="IVF_RQ",
            name=self.index_name,
            metric=self.config.metric,
            num_partitions=self.config.num_partitions,
            num_bits=self.config.num_bits,
            ivf_centroids=centroids,
            rabitq_model=rabitq_model,
            fragment_ids=fragment_ids,
        )


class BTreeIndexHandler(IndexHandler):
    """Builds a btree scalar index whose segments are committed disjointly."""

    def index_type(self) -> str:
        """Return the btree index type.

        Returns:
            The string ``BTREE``.
        """
        return "BTREE"

    def build_segment(
        self, dataset: "lance.LanceDataset", fragment_ids: List[int], artifacts: Optional[object]
    ) -> Index:
        """Build one btree segment over a shard of fragments.

        Args:
            dataset: A dataset handle pinned to the build version.
            fragment_ids: The fragment ids for this shard.
            artifacts: Unused.

        Returns:
            The uncommitted segment metadata.
        """
        return dataset.create_index_uncommitted(
            column=self.column,
            index_type="BTREE",
            name=self.index_name,
            fragment_ids=fragment_ids,
        )


class BitmapIndexHandler(IndexHandler):
    """Builds a bitmap scalar index whose segments are committed disjointly."""

    def index_type(self) -> str:
        """Return the bitmap index type.

        Returns:
            The string ``BITMAP``.
        """
        return "BITMAP"

    def build_segment(
        self, dataset: "lance.LanceDataset", fragment_ids: List[int], artifacts: Optional[object]
    ) -> Index:
        """Build one bitmap segment over a shard of fragments.

        Args:
            dataset: A dataset handle pinned to the build version.
            fragment_ids: The fragment ids for this shard.
            artifacts: Unused.

        Returns:
            The uncommitted segment metadata.
        """
        return dataset.create_index_uncommitted(
            column=self.column,
            index_type="BITMAP",
            name=self.index_name,
            fragment_ids=fragment_ids,
        )


class FtsIndexHandler(IndexHandler):
    """Builds a full-text BM25 inverted index via the metadata-merge path.

    Inverted indices are not built through the segment API. Each shard builds its
    fragments under one shared index id, the driver merges the per-fragment
    metadata, and the index is published with a create-index commit. The whole
    index is rebuilt each run, so any existing index of the same name is dropped
    first.
    """

    def index_type(self) -> str:
        """Return the inverted index type.

        Returns:
            The string ``INVERTED``.
        """
        return "INVERTED"

    def field_id(self, dataset: "lance.LanceDataset") -> int:
        """Return the top-level field index of the text column.

        Args:
            dataset: The dataset to inspect.

        Returns:
            The Arrow schema field index.

        Raises:
            ValueError: If the column is not a top-level field.
        """
        index: int = dataset.schema.get_field_index(self.column)
        if index < 0:
            raise ValueError(f"text column {self.column} is not a top-level field")
        return index

    def commit_index(
        self,
        uri: str,
        dataset: "lance.LanceDataset",
        index_uuid: str,
        fragment_ids: List[int],
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
            RuntimeError: If commits keep conflicting past the retry budget.
        """
        config: IndexJobConfig = self.config
        field_id: int = self.field_id(dataset)
        index_name: str = self.index_name
        fragments: Set[int] = set(fragment_ids)
        storage_options: Optional[Dict[str, Any]] = config.storage_options
        tags: List[str] = ["index_type:INVERTED"]

        def action() -> None:
            """Publish the merged inverted index at the latest version."""
            current: "lance.LanceDataset" = lance.dataset(uri, storage_options=storage_options)
            index: Index = Index(
                uuid=index_uuid,
                name=index_name,
                fields=[field_id],
                dataset_version=current.version,
                fragment_ids=fragments,
                index_version=0,
            )
            operation = lance.LanceOperation.CreateIndex(new_indices=[index], removed_indices=[])
            lance.LanceDataset.commit(
                uri, operation, read_version=current.version, storage_options=storage_options
            )
            telemetry.incr("index.committed", tags=tags)

        commit_with_retries(
            action,
            config.commit_retries,
            config.commit_backoff_seconds,
            lambda: telemetry.incr("index.commit_conflict", tags=tags),
        )

    def build(self, spark: SparkSession, uri: str, telemetry: Telemetry) -> Dict[str, Any]:
        """Build and commit the inverted index across executors.

        Args:
            spark: Active Spark session.
            uri: Dataset URI.
            telemetry: Driver telemetry facade.

        Returns:
            A statistics dictionary for the index.
        """
        config: IndexJobConfig = self.config
        dataset: "lance.LanceDataset" = lance.dataset(uri, storage_options=config.storage_options)
        if self.covered_fragments(dataset):
            dataset.drop_index(self.index_name)
            dataset = lance.dataset(uri, storage_options=config.storage_options)

        fragment_ids: List[int] = [fragment.fragment_id for fragment in dataset.get_fragments()]
        if not fragment_ids:
            return {"column": self.column, "index": self.index_name, "segments": 0, "fragments": 0}

        version: int = dataset.version
        index_uuid: str = str(uuid.uuid4())
        params: Dict[str, Any] = config.fts_params()
        groups: List[List[int]] = split_evenly(fragment_ids, config.num_shards)
        column: str = self.column
        index_name: str = self.index_name
        storage_options: Optional[Dict[str, Any]] = config.storage_options

        def build_partition(group_iterator: Iterator[List[int]]) -> Iterator[int]:
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
                    shard_dataset: "lance.LanceDataset" = lance.dataset(
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
            counts: List[int] = (
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


class LanceIndexer:
    """Builds the configured indices on Lance datasets via per-type handlers."""

    def __init__(self, config: IndexJobConfig) -> None:
        """Initialize the indexer.

        Args:
            config: Indexing configuration.
        """
        self.config: IndexJobConfig = config

    def handlers(self) -> List[IndexHandler]:
        """Build the index handlers selected by the configuration.

        Returns:
            One handler per configured index.
        """
        config: IndexJobConfig = self.config
        result: List[IndexHandler] = []
        if config.vector_column is not None:
            result.append(
                VectorIndexHandler(config, config.vector_column, config.resolved_vector_index_name())
            )
        for column in config.scalar_columns:
            result.append(BTreeIndexHandler(config, column, scalar_index_name(column)))
        for column in config.bitmap_columns:
            result.append(BitmapIndexHandler(config, column, bitmap_index_name(column)))
        for column in config.text_columns:
            result.append(FtsIndexHandler(config, column, fts_index_name(column)))
        return result

    def build(self, spark: SparkSession, uri: str, telemetry: Telemetry) -> Dict[str, Any]:
        """Build every configured index for one dataset.

        Args:
            spark: Active Spark session.
            uri: Dataset URI.
            telemetry: Driver telemetry facade.

        Returns:
            A statistics dictionary for the dataset's indices.
        """
        indexes: List[Dict[str, Any]] = []
        for handler in self.handlers():
            with telemetry.span("lance.indexing.index") as index_span:
                index_span.set_tag("index", handler.index_name)
                index_span.set_tag("index_type", handler.index_type())
                indexes.append(handler.build(spark, uri, telemetry))
        return {"uri": uri, "indexes": indexes}

    def run(self, spark: SparkSession, dataset_uris: List[str]) -> List[Dict[str, Any]]:
        """Index each dataset, distributing its build across executors.

        One distributed job is run per dataset; any failure propagates and fails
        the job.

        Args:
            spark: Active Spark session.
            dataset_uris: Datasets to index.

        Returns:
            One statistics dictionary per dataset.
        """
        driver_telemetry: Telemetry = Telemetry.create(self.config.telemetry)
        with driver_telemetry.span("lance.indexing.run") as run_span:
            run_span.set_tag("dataset_count", len(dataset_uris))
            results: List[Dict[str, Any]] = []
            for uri in dataset_uris:
                with driver_telemetry.span("lance.indexing.dataset") as dataset_span:
                    dataset_span.set_tag("uri", uri)
                    try:
                        with driver_telemetry.timed("dataset.total_ms", tags=[f"uri:{uri}"]):
                            stats: Dict[str, Any] = self.build(spark, uri, driver_telemetry)
                    except Exception:
                        driver_telemetry.error(f"indexing failed for {uri}", tags=[f"uri:{uri}"])
                        raise
                    segment_total: int = sum(int(item["segments"]) for item in stats["indexes"])
                    dataset_span.set_tag("indexes", len(stats["indexes"]))
                    dataset_span.set_tag("segments", segment_total)
                    driver_telemetry.gauge("dataset.segments", segment_total, tags=[f"uri:{uri}"])
                    logger.info(
                        "indexed %s: %d indices, %d segments",
                        uri,
                        len(stats["indexes"]),
                        segment_total,
                    )
                    results.append(stats)
            driver_telemetry.gauge("run.datasets", len(results))
            logger.info("indexing run: %d datasets", len(results))
            return results
