"""Per-type index handlers: vector (IVF_RQ), scalar (BTREE/BITMAP), and full-text (INVERTED).

Each handler encapsulates the build and maintain logic for one index type. The base
:class:`IndexHandler` provides the segment-API flow shared by scalar types. Subclasses override
the steps they specialise.
"""

from __future__ import annotations

import functools
import logging
import uuid
from collections.abc import Callable, Iterator
from typing import Any

import lance
import pyarrow as pa
from lance.dataset import Index
from lance.indices import IndicesBuilder
from pyspark.sql import SparkSession

from lance_etl.indexing.config import (
    IndexJobConfig,
    config_reusable,
    degrade_num_partitions,
    derive_num_partitions,
    memory_bounded_num_partitions,
)
from lance_etl.indexing.optimize import (
    drop_existing_index,
    index_delta_count,
    load_vector_config,
    maintain_index_locally,
    merge_index_deltas,
    write_vector_config,
)
from lance_etl.indexing.segments import (
    TRAIN_SEMAPHORE,
    all_fragment_ids,
    build_and_commit_segments,
    build_scalar_segment,
    build_vector_segment,
    centroids_to_ipc,
    commit_index_with_retries,
    lance_field_id,
    live_fragment_ids,
    serialize_segment,
    split_evenly,
    train_vector_artifacts,
)
from lance_etl.telemetry import Telemetry

logger: logging.Logger = logging.getLogger(__name__)


class IndexHandler:
    """Base handler that builds one index over a dataset's fragments.

    The default :meth:`build` implements the segment-API flow shared by the vector handler: split
    target fragments into shards, build one uncommitted segment per shard across executors, and
    commit the collected segments. Subclasses override the build steps or the whole flow.
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

        Subclasses override this to raise ``ValueError`` when the dataset does not satisfy the
        index's prerequisites. The base implementation accepts any dataset.

        Args:
            dataset: The dataset to validate against.
        """
        del dataset

    def skip_reason(self, dataset: lance.LanceDataset) -> str | None:
        """Return why this index should be skipped for the dataset, if at all.

        Subclasses override this to opt out of indexing, for example when the dataset is too small
        to benefit. The base implementation never skips.

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
        all_ids: list[int] = all_fragment_ids(dataset)
        if self.config.rebuild:
            return all_ids
        covered: set[int] = self.covered_fragments(dataset)
        return [fragment_id for fragment_id in all_ids if fragment_id not in covered]

    def prepare(
        self,
        dataset: lance.LanceDataset,
        uri: str,
        telemetry: Telemetry,
        spark: SparkSession | None = None,
    ) -> object | None:
        """Build artifacts to broadcast to the segment builders.

        Subclasses override this to train or load artifacts that are broadcast to each executor
        shard. The base implementation requires no artifacts.

        Args:
            dataset: The dataset being indexed.
            uri: Dataset URI.
            telemetry: Driver telemetry facade.
            spark: Active Spark session, unused by the base implementation.

        Returns:
            A broadcastable artifact, or ``None`` when none is needed.
        """
        del dataset, uri, telemetry, spark
        return None

    def build_segment(self, dataset: lance.LanceDataset, fragment_ids: list[int], artifacts: object | None) -> Index:
        """Build one uncommitted scalar segment over a shard of fragments.

        This base implementation covers the artifact-free scalar types (BTREE and BITMAP). It
        delegates to the module-level :func:`~lance_etl.indexing.segments.build_scalar_segment` so
        the same logic backs both direct calls and the closure-friendly builder returned by
        :meth:`segment_builder`. Handlers that need broadcast artifacts override this method.

        Args:
            dataset: A dataset handle pinned to the build version.
            fragment_ids: The fragment ids for this shard.
            artifacts: The broadcast artifact, unused by scalar builds.

        Returns:
            The uncommitted segment metadata.
        """
        return build_scalar_segment(
            dataset,
            fragment_ids,
            artifacts,
            column=self.column,
            index_name=self.index_name,
            index_type=self.index_type(),
        )

    def segment_builder(self) -> Callable[[lance.LanceDataset, list[int], object | None], Index]:
        """Return a picklable per-shard segment builder that does not capture the handler instance.

        The Spark closure in :meth:`build` ships this callable to executors. Returning a
        :func:`functools.partial` over the module-level
        :func:`~lance_etl.indexing.segments.build_scalar_segment` with only primitive values keeps
        the serialized task small. Capturing the bound ``self.build_segment`` instead would pickle
        the whole handler, including its ``config`` with ``storage_options`` and ``telemetry``,
        onto every task.

        Returns:
            A callable taking the shard dataset, fragment ids, and broadcast artifacts.
        """
        return functools.partial(
            build_scalar_segment,
            column=self.column,
            index_name=self.index_name,
            index_type=self.index_type(),
        )

    def merge_deltas(self, spark: SparkSession, uri: str, telemetry: Telemetry) -> bool:
        """Merge this index's accumulated deltas on one executor when over the cap.

        The driver only reads the index statistics. The merge itself, which can approach a rebuild
        for sort-merge scalar types, runs in a single-task Spark job so heavy work stays off the
        driver.

        Args:
            spark: Active Spark session.
            uri: Dataset URI.
            telemetry: Driver telemetry facade.

        Returns:
            ``True`` if a merge ran.
        """
        config: IndexJobConfig = self.config
        dataset: lance.LanceDataset = lance.dataset(uri, storage_options=config.storage_options)
        if self.index_name not in {description.name for description in dataset.describe_indices()}:
            return False
        if index_delta_count(dataset, self.index_name) <= config.max_index_deltas:
            return False
        index_name: str = self.index_name

        def merge_one(target: str) -> bool:
            """Merge the index deltas inside an executor task.

            Args:
                target: Dataset URI.

            Returns:
                ``True`` if a merge ran.
            """
            return merge_index_deltas(target, index_name, config, Telemetry.create(config.telemetry))

        with telemetry.timed("index.delta_merge_ms", tags=[f"index:{index_name}"]):
            merged: list[bool] = spark.sparkContext.parallelize([uri], 1).map(merge_one).collect()
        return bool(merged and merged[0])

    def build(self, spark: SparkSession, uri: str, telemetry: Telemetry) -> dict[str, Any]:
        """Build and commit this index across executors, then bound its deltas.

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

        spark_context = spark.sparkContext
        build_segment: Callable[[lance.LanceDataset, list[int], object | None], Index] = self.segment_builder()
        storage_options: dict[str, Any] | None = config.storage_options
        index_type: str = self.index_type()

        def build_documents(groups: list[list[int]], version: int, artifacts: object | None) -> list[str]:
            """Build one serialized segment per shard across executors at the pinned version.

            Args:
                groups: Fragment-id shards to build.
                version: Dataset version to pin every shard build to.
                artifacts: Broadcast artifacts for the segment builder, if any.

            Returns:
                The serialized segments collected from the executors.
            """
            broadcast_artifacts = spark_context.broadcast(artifacts) if artifacts is not None else None

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

            return spark_context.parallelize(groups, len(groups)).mapPartitions(build_partition).collect()

        with telemetry.timed("index.build_ms", tags=[f"index:{self.index_name}"]):
            stats: dict[str, int] = build_and_commit_segments(
                uri, self, config, telemetry, build_documents, spark=spark
            )
        result: dict[str, Any] = {
            "column": self.column,
            "index": self.index_name,
            "segments": stats["segments"],
            "fragments": stats["fragments"],
            "deltas_merged": self.merge_deltas(spark, uri, telemetry),
        }
        result.update(self.extra_stats())
        return result


class VectorIndexHandler(IndexHandler):
    """Builds an IVF_RQ vector index, storing artifacts in the dataset's own config KV.

    On the first build the IVF centroids and RaBitQ model are trained and written once via
    :func:`~lance_etl.indexing.optimize.write_vector_config` under the key
    ``lance-etl.vector.{column}``. On every subsequent incremental run the config is read back
    with :func:`~lance_etl.indexing.optimize.load_vector_config`, the centroids are recovered from
    the committed index via :meth:`lance.LanceDataset.get_ivf_model`, and no external writes are
    made. No sidecar files or directories are created. The ``cached_artifacts`` field memoizes the
    prepare result within one build call so the replan loop does not retrain when a stale-fragment
    rebuild triggers a second ``prepare``.
    """

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
        self.cached_artifacts: tuple | None = None
        self.full_rebuild: bool = False

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

        When ``reused_artifacts`` is true, the centroids were read back from the committed index
        and no training or external writes were performed.

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
            ValueError: If the dimension is unsupported.
        """
        if self.dimension(dataset) % 8 != 0:
            raise ValueError("IVF_RQ requires the vector dimension to be divisible by 8")

    def growth_requires_retrain(self, cfg: dict[str, Any], rows: int) -> bool:
        """Decide whether dataset growth since training forces a centroid retrain.

        A config without ``rows_at_train`` predates the retrain trigger and retrains once to
        record it.

        Args:
            cfg: The stored artifact config.
            rows: The dataset's current row count.

        Returns:
            ``True`` when the artifacts must be retrained instead of reused.
        """
        rows_at_train: Any = cfg.get("rows_at_train")
        if rows_at_train is None:
            return True
        return rows > self.config.retrain_growth_factor * int(rows_at_train)

    def target_fragments(self, dataset: lance.LanceDataset) -> list[int]:
        """Return fragments to index, expanding to all of them when a retrain is needed.

        Retrained centroids and rotation cannot merge with segments built from the old artifacts,
        so when the growth trigger fires every fragment is rebuilt, exactly as on a ``rebuild``
        run. A non-reusable config (changed dimension or metric) is also treated as a
        full-rebuild trigger so the index self-heals rather than remaining broken. An existing
        index with no stored config at all gets the same treatment: it was built by the
        small-dataset tier's plain ``create_index`` under its own private model, so appending
        segments built from freshly trained artifacts would create deltas whose IVF centroids and
        RaBitQ rotation disagree, and a later delta merge would silently corrupt the index by
        copying quantized codes across mismatched models. Once any trigger fires the decision is
        sticky for this handler instance (one build call), so a stale-fragment replan keeps
        rebuilding everything even after ``prepare`` has refreshed the stored config.

        Args:
            dataset: The dataset to inspect.

        Returns:
            Every fragment when retraining, rebuilding, or recovering from missing or mismatched
            artifacts, otherwise only uncovered fragments.
        """
        config: IndexJobConfig = self.config
        if not config.rebuild and not self.full_rebuild:
            cfg: dict[str, Any] | None = load_vector_config(dataset, self.column)
            if cfg is None:
                if self.covered_fragments(dataset):
                    logger.warning(
                        "index %s on %s exists without stored vector artifacts (small-tier build); "
                        "it will be retrained and fully rebuilt to keep all deltas on one model",
                        self.index_name,
                        dataset.uri,
                    )
                    self.full_rebuild = True
            elif not config_reusable(cfg, self.dimension(dataset), config.metric, config.ivf_rq_num_bits):
                logger.warning(
                    "stored vector config for %s on %s no longer matches the current configuration; "
                    "the index will be retrained and fully rebuilt",
                    self.index_name,
                    dataset.uri,
                )
                self.full_rebuild = True
            elif self.growth_requires_retrain(cfg, dataset.count_rows()):
                self.full_rebuild = True
        if self.full_rebuild:
            return all_fragment_ids(dataset)
        return super().target_fragments(dataset)

    def prepare(
        self,
        dataset: lance.LanceDataset,
        uri: str,
        telemetry: Telemetry,
        spark: SparkSession | None = None,
    ) -> object | None:
        """Load or train the IVF_RQ artifacts for this dataset's vector column.

        Returns the memoized result immediately on subsequent calls within the same build (replan
        loop). On the first call the reuse branch is taken when the dataset config contains a
        valid, reusable entry for this column, the index is actually committed (a stored config
        can outlive its index when a first commit was skipped as all-stale, and ``get_ivf_model``
        raises on a missing index), and the committed index has a non-None IVF model with
        centroids. Centroids are read back from the committed index via
        :meth:`lance.LanceDataset.get_ivf_model` and IPC-serialized for the Spark broadcast using
        :func:`~lance_etl.indexing.segments.centroids_to_ipc`. The ``rabitq_model`` string comes
        from the stored config. ``num_partitions`` is derived as ``len(centroids)`` — no stored
        value is needed.

        If any reuse condition fails (absent config, config mismatch, growth trigger, absent or
        None IVF model), the train branch runs. When ``spark`` is provided, IVF centroid training
        is offloaded to a single-task Spark job via
        :func:`~lance_etl.indexing.segments.train_vector_artifacts`, keeping heavy sample I/O and
        k-means compute off the driver. When ``spark`` is ``None`` (tests or no-cluster callers),
        training runs in-process under :data:`~lance_etl.indexing.segments.TRAIN_SEMAPHORE`.
        After training the config is written once via
        :func:`~lance_etl.indexing.optimize.write_vector_config` with the key shape
        ``{"rows_at_train": int, "dimension": int, "metric": str, "num_bits": int,
        "rabitq_model": str}``.

        Args:
            dataset: The dataset to train on if artifacts are absent or stale.
            uri: Dataset URI used for the config write and executor training.
            telemetry: Driver telemetry facade.
            spark: Active Spark session. When provided, centroid training is dispatched to one
                executor task so the driver does not hold the training sample in its heap. When
                ``None`` training runs in-process (test or no-cluster fallback).

        Returns:
            The centroids IPC bytes, the RaBitQ model JSON string, num_bits, and the IVF partition
            count.
        """
        if self.cached_artifacts is not None:
            return self.cached_artifacts

        config: IndexJobConfig = self.config
        dimension: int = self.dimension(dataset)
        rows: int = dataset.count_rows()

        if not config.rebuild:
            cfg: dict[str, Any] | None = load_vector_config(dataset, self.column)
            if cfg is not None and config_reusable(cfg, dimension, config.metric, config.ivf_rq_num_bits):
                if not self.growth_requires_retrain(cfg, rows):
                    committed: set[str] = {description.name for description in dataset.describe_indices()}
                    ivf_model = dataset.get_ivf_model(self.index_name) if self.index_name in committed else None
                    if ivf_model is not None and ivf_model.centroids is not None:
                        centroids: pa.Array = ivf_model.centroids
                        centroids_bytes: bytes = centroids_to_ipc(centroids)
                        rabitq_model: str = cfg["rabitq_model"]
                        num_partitions: int = len(centroids)
                        self.reused_artifacts = True
                        self.num_partitions_used = num_partitions
                        telemetry.incr("artifacts.reused")
                        self.cached_artifacts = (centroids_bytes, rabitq_model, config.ivf_rq_num_bits, num_partitions)
                        return self.cached_artifacts
                    logger.info(
                        "IVF model for %s on %s is absent or has no centroids; falling through to train",
                        self.index_name,
                        uri,
                    )
                else:
                    telemetry.incr("artifacts.retrained_for_growth")
                    logger.info(
                        "retraining IVF artifacts for %s: %d rows exceed %.1fx rows_at_train=%s",
                        uri,
                        rows,
                        config.retrain_growth_factor,
                        cfg.get("rows_at_train"),
                    )
            elif cfg is not None:
                logger.warning(
                    "stored vector config for %s is not reusable (missing rabitq_model or config mismatch); "
                    "retraining artifacts",
                    uri,
                )

        planned: int = derive_num_partitions(rows, config.num_partitions, config)
        after_degrade: int = degrade_num_partitions(planned, rows, config.train_sample_rate)
        partitions: int = memory_bounded_num_partitions(after_degrade, dimension, config)
        if partitions < planned:
            telemetry.incr("artifacts.partitions_degraded")
            if partitions < after_degrade:
                logger.warning(
                    "capped num_partitions %d -> %d for %s (dim=%d): training sample would exceed memory budget",
                    after_degrade,
                    partitions,
                    uri,
                    dimension,
                )
            else:
                logger.info("degraded num_partitions %d -> %d for %s (%d rows)", planned, partitions, uri, rows)

        with TRAIN_SEMAPHORE, telemetry.timed("artifacts.train_ms", tags=[f"index:{self.index_name}"]):
            if spark is not None:
                train_fn = functools.partial(
                    train_vector_artifacts,
                    column=self.column,
                    num_partitions=partitions,
                    sample_rate=config.train_sample_rate,
                    max_iters=config.train_max_iters,
                    num_bits=config.ivf_rq_num_bits,
                    distance_type=config.resolved_distance_type(),
                    storage_options=config.storage_options,
                )
                result: tuple[bytes, str] = spark.sparkContext.parallelize([uri], 1).map(train_fn).collect()[0]
                trained_centroids_bytes, trained_rabitq_model = result
            else:
                trained_centroids_bytes, trained_rabitq_model = train_vector_artifacts(
                    uri=uri,
                    column=self.column,
                    num_partitions=partitions,
                    sample_rate=config.train_sample_rate,
                    max_iters=config.train_max_iters,
                    num_bits=config.ivf_rq_num_bits,
                    distance_type=config.resolved_distance_type(),
                    storage_options=config.storage_options,
                )

        new_cfg: dict[str, Any] = {
            "rows_at_train": rows,
            "dimension": dimension,
            "metric": config.metric,
            "num_bits": config.ivf_rq_num_bits,
            "num_partitions": partitions,
            "rabitq_model": trained_rabitq_model,
        }
        write_vector_config(uri, self.column, new_cfg, config, telemetry)
        self.reused_artifacts = False
        self.num_partitions_used = partitions
        telemetry.incr("artifacts.trained")
        self.cached_artifacts = (trained_centroids_bytes, trained_rabitq_model, config.ivf_rq_num_bits, partitions)
        return self.cached_artifacts

    def build_segment(self, dataset: lance.LanceDataset, fragment_ids: list[int], artifacts: object | None) -> Index:
        """Build one IVF_RQ segment over a shard of fragments.

        Delegates to the module-level
        :func:`~lance_etl.indexing.segments.build_vector_segment` so the same logic backs both
        direct calls and the closure-friendly builder returned by :meth:`segment_builder`.

        Args:
            dataset: A dataset handle pinned to the build version.
            fragment_ids: The fragment ids for this shard.
            artifacts: The centroids bytes, the shared RaBitQ model string, num_bits, and the IVF
                partition count.

        Returns:
            The uncommitted segment metadata.

        Raises:
            ValueError: If ``artifacts`` is ``None``. Vector segment builds require the artifact
                tuple produced by :meth:`prepare`.
        """
        return build_vector_segment(
            dataset,
            fragment_ids,
            artifacts,
            column=self.column,
            index_name=self.index_name,
            metric=self.config.metric,
        )

    def segment_builder(self) -> Callable[[lance.LanceDataset, list[int], object | None], Index]:
        """Return a picklable IVF_RQ segment builder that does not capture the handler instance.

        Mirrors :meth:`IndexHandler.segment_builder` but binds the vector-specific
        :func:`~lance_etl.indexing.segments.build_vector_segment` with the metric. The centroids
        and RaBitQ model are not bound here. They reach executors through the separate artifact
        broadcast and arrive as the ``artifacts`` argument at call time.

        Returns:
            A callable taking the shard dataset, fragment ids, and broadcast artifacts.
        """
        return functools.partial(
            build_vector_segment,
            column=self.column,
            index_name=self.index_name,
            metric=self.config.metric,
        )


class BTreeIndexHandler(IndexHandler):
    """Builds a btree scalar index through the segment API.

    Each shard calls ``create_index_uncommitted`` and the driver publishes the collected segments
    with ``commit_existing_index_segments``. BTREE segments do not support driver-side merging, so
    they are committed unmerged. The segment build and incremental fragment coverage are inherited
    from :class:`IndexHandler`.
    """

    def index_type(self) -> str:
        """Return the btree index type.

        Returns:
            The string ``BTREE``.
        """
        return "BTREE"


class BitmapIndexHandler(IndexHandler):
    """Builds a bitmap scalar index through the segment API.

    Each shard calls ``create_index_uncommitted`` and the driver merges the collected segments into
    one with ``merge_existing_index_segments`` before publishing via
    ``commit_existing_index_segments``. The segment build and incremental fragment coverage are
    inherited from :class:`IndexHandler`.
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


class FtsIndexHandler(IndexHandler):
    """Maintains a full-text BM25 inverted index, rebuilding only when it must.

    An existing index whose unindexed backlog is within ``fts_max_unindexed_fragments`` is
    maintained incrementally on one executor with ``optimize_indices``, which merges INVERTED
    deltas natively and falls back internally to an old-plus-new rebuild only when the index's
    update criteria require it. The distributed metadata-merge rebuild remains for first builds,
    large backlogs, and ``rebuild`` runs after tokenizer-parameter changes. Inverted indices are
    not built through the segment API: each shard builds its fragments under one shared index id,
    the driver merges the per-fragment metadata, and the index is published with a create-index
    commit.
    """

    def index_type(self) -> str:
        """Return the inverted index type.

        Returns:
            The string ``INVERTED``.
        """
        return "INVERTED"

    def maintainable(self, dataset: lance.LanceDataset) -> bool:
        """Decide whether the existing index can be maintained incrementally.

        Args:
            dataset: The dataset to inspect.

        Returns:
            ``True`` when the index exists, no rebuild was requested, and the unindexed backlog is
            within the configured fragment threshold.
        """
        if self.config.rebuild or not self.covered_fragments(dataset):
            return False
        stats: dict[str, Any] = dataset.stats.index_stats(self.index_name)
        return int(stats.get("num_unindexed_fragments") or 0) <= self.config.fts_max_unindexed_fragments

    def maintain(self, spark: SparkSession, uri: str, telemetry: Telemetry) -> dict[str, Any]:
        """Maintain the existing inverted index incrementally on one executor.

        Runs ``optimize_indices`` for this index in a single-task Spark job, then bounds the delta
        count.

        Args:
            spark: Active Spark session.
            uri: Dataset URI.
            telemetry: Driver telemetry facade.

        Returns:
            A statistics dictionary for the index with ``maintained`` set.
        """
        config: IndexJobConfig = self.config
        index_name: str = self.index_name

        def maintain_one(target: str) -> bool:
            """Optimize and delta-bound the index inside an executor task.

            Args:
                target: Dataset URI.

            Returns:
                ``True`` if a delta merge ran.
            """
            return maintain_index_locally(target, index_name, config, Telemetry.create(config.telemetry))

        with telemetry.timed("index.build_ms", tags=[f"index:{index_name}"]):
            merged: list[bool] = spark.sparkContext.parallelize([uri], 1).map(maintain_one).collect()
        return {
            "column": self.column,
            "index": index_name,
            "segments": 0,
            "fragments": 0,
            "maintained": True,
            "deltas_merged": bool(merged and merged[0]),
        }

    def commit_index(
        self,
        uri: str,
        dataset: lance.LanceDataset,
        index_uuid: str,
        fragment_ids: list[int],
        telemetry: Telemetry,
    ) -> None:
        """Publish the merged inverted index, retrying conflicts.

        Each attempt validates that every covered fragment still exists at the latest version. A
        concurrent compaction can rewrite covered fragments between the executor build and this
        commit, and a blind retry at the new head version would then publish an index whose row
        addresses point at compacted-away fragments.

        Args:
            uri: Dataset URI.
            dataset: A dataset handle refreshed to the latest version after the executor build.
            index_uuid: The shared index id the shards built under.
            fragment_ids: The fragments the index covers.
            telemetry: Driver telemetry facade.

        Raises:
            ValueError: If covered fragments no longer exist because a compaction rewrote them.
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
            live: set[int] = live_fragment_ids(current)
            missing: set[int] = fragments - live
            if missing:
                raise ValueError(
                    f"inverted index {index_name} on {uri} covers fragments {sorted(missing)} that no longer exist; "
                    "a compaction rewrote them between build and commit, so this build must be redone"
                )
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

        commit_index_with_retries(action, config, telemetry, tags)

    def build(self, spark: SparkSession, uri: str, telemetry: Telemetry) -> dict[str, Any]:
        """Maintain the inverted index incrementally, or rebuild it across executors.

        An existing index with a small unindexed backlog is maintained with ``optimize_indices``
        on one executor. Otherwise the full distributed rebuild runs: the dataset handle is
        refreshed after the executor build so the metadata merge and the publish commit both
        operate against the latest committed version rather than the snapshot captured before the
        Spark job ran.

        On the distributed rebuild path an existing same-name index is dropped AFTER the executor
        builds complete, just before the metadata merge and the publish commit. This shrinks the
        availability gap compared to dropping before the Spark job: the old index stays live for
        the entire (potentially hours-long) executor build phase and is absent only for the short
        merge-plus-commit window.

        Args:
            spark: Active Spark session.
            uri: Dataset URI.
            telemetry: Driver telemetry facade.

        Returns:
            A statistics dictionary for the index.
        """
        config: IndexJobConfig = self.config
        dataset: lance.LanceDataset = lance.dataset(uri, storage_options=config.storage_options)
        if self.maintainable(dataset):
            return self.maintain(spark, uri, telemetry)
        has_existing: bool = bool(self.covered_fragments(dataset))

        fragment_ids: list[int] = all_fragment_ids(dataset)
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

            Uses ``replace=True`` to bypass the same-name existence guard on the uncommitted
            per-fragment path so the old committed index remains live and searchable while the
            executor builds run.

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
                                replace=True,
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
        if has_existing:
            drop_existing_index(uri, self.index_name, config, telemetry)
        dataset = lance.dataset(uri, storage_options=config.storage_options)
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
