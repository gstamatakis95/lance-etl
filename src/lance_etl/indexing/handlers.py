"""Per-type index handlers: vector (IVF_RQ), scalar (BTREE/BITMAP/ZONEMAP), and full-text (INVERTED).

Each handler encapsulates the build and maintain logic for one index type. The base
:class:`IndexHandler` provides the segment-API flow shared by scalar types. Subclasses override
the steps they specialise.
"""

from __future__ import annotations

import logging
from typing import Any

import lance
import pyarrow as pa
from lance.dataset import Index
from lance.indices import IndicesBuilder

from lance_etl.indexing.config import IndexJobConfig, config_reusable, growth_exceeds_retrain_factor
from lance_etl.indexing.optimize import (
    load_centroids,
    load_vector_config,
    save_centroids,
)
from lance_etl.indexing.segments import (
    all_fragment_ids,
    build_scalar_segment,
    build_vector_segment,
    commit_index_with_retries,
    lance_field_id,
    live_fragment_ids,
)
from lance_etl.telemetry import Telemetry

logger: logging.Logger = logging.getLogger(__name__)


class IndexHandler:
    """Base policy for planning one index over a dataset's fragments.

    Subclasses validate type-specific prerequisites, select uncovered fragments, prepare build
    artifacts, and declare whether uncommitted segments merge before publication.
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

    def covered_fragments(self, dataset: lance.LanceDataset) -> set[int]:
        """Return fragments already covered by this index.

        Args:
            dataset: The dataset to inspect.

        Returns:
            The set of covered fragment ids.
        """
        covered: set[int] = set()
        description: Any
        for description in dataset.describe_indices():
            if description.name == self.index_name and self.column in description.field_names:
                segment: Any
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

    def prepare(self, dataset: lance.LanceDataset, uri: str, telemetry: Telemetry) -> object | None:
        """Build artifacts to broadcast to the segment builders.

        Subclasses override this to train or load artifacts that are broadcast to each executor
        shard. The base implementation requires no artifacts.

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
        """Build one uncommitted scalar segment over a shard of fragments.

        This base implementation covers the artifact-free scalar types (BTREE and BITMAP). It
        delegates to the module-level :func:`~lance_etl.indexing.segments.build_scalar_segment` so
        the same logic backs both direct calls and the closure-friendly builder returned by
        the runner's build tasks. Handlers that need broadcast artifacts override this method.

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


class VectorIndexHandler(IndexHandler):
    """Builds an IVF_RQ vector index, reusing centroids from an object-store sidecar cache.

    On the first build the RaBitQ model and the ``rows_at_train`` fingerprint are trained and
    written once via :func:`~lance_etl.indexing.optimize.write_vector_config` under the key
    ``lance-etl.vector.{column}``, and the trained centroids are cached to the object-store sidecar
    keyed by that fingerprint (ADR 0040). On every subsequent incremental run the config is read
    back with :func:`~lance_etl.indexing.optimize.load_vector_config` and the centroids are read
    sidecar-first with :func:`~lance_etl.indexing.optimize.load_centroids`, falling back to the
    committed index via :meth:`lance.LanceDataset.get_ivf_model` and backfilling the sidecar
    best-effort on a miss. Each executor shard creates one handler and calls ``prepare`` once, so
    artifacts are resolved directly without per-instance memoization.
    """

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
        record it. The threshold comparison itself is shared with
        :func:`~lance_etl.indexing.runner.vector_index_needs_retrain` through
        :func:`~lance_etl.indexing.config.growth_exceeds_retrain_factor`.

        Args:
            cfg: The stored artifact config.
            rows: The dataset's current row count.

        Returns:
            ``True`` when the artifacts must be retrained instead of reused.
        """
        rows_at_train: Any = cfg.get("rows_at_train")
        if rows_at_train is None:
            return True
        return growth_exceeds_retrain_factor(rows, int(rows_at_train), self.config.retrain_growth_factor)

    def needs_bootstrap(self, dataset: lance.LanceDataset) -> bool:
        """Decide whether this index must be rebuilt through a streaming bootstrap.

        Retrained centroids and rotation cannot merge with segments built from the old artifacts,
        so any artifact trigger routes the whole index back to the committed streaming
        ``create_index`` bootstrap (ADR 0030). Three triggers fire it. A non-reusable config
        (changed dimension, metric, or num_bits) means the index must self-heal rather than
        remain broken. Growth past ``RETRAIN_GROWTH_FACTOR`` times the recorded ``rows_at_train``
        means the centroids are stale. An existing index with no stored config at all gets the
        same treatment: it was built by a plain ``create_index`` under its own private model, so
        appending segments built from freshly trained artifacts would create deltas whose IVF
        centroids and RaBitQ rotation disagree, and a later delta merge would silently corrupt
        the index by copying quantized codes across mismatched models.

        Args:
            dataset: The dataset to inspect.

        Returns:
            ``True`` when the index must be retrained from scratch, ``False`` when its stored
            artifacts are reusable for incremental segment builds.
        """
        cfg: dict[str, Any] | None = load_vector_config(dataset, self.column)
        if cfg is None:
            if self.covered_fragments(dataset):
                logger.warning(
                    "index %s on %s exists without stored vector artifacts (plain create_index build); "
                    "it will be retrained and fully rebuilt to keep all deltas on one model",
                    self.index_name,
                    dataset.uri,
                )
            return True
        if not config_reusable(cfg, self.dimension(dataset), self.config.metric, self.config.num_bits):
            logger.warning(
                "stored vector config for %s on %s no longer matches the current configuration; "
                "the index will be retrained and fully rebuilt",
                self.index_name,
                dataset.uri,
            )
            return True
        return self.growth_requires_retrain(cfg, dataset.count_rows())

    def resolve_centroids(
        self, dataset: lance.LanceDataset, uri: str, cfg: dict[str, Any], telemetry: Telemetry
    ) -> pa.Array:
        """Resolve the reusable IVF centroids sidecar-first, falling back to the committed index.

        Reads the centroids from the object-store sidecar keyed by the stored ``rows_at_train``
        fingerprint (:func:`~lance_etl.indexing.optimize.load_centroids`). On a miss it reads them
        from the committed index via :meth:`lance.LanceDataset.get_ivf_model` and backfills the
        sidecar best-effort so later shards and runs reuse it (ADR 0040). The backfill never fails
        the build: a sidecar-write error is counted and logged, and the freshly read centroids are
        returned regardless.

        Args:
            dataset: The dataset whose committed index carries the centroids.
            uri: Dataset URI, for the sidecar path and error messages.
            cfg: The stored, already-validated reusable vector config.
            telemetry: Telemetry facade for the current process.

        Returns:
            The IVF centroid ``pa.Array``.

        Raises:
            RuntimeError: If the committed index carries no reusable centroids. The plan phase
                should have chosen a bootstrap build for this index.
        """
        config: IndexJobConfig = self.config
        rows_at_train: Any = cfg.get("rows_at_train")
        if rows_at_train is not None:
            cached: pa.Array | None = load_centroids(uri, self.index_name, int(rows_at_train), config.storage_options)
            if cached is not None:
                return cached
        ivf_model: Any = dataset.get_ivf_model(self.index_name)
        if ivf_model is None or ivf_model.centroids is None:
            raise RuntimeError(
                f"vector artifacts for {self.index_name} on {uri} are not reusable; "
                "the plan phase should have chosen a streaming bootstrap build"
            )
        centroids: pa.Array = ivf_model.centroids
        if rows_at_train is not None:
            try:
                save_centroids(
                    uri, self.index_name, centroids, config.metric, int(rows_at_train), config.storage_options
                )
                telemetry.incr("artifacts.centroid_sidecar_backfilled")
            except Exception as exc:
                telemetry.incr("index.centroid_sidecar_write_error")
                logger.warning("centroid sidecar backfill failed for %s on %s: %s", self.index_name, uri, exc)
        return centroids

    def prepare(self, dataset: lance.LanceDataset, uri: str, telemetry: Telemetry) -> tuple:
        """Resolve this dataset's reusable IVF_RQ artifacts, reading centroids sidecar-first.

        The centroids come from :meth:`resolve_centroids` (sidecar cache first, committed-index
        fallback with best-effort backfill), the ``rabitq_model`` string from the stored config,
        and the partition count as ``len(centroids)`` (equal to the stored ``num_partitions`` by
        the bootstrap invariant, and always consistent with the centroid array).

        This is reuse-only by invariant: the plan phase routes any dataset whose artifacts are
        absent, mismatched, or growth-stale to a streaming bootstrap build instead (ADR 0030),
        so reaching this method without a committed index and a reusable config is a planning
        bug and raises.

        Args:
            dataset: The dataset whose committed index and sidecar carry the centroids.
            uri: Dataset URI, for the sidecar path and error messages.
            telemetry: Telemetry facade for the current process.

        Returns:
            The centroid ``pa.Array``, the RaBitQ model JSON string, num_bits, and the IVF
            partition count.

        Raises:
            RuntimeError: If the artifacts are not reusable. The plan phase should have chosen
                a bootstrap build for this index.
        """
        config: IndexJobConfig = self.config
        cfg: dict[str, Any] | None = load_vector_config(dataset, self.column)
        committed: set[str] = {description.name for description in dataset.describe_indices()}
        reusable: bool = (
            cfg is not None
            and config_reusable(cfg, self.dimension(dataset), config.metric, config.num_bits)
            and self.index_name in committed
        )
        if not reusable or cfg is None:
            raise RuntimeError(
                f"vector artifacts for {self.index_name} on {uri} are not reusable; "
                "the plan phase should have chosen a streaming bootstrap build"
            )
        centroids: pa.Array = self.resolve_centroids(dataset, uri, cfg, telemetry)
        telemetry.incr("artifacts.reused")
        return (
            centroids,
            cfg["rabitq_model"],
            config.num_bits,
            len(centroids),
        )

    def build_segment(self, dataset: lance.LanceDataset, fragment_ids: list[int], artifacts: object | None) -> Index:
        """Build one IVF_RQ segment over a shard of fragments.

        Delegates to the module-level
        :func:`~lance_etl.indexing.segments.build_vector_segment` so the same logic backs both
        direct calls and the runner's build tasks.

        Args:
            dataset: A dataset handle pinned to the build version.
            fragment_ids: The fragment ids for this shard.
            artifacts: The centroids ``pa.Array``, the shared RaBitQ model string, num_bits, and
                the IVF partition count.

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

    Each shard calls ``create_index_uncommitted`` and the driver publishes the collected segments
    with ``commit_existing_index_segments``. BITMAP segments commit unmerged like BTREE (see
    :meth:`merges`), so there is no driver-side ``merge_existing_index_segments`` step: Lance
    unions the per-shard segments at query time and the delta-merge maintenance pass consolidates
    them on an executor (ADR 0033). The segment build and incremental fragment coverage are
    inherited from :class:`IndexHandler`.
    """

    def index_type(self) -> str:
        """Return the bitmap index type.

        Returns:
            The string ``BITMAP``.
        """
        return "BITMAP"

    def merges(self) -> bool:
        """Report that bitmap segments commit unmerged, like BTREE.

        The driver-side bitmap merge materializes every value bitmap on one heap (8-16 GB at
        a billion rows), so per-shard segments are committed as-is instead. Lance unions the
        segments in parallel at query time, and the delta-merge maintenance pass consolidates
        them through the streaming rebuild path on an executor.

        Returns:
            Always ``False``.
        """
        return False


class ZonemapIndexHandler(IndexHandler):
    """Builds a zonemap scalar index through the segment API.

    Each shard calls ``create_index_uncommitted`` and the driver merges the collected segments
    into one with ``merge_existing_index_segments`` before publishing via
    ``commit_existing_index_segments``. ZONEMAP is the only scalar type that merges before
    commit: BTREE and BITMAP both commit unmerged as deltas and rely on the delta-merge
    maintenance pass to consolidate them. Merging keeps one small zone-summary segment per index
    instead of accumulating one per build round, which matters because a ZONEMAP index is only
    effective when the column is approximately sorted across the whole scan, not per fragment.
    The segment build and incremental fragment coverage are inherited from :class:`IndexHandler`.
    """

    def index_type(self) -> str:
        """Return the zonemap index type.

        Returns:
            The string ``ZONEMAP``.
        """
        return "ZONEMAP"

    def merges(self) -> bool:
        """Report that zonemap segments are merged before commit.

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


def commit_fts_index(
    uri: str,
    column: str,
    index_name: str,
    index_uuid: str,
    fragment_ids: list[int],
    config: IndexJobConfig,
    telemetry: Telemetry,
) -> None:
    """Publish a rebuilt inverted index on an executor: merge metadata, then atomically swap.

    The old same-name index is never dropped separately. :func:`publish_fts_index` lists its
    committed segments at commit time and passes them as the ``removed_indices`` of the same
    ``CreateIndex`` operation that publishes the rebuilt index, so the old index stays live and
    searchable for the whole (potentially hours-long) build phase and the replacement is one
    atomic commit with no window where the dataset carries no FTS index. Each commit attempt
    validates that every covered fragment still exists at the latest version: a concurrent
    compaction can rewrite covered fragments between build and commit, and a blind retry at the
    new head would publish an index whose row addresses point at compacted-away fragments.

    Args:
        uri: Dataset URI.
        column: The indexed text column.
        index_name: The index name to publish under.
        index_uuid: The shared index id the fragment builds used.
        fragment_ids: The fragments the index covers.
        config: Indexing configuration.
        telemetry: Telemetry facade for the current process.

    Raises:
        ValueError: If covered fragments no longer exist because a compaction rewrote them.
        OSError | RuntimeError: If commits keep conflicting past the retry budget.
    """
    dataset: lance.LanceDataset = lance.dataset(uri, storage_options=config.storage_options)
    with telemetry.timed("index.merge_ms", tags=[f"index:{index_name}"]):
        dataset.merge_index_metadata(index_uuid, index_type="INVERTED")
    publish_fts_index(uri, column, index_name, index_uuid, fragment_ids, config, telemetry)


def same_name_index_segments(dataset: lance.LanceDataset, index_name: str) -> list[Index]:
    """Collect the committed segment metadata of every index carrying a name.

    Builds the ``Index`` records the ``CreateIndex`` operation's ``removed_indices`` field
    expects (removal matches on segment uuid), one per committed segment because an
    incrementally maintained index carries one segment per delta. This lets a rebuilt inverted
    index atomically replace its predecessor inside the very commit that publishes it, instead
    of dropping the old index in a separate earlier commit.

    Args:
        dataset: The dataset whose committed indexes to inspect.
        index_name: The index name whose segments to collect.

    Returns:
        One ``Index`` record per committed segment of the named index, empty when the name is
        absent.
    """
    return [
        Index(
            uuid=segment.uuid,
            name=description.name,
            fields=list(description.fields),
            dataset_version=segment.dataset_version_at_last_update,
            fragment_ids=set(segment.fragment_ids),
            index_version=segment.index_version,
            created_at=segment.created_at,
            base_id=segment.base_id,
        )
        for description in dataset.describe_indices()
        if description.name == index_name
        for segment in description.segments
    ]


def publish_fts_index(
    uri: str,
    column: str,
    index_name: str,
    index_uuid: str,
    fragment_ids: list[int],
    config: IndexJobConfig,
    telemetry: Telemetry,
) -> None:
    """Publish an already-merged inverted index, retrying conflicts and refusing stale coverage.

    Each attempt validates that every covered fragment still exists at the latest version before
    committing the ``CreateIndex`` operation. The commit atomically replaces any existing
    same-name index: its committed segments are re-listed at the latest version on every attempt
    (:func:`same_name_index_segments`) and passed as the operation's ``removed_indices``, so the
    drop and the publish are one commit and a publish failure leaves the old index fully live.

    Args:
        uri: Dataset URI.
        column: The indexed text column.
        index_name: The index name to publish under.
        index_uuid: The shared index id the fragment builds used.
        fragment_ids: The fragments the index covers.
        config: Indexing configuration.
        telemetry: Telemetry facade for the current process.

    Raises:
        ValueError: If covered fragments no longer exist because a compaction rewrote them.
        OSError | RuntimeError: If commits keep conflicting past the retry budget.
    """
    dataset: lance.LanceDataset = lance.dataset(uri, storage_options=config.storage_options)
    field_id: int = lance_field_id(dataset, column)
    fragments: set[int] = set(fragment_ids)
    storage_options: dict[str, Any] | None = config.storage_options
    tags: list[str] = ["index_type:INVERTED"]

    def action() -> None:
        """Atomically swap the merged inverted index in at the latest version."""
        current: lance.LanceDataset = lance.dataset(uri, storage_options=storage_options)
        live: set[int] = live_fragment_ids(current)
        missing: set[int] = fragments - live
        if missing:
            raise ValueError(
                f"inverted index {index_name} on {uri} covers fragments {sorted(missing)} that no longer exist; "
                "a compaction rewrote them between build and commit, so this build must be redone"
            )
        removed: list[Index] = same_name_index_segments(current, index_name)
        index: Index = Index(
            uuid=index_uuid,
            name=index_name,
            fields=[field_id],
            dataset_version=current.version,
            fragment_ids=fragments,
            index_version=0,
        )
        operation: Any = lance.LanceOperation.CreateIndex(new_indices=[index], removed_indices=removed)
        lance.LanceDataset.commit(uri, operation, read_version=current.version, storage_options=storage_options)
        telemetry.incr("index.committed", tags=tags)

    with telemetry.timed("index.commit_ms", tags=[f"index:{index_name}"]):
        commit_index_with_retries(action, config, telemetry, tags)
