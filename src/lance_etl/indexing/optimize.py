"""Incremental index maintenance helpers.

Provides load/write for the vector artifact config, the object-store centroid sidecar cache
(save/load keyed by the ``rows_at_train`` fingerprint), optimize-in-place for one existing index,
delta-count query, delta merge, old-index drop, and the combined maintain-locally helper used
by the FTS incremental path.
"""

from __future__ import annotations

import json
import logging
from typing import Any

import lance
import pyarrow as pa
from lance.indices import IvfModel

from lance_etl.indexing.config import IndexJobConfig, vector_config_key
from lance_etl.indexing.segments import commit_index_with_retries
from lance_etl.telemetry import Telemetry

logger: logging.Logger = logging.getLogger(__name__)


def load_vector_config(dataset: lance.LanceDataset, column: str) -> dict[str, Any] | None:
    """Read the vector artifact config for a column from the dataset's own config KV.

    The config KV is already in-memory from the open manifest, so this call does not perform any
    additional object-store I/O. Returns ``None`` when the key is absent or the stored value
    cannot be parsed as JSON, logging a warning in the latter case.

    Args:
        dataset: The open dataset whose config to read.
        column: The vector column whose config to return.

    Returns:
        The parsed config dictionary, or ``None`` on absent or malformed values.
    """
    key: str = vector_config_key(column)
    raw: str | None = dataset.config().get(key)
    if raw is None:
        return None
    try:
        return json.loads(raw)
    except (json.JSONDecodeError, ValueError):
        logger.warning("malformed vector config for column %r on %s; treating as absent", column, dataset.uri)
        return None


def write_vector_config(
    uri: str,
    column: str,
    data: dict[str, Any],
    config: IndexJobConfig,
    telemetry: Telemetry,
) -> None:
    """Write the vector artifact config for a column into the dataset's own config KV.

    Uses ``dataset.update_config`` wrapped in :func:`~lance_etl.indexing.segments.commit_index_with_retries`.
    The pyo3 binding surfaces conflicts as ``OSError``, so each retry re-opens the dataset at the
    latest version before calling ``update_config``. The JSON value is stored under
    ``lance-etl.vector.{column}``. Because same-key ``update_config`` writes are incompatible
    conflicts (not retried by lance itself), two concurrent index runs on the same dataset that
    both reach the train branch would conflict here. Index runs are serialized per dataset
    upstream, so this is an operational constraint rather than a hot path.

    Args:
        uri: Dataset URI.
        column: The vector column whose config to write.
        data: The config dict to serialise as the key value.
        config: Indexing configuration for the retry budget and backoff.
        telemetry: Driver telemetry facade.

    Raises:
        OSError | RuntimeError: If commits keep conflicting past the retry budget.
    """
    key: str = vector_config_key(column)
    payload: str = json.dumps(data)
    tags: list[str] = [f"column:{column}"]

    def action() -> None:
        """Write the config key at the latest dataset version."""
        dataset: lance.LanceDataset = lance.dataset(uri, storage_options=config.storage_options)
        dataset.update_config({key: payload})

    commit_index_with_retries(action, config, telemetry, tags)


def centroid_sidecar_uri(uri: str, index_name: str, rows_at_train: int) -> str:
    """Build the object-store URI of the centroid sidecar for one vector-index generation.

    The sidecar lives in a ``{uri}.artifacts`` directory that is a sibling of the ``.lance``
    dataset directory, so dataset discovery skips it: its final path component ends in
    ``.artifacts`` rather than ``.lance`` (see
    :func:`lance_etl.cloud_storage.dataset_paths_under`). The ``rows_at_train`` fingerprint in the
    filename makes the whole path change whenever the centroids are retrained, so a stale sidecar
    can never be mistaken for a current one and reuse or invalidation is automatic (ADR 0040).

    Args:
        uri: Dataset URI, ending in ``.lance``.
        index_name: The vector index name the centroids belong to.
        rows_at_train: The row count recorded when the centroids were trained, the staleness
            fingerprint that keys the sidecar generation.

    Returns:
        The sidecar URI ``{uri}.artifacts/{index_name}.{rows_at_train}.ivf``.
    """
    base: str = uri.rstrip("/")
    return f"{base}.artifacts/{index_name}.{rows_at_train}.ivf"


def save_centroids(
    uri: str,
    index_name: str,
    centroids: pa.Array,
    metric: str,
    rows_at_train: int,
    storage_options: dict[str, Any] | None,
) -> None:
    """Persist IVF centroids to an object-store sidecar for future-run reuse.

    Writes the centroids through lance's native :class:`lance.indices.IvfModel` single-file
    format, which threads ``storage_options`` through lance's own object-store layer (the same
    credential path as :func:`lance.dataset`). The ``distance_type`` is set from ``metric`` because
    :meth:`IvfModel.save` requires a non-``None`` string. It is unused on the reuse read path,
    which passes the metric to ``create_index_uncommitted`` separately.

    Args:
        uri: Dataset URI.
        index_name: The vector index name the centroids belong to.
        centroids: The IVF centroid array from ``get_ivf_model(index_name).centroids``.
        metric: The distance metric, stored as the model's ``distance_type``.
        rows_at_train: The staleness fingerprint keying the sidecar generation.
        storage_options: Object-store options forwarded to lance.
    """
    sidecar: str = centroid_sidecar_uri(uri, index_name, rows_at_train)
    IvfModel(centroids, distance_type=metric).save(sidecar, storage_options=storage_options)


def load_centroids(
    uri: str,
    index_name: str,
    rows_at_train: int,
    storage_options: dict[str, Any] | None,
) -> pa.Array | None:
    """Read the IVF centroid sidecar for one vector-index generation, or ``None`` on a miss.

    This is the conditional-reuse gate. A returned array means the sidecar for the exact
    ``rows_at_train`` fingerprint exists and was read, so the current centroids can be reused
    without re-reading the committed index. Any failure — an absent sidecar, a partial write, or a
    version-incompatible file — returns ``None`` so the caller falls back to ``get_ivf_model``,
    which keeps correctness independent of sidecar liveness.

    Args:
        uri: Dataset URI.
        index_name: The vector index name the centroids belong to.
        rows_at_train: The staleness fingerprint keying the sidecar generation.
        storage_options: Object-store options forwarded to lance.

    Returns:
        The centroid array on a fingerprint hit, or ``None`` when the sidecar is absent or
        unreadable.
    """
    sidecar: str = centroid_sidecar_uri(uri, index_name, rows_at_train)
    try:
        return IvfModel.load(sidecar, storage_options=storage_options).centroids
    except Exception as exc:
        logger.debug("centroid sidecar miss for %s on %s: %s", index_name, uri, exc)
        return None


def optimize_existing_index(
    uri: str,
    index_name: str,
    config: IndexJobConfig,
    telemetry: Telemetry,
    num_indices_to_merge: int | None = None,
) -> None:
    """Run incremental maintenance for one existing index, retrying conflicts.

    Appends unindexed fragments to the existing index without retraining and no-ops cheaply when
    the index already covers everything. Runs in the calling process, so call it from an executor
    task. Each retry re-opens the dataset at the latest version, which makes the retry productive
    against concurrent ingestion and compaction.

    Args:
        uri: Dataset URI.
        index_name: The existing index to maintain.
        config: Indexing configuration.
        telemetry: Telemetry facade for the current process.
        num_indices_to_merge: When set, also merge the delta with this many existing indices.

    Raises:
        OSError | RuntimeError: If commits keep conflicting past the retry budget.
    """
    tags: list[str] = [f"index:{index_name}"]

    def action() -> None:
        """Optimize the index against the latest dataset version."""
        dataset: lance.LanceDataset = lance.dataset(uri, storage_options=config.storage_options)
        kwargs: dict[str, Any] = {"index_names": [index_name]}
        if num_indices_to_merge is not None:
            kwargs["num_indices_to_merge"] = num_indices_to_merge
        dataset.optimize.optimize_indices(**kwargs)
        telemetry.incr("index.optimized", tags=tags)

    commit_index_with_retries(action, config, telemetry, tags)


def index_delta_count(dataset: lance.LanceDataset, index_name: str) -> int:
    """Return how many deltas (per-name index metadata entries) an index has.

    Args:
        dataset: The dataset to inspect.
        index_name: The index name.

    Returns:
        The ``num_indices`` value from the index statistics.
    """
    stats: dict[str, Any] = dataset.stats.index_stats(index_name)
    return int(stats.get("num_indices") or 0)


def merge_index_deltas(uri: str, index_name: str, config: IndexJobConfig, telemetry: Telemetry) -> bool:
    """Merge an index's accumulated deltas into one when over the configured cap.

    Incremental runs add one delta per run per index, and every query consults all of them. The
    merge rewrites the deltas against current row addresses, which also permanently retires
    deferred frag-reuse remap debt. Runs in the calling process, so call it from an executor task,
    and only for an index that exists.

    Args:
        uri: Dataset URI.
        index_name: The existing index whose deltas to bound.
        config: Indexing configuration.
        telemetry: Telemetry facade for the current process.

    Returns:
        ``True`` if a merge ran, ``False`` when the delta count was within the cap.
    """
    dataset: lance.LanceDataset = lance.dataset(uri, storage_options=config.storage_options)
    deltas: int = index_delta_count(dataset, index_name)
    if deltas <= config.max_index_deltas:
        return False
    optimize_existing_index(uri, index_name, config, telemetry, num_indices_to_merge=deltas)
    telemetry.incr("index.deltas_merged", tags=[f"index:{index_name}"])
    logger.info("merged %d index deltas into one for %s on %s", deltas, index_name, uri)
    return True


def maintain_index_locally(uri: str, index_name: str, config: IndexJobConfig, telemetry: Telemetry) -> bool:
    """Maintain one existing index in process, then bound its delta count.

    Args:
        uri: Dataset URI.
        index_name: The existing index to maintain.
        config: Indexing configuration.
        telemetry: Telemetry facade for the current process.

    Returns:
        ``True`` if a delta merge ran after the maintenance pass.
    """
    optimize_existing_index(uri, index_name, config, telemetry)
    return merge_index_deltas(uri, index_name, config, telemetry)
