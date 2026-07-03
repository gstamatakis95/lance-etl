"""Incremental index maintenance helpers.

Provides load/write for the vector artifact config, optimize-in-place for one existing index,
delta-count query, delta merge, old-index drop, and the combined maintain-locally helper used
by the FTS incremental path.
"""

from __future__ import annotations

import json
import logging
from typing import Any

import lance

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


def drop_existing_index(uri: str, index_name: str, config: IndexJobConfig, telemetry: Telemetry) -> None:
    """Drop a named index, retrying conflicts.

    Used by :class:`~lance_etl.indexing.handlers.FtsIndexHandler` to remove the old committed
    inverted index just before the metadata merge and publish commit, minimising the availability
    gap. Runs in the calling process.

    Args:
        uri: Dataset URI.
        index_name: The index to drop.
        config: Indexing configuration.
        telemetry: Telemetry facade for the current process.

    Raises:
        OSError | RuntimeError: If commits keep conflicting past the retry budget.
    """
    tags: list[str] = [f"index:{index_name}"]

    def action() -> None:
        """Drop the index at the latest version when it still exists."""
        dataset: lance.LanceDataset = lance.dataset(uri, storage_options=config.storage_options)
        if index_name in {description.name for description in dataset.describe_indices()}:
            dataset.drop_index(index_name)
            telemetry.incr("index.dropped", tags=tags)

    commit_index_with_retries(action, config, telemetry, tags)


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
