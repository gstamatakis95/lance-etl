"""Segment serialisation, IVF centroid I/O, executor-offloaded training, and the build-and-commit loop.

Provides all the primitives needed by the distributed index build: serialise/deserialise
uncommitted segment metadata, IPC-encode/decode IVF centroids, shard fragment lists, validate
live fragments, and commit collected segments with conflict retries. IVF centroid training
happens inside the streaming bootstrap build (see :mod:`lance_etl.indexing.runner`), and the
stale-fragment replan loop lives there as fleet-level rounds.
"""

from __future__ import annotations

import base64
import json
import logging
from collections.abc import Callable
from typing import Any

import lance
import pyarrow as pa
from lance.dataset import Index

from lance_etl.indexing.config import IndexJobConfig
from lance_etl.telemetry import Telemetry, commit_with_retries

logger: logging.Logger = logging.getLogger(__name__)

STALE_FRAGMENT_MARKERS: tuple[str, ...] = ("would orphan fragments", "no longer exist")
"""Error-message substrings that identify a segment commit invalidated by a concurrent compaction."""


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


def all_fragment_ids(dataset: lance.LanceDataset) -> list[int]:
    """Return every fragment id of a dataset in fragment order.

    Args:
        dataset: The dataset to inspect.

    Returns:
        The fragment ids in the order ``get_fragments`` reports them.
    """
    return [fragment.fragment_id for fragment in dataset.get_fragments()]


def live_fragment_ids(dataset: lance.LanceDataset) -> set[int]:
    """Return the set of fragment ids currently live in a dataset.

    Args:
        dataset: The dataset to inspect.

    Returns:
        The live fragment ids as a set.
    """
    return set(all_fragment_ids(dataset))


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


def lance_field_id(dataset: lance.LanceDataset, column: str) -> int:
    """Return the Lance field id for a top-level column.

    Uses the internal Lance schema rather than the Arrow positional index so the field id remains
    stable across schema evolution. ``dataset._ds`` is the only way to access the Lance schema
    from Python. Access is wrapped here to contain the private-attribute usage.

    Args:
        dataset: The dataset to inspect.
        column: The column name to look up.

    Returns:
        The Lance field id for the column.

    Raises:
        ValueError: If the column is not present in the Lance schema.
    """
    lance_field = dataset._ds.lance_schema.field_case_insensitive(column)
    if lance_field is None:
        raise ValueError(f"column {column!r} not found in Lance schema")
    return lance_field.id()


def commit_index_with_retries(
    action: Callable[[], Any],
    config: IndexJobConfig,
    telemetry: Telemetry,
    tags: list[str],
) -> Any:
    """Run an index commit action, retrying commit conflicts with the configured budget.

    Centralizes the retry budget, backoff, and the shared ``index.commit_conflict`` conflict metric
    used by every index commit path so each call site states only its action and its metric tags.

    Args:
        action: The commit to attempt, returning any result. It must re-read the dataset so each
            retry rebases.
        config: Indexing configuration supplying the retry budget and backoff.
        telemetry: Telemetry facade for the current process.
        tags: Metric tags applied to the conflict counter.

    Returns:
        Whatever ``action`` returns on its first non-conflicting attempt.

    Raises:
        OSError | RuntimeError: If commits keep conflicting past the retry budget.
    """
    return commit_with_retries(
        action,
        config.commit_retries,
        config.commit_backoff_seconds,
        lambda: telemetry.incr("index.commit_conflict", tags=tags),
    )


def build_scalar_segment(
    dataset: lance.LanceDataset,
    fragment_ids: list[int],
    artifacts: object | None,
    column: str,
    index_name: str,
    index_type: str,
) -> Index:
    """Build one artifact-free scalar segment (BTREE or BITMAP) over a shard of fragments.

    This is a module-level function so the Spark closure can capture it through a small
    :func:`functools.partial` of primitive values rather than a bound method, which would pickle
    the entire handler instance onto every task. ``index_uuid`` must not be passed for these
    segment builds. Lance mints segment ids itself. ``replace=True`` bypasses the same-name
    existence guard on the uncommitted path so incremental segments can extend an existing index.
    It removes nothing, since delta removal happens only on the committed path.

    Args:
        dataset: A dataset handle pinned to the build version.
        fragment_ids: The fragment ids for this shard.
        artifacts: Unused by scalar builds. Present so the callable signature matches the vector
            builder.
        column: The column to index.
        index_name: The name of the index.
        index_type: The scalar index type (``BTREE`` or ``BITMAP``).

    Returns:
        The uncommitted segment metadata.
    """
    del artifacts
    return dataset.create_index_uncommitted(
        column=column,
        index_type=index_type,
        name=index_name,
        replace=True,
        fragment_ids=fragment_ids,
    )


def build_vector_segment(
    dataset: lance.LanceDataset,
    fragment_ids: list[int],
    artifacts: object | None,
    column: str,
    index_name: str,
    metric: str,
) -> Index:
    """Build one IVF_RQ segment over a shard of fragments.

    Module-level for the same closure-pickling reason as :func:`build_scalar_segment`.
    ``replace=True`` is required for incremental coverage: the uncommitted build path applies the
    same-name existence guard, and without it a segment build for an index that already exists
    raises. The flag only bypasses that guard. Removal of prior deltas happens solely on the
    committed ``execute`` path, never here, so existing coverage is preserved and
    :func:`commit_segments` publishes the new segments as a delta.

    Args:
        dataset: A dataset handle pinned to the build version.
        fragment_ids: The fragment ids for this shard.
        artifacts: The centroids bytes, the shared RaBitQ model string, num_bits, and the IVF
            partition count.
        column: The column to index.
        index_name: The name of the index.
        metric: The distance metric for the vector index.

    Returns:
        The uncommitted segment metadata.

    Raises:
        ValueError: If ``artifacts`` is ``None``. Vector segment builds require the artifact tuple
            produced by :meth:`VectorIndexHandler.prepare`.
    """
    if artifacts is None:
        raise ValueError("build_vector_segment requires artifacts from prepare; got None")
    centroids_bytes, rabitq_model, num_bits, num_partitions = artifacts
    centroids: pa.Array = centroids_from_ipc(centroids_bytes)
    return dataset.create_index_uncommitted(
        column=column,
        index_type="IVF_RQ",
        name=index_name,
        metric=metric,
        replace=True,
        num_partitions=num_partitions,
        num_bits=num_bits,
        ivf_centroids=centroids,
        rabitq_model=rabitq_model,
        fragment_ids=fragment_ids,
    )


def is_stale_fragment_error(exc: BaseException) -> bool:
    """Report whether an exception marks a segment commit invalidated by a concurrent compaction.

    Shared by the vector, scalar, and inverted-index paths so the stale-fragment guard is detected
    in one place rather than reimplemented per index type.

    Args:
        exc: The exception to inspect.

    Returns:
        ``True`` when the exception means the planned fragment set is stale and the build must be
        redone.
    """
    if not isinstance(exc, ValueError):
        return False
    message: str = str(exc)
    return any(marker in message for marker in STALE_FRAGMENT_MARKERS)


def commit_segments(
    uri: str,
    segment_documents: list[str],
    column: str,
    index_name: str,
    merge: bool,
    config: IndexJobConfig,
    telemetry: Telemetry,
) -> int:
    """Commit built segments, merging once and retrying only the cheap commit on conflicts.

    The expensive work runs exactly once, outside the retry loop: stale segments (those whose
    fragments were rewritten between build and commit) are dropped with a metric, and the fresh
    survivors are merged when the index type requires it. For a large dataset that merge streams
    the segment index files through the committing executor (this function's only caller is the
    per-index commit fan-out), so re-running it on every benign commit conflict would amplify a
    manifest race into repeated whale-scale work. The retried action is therefore
    only open, validate, and commit: each attempt re-opens the dataset at the latest version,
    verifies every prepared segment still covers only live fragments, and commits. When a
    concurrent rewrite invalidates the prepared segments mid-retry, the action raises a
    ``ValueError`` carrying the ``"no longer exist"`` stale-fragment marker so the caller's
    replan loop re-resolves and rebuilds, the same recovery path as the lance
    ``"would orphan fragments"`` error.

    When every built segment is stale before the merge, the commit is skipped and ``0`` is
    returned.

    Args:
        uri: Dataset URI.
        segment_documents: Serialized segments returned by the executors.
        column: The indexed column.
        index_name: The index name to publish under.
        merge: Whether to merge segments before committing, used for IVF_RQ and BITMAP.
        config: Indexing configuration.
        telemetry: Driver telemetry facade.

    Returns:
        The number of fresh (non-stale) segments that were committed. ``0`` when every segment was
        stale and the commit was skipped.

    Raises:
        ValueError: If the prepared segments went stale during commit retries, or if publishing
            them would orphan fragments held by a wider existing segment. Either way the caller
            must re-resolve the fragment set and rebuild.
        OSError | RuntimeError: If commits keep conflicting past the retry budget.
    """
    tags: list[str] = [f"index:{index_name}"]
    dataset: lance.LanceDataset = lance.dataset(uri, storage_options=config.storage_options)
    live: set[int] = live_fragment_ids(dataset)
    segments: list[Index] = [deserialize_segment(document) for document in segment_documents]
    fresh: list[Index] = [segment for segment in segments if set(segment.fragment_ids) <= live]
    stale: int = len(segments) - len(fresh)
    if stale:
        telemetry.incr("index.stale_segments_dropped", value=stale, tags=tags)
        logger.warning(
            "dropping %d stale segments for %s on %s: their fragments were rewritten between build and commit",
            stale,
            index_name,
            uri,
        )
    if not fresh:
        logger.warning("every segment for %s on %s is stale; skipping commit, next run re-covers", index_name, uri)
        return 0
    if merge and len(fresh) > 1:
        to_commit: list[Index] = [dataset.merge_existing_index_segments(fresh)]
    else:
        to_commit = fresh

    def action() -> int:
        """Re-open at the latest version, verify the prepared segments are live, and commit."""
        latest: lance.LanceDataset = lance.dataset(uri, storage_options=config.storage_options)
        current: set[int] = live_fragment_ids(latest)
        for segment in to_commit:
            if not set(segment.fragment_ids) <= current:
                raise ValueError(
                    f"prepared segments for {index_name} cover fragments that no longer exist after a "
                    f"concurrent rewrite; the merge is stale and the build must re-resolve"
                )
        latest.commit_existing_index_segments(index_name, column, to_commit)
        telemetry.incr("index.committed", tags=tags)
        return len(fresh)

    return commit_index_with_retries(action, config, telemetry, tags)
