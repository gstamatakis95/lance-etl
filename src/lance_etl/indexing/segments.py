"""Segment serialisation, IVF centroid I/O, and the build-and-commit loop.

Provides all the primitives needed by the distributed index build: serialise/deserialise
uncommitted segment metadata, IPC-encode/decode IVF centroids, shard fragment lists, validate
live fragments, commit collected segments with conflict retries, and the outer
build-and-commit-segments loop that handles stale-fragment replans.
"""

from __future__ import annotations

import base64
import json
import logging
import threading
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

TRAIN_SEMAPHORE: threading.Semaphore = threading.Semaphore(1)
"""Process-level semaphore that serialises concurrent IVF trainings from the driver thread pool."""


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
    """Commit built segments, retrying conflicts to coexist with writers.

    Each attempt validates the segments against the latest fragment set first. A concurrent
    compaction can rewrite fragments between the segment build and this commit, and a blind retry
    at the new head version would then publish segments pointing at fragments that no longer exist,
    silently corrupting search results. Stale segments are dropped with a metric instead. When
    every segment is stale the commit is skipped and ``0`` is returned. When a surviving segment
    overlaps a wider existing segment that a compaction remapped over a rewritten fragment, lance
    raises the ``"would orphan fragments"`` ``ValueError``, which propagates so the caller can
    re-resolve and rebuild.

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
        ValueError: If publishing the fresh segments would orphan fragments held by a wider
            existing segment, so the caller must re-resolve the fragment set and rebuild.
        OSError | RuntimeError: If commits keep conflicting past the retry budget.
    """
    segments: list[Index] = [deserialize_segment(document) for document in segment_documents]
    tags: list[str] = [f"index:{index_name}"]

    def action() -> int:
        """Drop stale segments, merge if needed, and commit at the latest version."""
        dataset: lance.LanceDataset = lance.dataset(uri, storage_options=config.storage_options)
        live: set[int] = live_fragment_ids(dataset)
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
            merged = dataset.merge_existing_index_segments(fresh)
            dataset.commit_existing_index_segments(index_name, column, [merged])
        else:
            dataset.commit_existing_index_segments(index_name, column, fresh)
        telemetry.incr("index.committed", tags=tags)
        return len(fresh)

    return commit_index_with_retries(action, config, telemetry, tags)


def build_and_commit_segments(
    uri: str,
    handler: Any,
    config: IndexJobConfig,
    telemetry: Telemetry,
    build_documents: Callable[[list[list[int]], int, object | None], list[str]],
) -> dict[str, int]:
    """Build per-shard segments and commit them, rebuilding when a concurrent compaction makes the plan stale.

    The segment build plans over a fragment set resolved at one version. A concurrent compaction
    can rewrite some of those fragments and remap an existing wider index segment over them between
    the build and the commit, so publishing the freshly built segments would either orphan fragments
    the existing segment still holds (:func:`commit_segments` re-raises the ``"would orphan
    fragments"`` ``ValueError``) or cover fragments that no longer exist (:func:`commit_segments`
    drops them). Both mean the fragment set is stale. This mirrors the compactor's
    re-plan-on-conflict loop: it re-reads the dataset at the latest version, re-resolves the target
    fragments (dropping fragments that no longer exist), rebuilds the affected segments, and
    re-commits, bounded by ``config.commit_retries``. Every live target therefore ends up covered
    rather than silently skipped. The full-rebuild replan loop is bounded by
    ``config.max_stale_replans`` (not by ``config.commit_retries``): each replan rebuilds every
    remaining target fragment from scratch which can be terabytes of I/O for a whale org, so it is
    bounded tightly and independently from the cheap commit-conflict budget. If the budget is
    exhausted while a writer keeps rewriting, the remaining fragments are left for the next
    scheduled run, which re-covers them once the contention clears.

    Args:
        uri: Dataset URI.
        handler: The per-type index handler resolving targets.
        config: Indexing configuration.
        telemetry: Telemetry facade for the current process.
        build_documents: Builds one serialized segment per shard for the given fragment groups,
            pinned to the given dataset version, using the broadcast artifacts. Supplied by the
            distributed and in-process callers so the rebuild loop is shared.

    Returns:
        A mapping with the total ``segments`` committed and the ``fragments`` targeted on the first
        attempt.
    """
    tags: list[str] = [f"index:{handler.index_name}"]
    total_segments: int = 0
    first_targets: int = 0
    for attempt in range(config.max_stale_replans):
        dataset: lance.LanceDataset = lance.dataset(uri, storage_options=config.storage_options)
        targets: list[int] = handler.target_fragments(dataset)
        if attempt == 0:
            first_targets = len(targets)
        if not targets:
            return {"segments": total_segments, "fragments": first_targets}
        artifacts: object | None = handler.prepare(dataset, uri, telemetry)
        version: int = dataset.version
        groups: list[list[int]] = split_evenly(targets, config.num_shards)
        documents: list[str] = build_documents(groups, version, artifacts)
        try:
            committed: int = commit_segments(
                uri, documents, handler.column, handler.index_name, handler.merges(), config, telemetry
            )
        except ValueError as exc:
            if not is_stale_fragment_error(exc):
                raise
            telemetry.incr("index.stale_fragment_replan", tags=tags)
            logger.warning(
                "rebuilding %s on %s: a concurrent compaction orphaned the planned fragment set (%s)",
                handler.index_name,
                uri,
                exc,
            )
            continue
        if committed:
            total_segments += committed
        if committed == len(documents):
            return {"segments": total_segments, "fragments": first_targets}
        telemetry.incr("index.stale_fragment_replan", tags=tags)
    logger.warning(
        "index %s on %s still has uncovered fragments after %d stale-replan attempts; next scheduled run re-covers",
        handler.index_name,
        uri,
        config.max_stale_replans,
    )
    return {"segments": total_segments, "fragments": first_targets}
