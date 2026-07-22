"""Bounded parent-linked Iceberg snapshot ancestry traversal."""

from __future__ import annotations

from collections.abc import Iterable

from lance_etl.source.errors import SourceLineageError
from lance_etl.source.models import SnapshotRecord


def index_snapshots(snapshots: Iterable[SnapshotRecord]) -> dict[int, SnapshotRecord]:
    """Index a bounded catalog result while rejecting conflicting duplicate records.

    Args:
        snapshots: Snapshots returned for a pinned head through a requested stopping point.

    Returns:
        Mapping from snapshot id to its unique immutable metadata.

    Raises:
        SourceLineageError: If one id has inconsistent metadata.
    """
    indexed: dict[int, SnapshotRecord] = {}
    for snapshot in snapshots:
        existing = indexed.get(snapshot.snapshot_id)
        if existing is not None and existing != snapshot:
            raise SourceLineageError(f"conflicting metadata for snapshot {snapshot.snapshot_id}")
        indexed[snapshot.snapshot_id] = snapshot
    return indexed


def walk_snapshot_lineage(
    snapshots: Iterable[SnapshotRecord],
    pinned_head_snapshot_id: int,
    stop_snapshot_id: int,
    expected_table_uuid: str,
    expected_partition_spec_id: int,
) -> tuple[SnapshotRecord, ...]:
    """Walk from a pinned head to an exclusive recorded ancestor and return lineage order.

    Commit timestamps and snapshot identifiers never determine ordering. The direct parent chain
    is authoritative and sequence numbers must increase strictly along that chain.

    Args:
        snapshots: Bounded snapshot metadata containing the requested ancestry.
        pinned_head_snapshot_id: Inclusive head captured at planner start.
        stop_snapshot_id: Exclusive recorded ancestor that must be on the head's lineage.
        expected_table_uuid: Stable table identity.
        expected_partition_spec_id: Stable active partition specification id.

    Returns:
        Descendant snapshots ordered from oldest to newest.

    Raises:
        SourceLineageError: If ancestry is missing, cyclic, forked, or non-monotonic.
    """
    indexed = index_snapshots(snapshots)
    stop = indexed.get(stop_snapshot_id)
    if stop is None:
        raise SourceLineageError(f"recorded ancestor snapshot {stop_snapshot_id} is missing from bounded metadata")
    validate_snapshot_identity(stop, expected_table_uuid, expected_partition_spec_id)
    if pinned_head_snapshot_id == stop_snapshot_id:
        return ()
    reverse_path: list[SnapshotRecord] = []
    visited: set[int] = set()
    cursor = pinned_head_snapshot_id
    while cursor != stop_snapshot_id:
        if cursor in visited:
            raise SourceLineageError(f"cycle detected at snapshot {cursor}")
        visited.add(cursor)
        snapshot = indexed.get(cursor)
        if snapshot is None:
            raise SourceLineageError(f"missing snapshot {cursor} before recorded ancestor {stop_snapshot_id}")
        validate_snapshot_identity(snapshot, expected_table_uuid, expected_partition_spec_id)
        reverse_path.append(snapshot)
        if snapshot.parent_snapshot_id is None:
            raise SourceLineageError(f"recorded snapshot {stop_snapshot_id} is not an ancestor of pinned head")
        cursor = snapshot.parent_snapshot_id
    ordered = tuple(reversed(reverse_path))
    previous_sequence: int | None = None
    for snapshot in ordered:
        if previous_sequence is not None and snapshot.sequence_number <= previous_sequence:
            raise SourceLineageError("Iceberg sequence numbers are not strictly increasing along snapshot ancestry")
        previous_sequence = snapshot.sequence_number
    if ordered and ordered[0].sequence_number <= stop.sequence_number:
        raise SourceLineageError("first descendant sequence number does not follow the recorded ancestor")
    return ordered


def validate_snapshot_identity(snapshot: SnapshotRecord, table_uuid: str, partition_spec_id: int) -> None:
    """Validate stable identity fields on one snapshot.

    Args:
        snapshot: Snapshot to validate.
        table_uuid: Expected table UUID.
        partition_spec_id: Expected active partition specification id.

    Raises:
        SourceLineageError: If identity or spec changed.
    """
    if snapshot.table_uuid != table_uuid:
        raise SourceLineageError(f"table UUID changed at snapshot {snapshot.snapshot_id}")
    if snapshot.partition_spec_id != partition_spec_id:
        raise SourceLineageError(f"partition specification changed at snapshot {snapshot.snapshot_id}")
