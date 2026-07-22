"""Snapshot classification and manifest-derived target discovery."""

from __future__ import annotations

from collections.abc import Iterable

from lance_etl.source.errors import blocked_snapshot_error
from lance_etl.source.models import (
    MaintenanceTrust,
    ManifestContent,
    ManifestEntry,
    ManifestStatus,
    SnapshotRecord,
    TargetKey,
    WindowKind,
)

APPEND_OPERATIONS: frozenset[str] = frozenset({"append", "fast-append"})
REWRITE_OPERATIONS: frozenset[str] = frozenset({"replace", "overwrite"})


def classify_snapshot(
    snapshot: SnapshotRecord,
    entries: Iterable[ManifestEntry],
    trust: MaintenanceTrust | None,
) -> WindowKind:
    """Classify one snapshot while failing closed on physical or untrusted changes.

    Args:
        snapshot: Snapshot metadata.
        entries: Manifest entries associated with the snapshot.
        trust: Catalog-authenticated maintenance evidence, when applicable.

    Returns:
        Accepted window kind.

    Raises:
        SourceSnapshotBlockedError: If the operation cannot be replayed safely.
    """
    operation = snapshot.operation.strip().lower()
    materialized = tuple(entries)
    if operation in APPEND_OPERATIONS:
        validate_append_entries(snapshot, materialized)
        return WindowKind.APPEND
    if operation in REWRITE_OPERATIONS:
        validate_trusted_rewrite(snapshot, materialized, trust)
        return WindowKind.TRUSTED_MAINTENANCE
    if operation in {"delete", "row_delta"}:
        raise blocked_snapshot_error(snapshot.snapshot_id, "PHYSICAL_DELETE", "physical delete snapshot is blocked")
    raise blocked_snapshot_error(snapshot.snapshot_id, "UNKNOWN_OPERATION", f"unsupported operation {operation!r}")


def validate_append_entries(snapshot: SnapshotRecord, entries: tuple[ManifestEntry, ...]) -> None:
    """Require an append snapshot to contain only added data files.

    Args:
        snapshot: Append snapshot being classified.
        entries: Its normalized manifest entries.

    Raises:
        SourceSnapshotBlockedError: If the snapshot removes data or adds delete files.
    """
    summary: dict[str, str] = dict(snapshot.summary)
    try:
        declared_nonempty: bool = any(int(summary.get(name, "0")) > 0 for name in ("added-data-files", "added-records"))
    except ValueError as exc:
        raise blocked_snapshot_error(
            snapshot.snapshot_id,
            "MANIFEST_SUMMARY_INVALID",
            "append snapshot carries a malformed added-data summary",
        ) from exc
    has_added_data: bool = any(
        entry.status is ManifestStatus.ADDED and entry.content is ManifestContent.DATA for entry in entries
    )
    if declared_nonempty and not has_added_data:
        raise blocked_snapshot_error(
            snapshot.snapshot_id,
            "MANIFEST_EVIDENCE_MISSING",
            "nonempty append snapshot has no added data-file manifest evidence",
        )
    for entry in entries:
        if entry.snapshot_id != snapshot.snapshot_id and entry.status is not ManifestStatus.EXISTING:
            raise blocked_snapshot_error(
                snapshot.snapshot_id, "MANIFEST_SNAPSHOT_MISMATCH", "changed manifest entry belongs to another snapshot"
            )
        if entry.status is ManifestStatus.DELETED:
            raise blocked_snapshot_error(
                snapshot.snapshot_id, "PHYSICAL_DELETE", "append snapshot contains a removed file"
            )
        if entry.status is ManifestStatus.ADDED and entry.content is not ManifestContent.DATA:
            raise blocked_snapshot_error(
                snapshot.snapshot_id, "DELETE_FILE", "append snapshot contains an Iceberg delete file"
            )


def validate_trusted_rewrite(
    snapshot: SnapshotRecord,
    entries: tuple[ManifestEntry, ...],
    trust: MaintenanceTrust | None,
) -> None:
    """Require authenticated catalog evidence and manifest invariants for a logical rewrite.

    Args:
        snapshot: Rewrite snapshot.
        entries: Normalized manifest entries used by the trust verifier.
        trust: Catalog-authenticated decision.

    Raises:
        SourceSnapshotBlockedError: If any trust requirement is absent.
    """
    has_delete_file = any(entry.content is not ManifestContent.DATA for entry in entries)
    accepted = (
        trust is not None
        and trust.writer_identity is not None
        and trust.writer_authenticated
        and trust.writer_allowlisted
        and trust.logical_change is False
        and trust.manifest_invariants_match
        and not has_delete_file
    )
    if not accepted:
        raise blocked_snapshot_error(
            snapshot.snapshot_id,
            "UNTRUSTED_REWRITE",
            "rewrite lacks authenticated writer, logical no-change marker, or verified manifest invariants",
        )


def discover_added_targets(
    snapshot: SnapshotRecord,
    entries: Iterable[ManifestEntry],
) -> tuple[TargetKey, ...]:
    """Discover target identities only from this snapshot's added data files.

    Args:
        snapshot: Accepted append snapshot.
        entries: Normalized manifest entries.

    Returns:
        Unique targets in deterministic identity order.

    Raises:
        SourceSnapshotBlockedError: If a changed entry uses a different partition spec.
    """
    targets: set[TargetKey] = set()
    for entry in entries:
        if entry.status is not ManifestStatus.ADDED or entry.content is not ManifestContent.DATA:
            continue
        validate_entry_spec(snapshot, entry)
        targets.add(entry.partition.target_key())
    return sorted_targets(targets)


def discover_baseline_targets(
    snapshot: SnapshotRecord,
    entries: Iterable[ManifestEntry],
) -> tuple[TargetKey, ...]:
    """Discover all live data-file target partitions for a validated canonical baseline.

    Args:
        snapshot: Validated baseline snapshot.
        entries: Live manifest entries at that exact snapshot.

    Returns:
        Unique targets in deterministic identity order.
    """
    targets: set[TargetKey] = set()
    for entry in entries:
        if entry.status is ManifestStatus.DELETED or entry.content is not ManifestContent.DATA:
            continue
        validate_entry_spec(snapshot, entry)
        targets.add(entry.partition.target_key())
    return sorted_targets(targets)


def validate_entry_spec(snapshot: SnapshotRecord, entry: ManifestEntry) -> None:
    """Require manifest partitions to use the snapshot's approved specification.

    Args:
        snapshot: Owning source snapshot.
        entry: Manifest entry to validate.

    Raises:
        SourceSnapshotBlockedError: If the entry carries another partition specification id.
    """
    if entry.partition_spec_id != snapshot.partition_spec_id:
        raise blocked_snapshot_error(
            snapshot.snapshot_id, "PARTITION_SPEC_CHANGED", "manifest entry uses an unapproved partition specification"
        )


def sorted_targets(targets: set[TargetKey]) -> tuple[TargetKey, ...]:
    """Freeze target identities in deterministic string order.

    Args:
        targets: Mutable discovery accumulator.

    Returns:
        Immutable deterministic target identities.
    """
    return tuple(sorted(targets, key=lambda key: (key.tenant_id, key.namespace, key.org_id)))
