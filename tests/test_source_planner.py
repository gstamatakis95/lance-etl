"""Unit tests for pure Iceberg source contract, lineage, manifest, and scan primitives."""

from __future__ import annotations

import pytest

from lance_etl.source import (
    REQUIRED_PARTITION_FIELDS,
    MaintenanceTrust,
    ManifestContent,
    ManifestEntry,
    ManifestStatus,
    PartitionField,
    PartitionSpec,
    PartitionValues,
    SnapshotRecord,
    SourceCheckpoint,
    SourceContractError,
    SourceLineageError,
    SourcePlanningError,
    SourceSnapshotBlockedError,
    TableMetadata,
    TargetKey,
    WindowKind,
    classify_snapshot,
    discover_added_targets,
    discover_baseline_targets,
    snapshot_scan_options,
    validate_table_contract,
    walk_snapshot_lineage,
)


def partition_spec(spec_id: int = 7) -> PartitionSpec:
    """Return the exact production partition contract.

    Args:
        spec_id: Iceberg partition specification id.

    Returns:
        Required specification.
    """
    return PartitionSpec(spec_id, REQUIRED_PARTITION_FIELDS)


def snapshot(
    snapshot_id: int,
    parent_snapshot_id: int | None,
    sequence_number: int,
    operation: str = "append",
    committed_at_ms: int = 1_000,
    table_uuid: str = "table-a",
    spec_id: int = 7,
    summary: tuple[tuple[str, str], ...] = (),
) -> SnapshotRecord:
    """Return normalized snapshot metadata.

    Args:
        snapshot_id: Snapshot identifier.
        parent_snapshot_id: Direct parent identifier.
        sequence_number: Iceberg sequence number.
        operation: Snapshot operation summary value.
        committed_at_ms: Commit timestamp retained only for audit.
        table_uuid: Stable table identity.
        spec_id: Partition specification id.
        summary: Immutable snapshot summary fields.

    Returns:
        Snapshot record fixture.
    """
    return SnapshotRecord(
        table_uuid,
        snapshot_id,
        parent_snapshot_id,
        sequence_number,
        committed_at_ms,
        operation,
        spec_id,
        summary,
    )


def manifest_entry(
    snapshot_id: int,
    hour: int,
    tenant_id: str = "tenant-a",
    namespace: str = "vectors",
    org_id: str = "org-a",
    status: ManifestStatus = ManifestStatus.ADDED,
    content: ManifestContent = ManifestContent.DATA,
    spec_id: int = 7,
) -> ManifestEntry:
    """Return one normalized production manifest entry.

    Args:
        snapshot_id: Snapshot that changed the entry.
        hour: Iceberg hour-transform integer retained in raw manifest evidence.
        tenant_id: Tenant partition value.
        namespace: Namespace partition value.
        org_id: Organization partition value.
        status: Manifest status.
        content: Data or delete-file content type.
        spec_id: Partition specification id.

    Returns:
        Manifest entry fixture.
    """
    partition = PartitionValues(tenant_id, namespace, org_id, hour)
    return ManifestEntry(snapshot_id, spec_id, status, content, partition)


def trusted_rewrite() -> MaintenanceTrust:
    """Return complete authenticated logical-maintenance evidence.

    Returns:
        Accepted trust evidence.
    """
    return MaintenanceTrust("optimizer-service", True, True, False, True)


def test_partition_contract_is_exact() -> None:
    """A renamed hour field is rejected even when its source and transform match."""
    renamed = (*REQUIRED_PARTITION_FIELDS[:3], PartitionField("hour", "ts", "hour"))
    metadata = TableMetadata("table-a", 1, PartitionSpec(7, renamed))

    with pytest.raises(SourceContractError, match="active partition spec"):
        validate_table_contract(metadata, None)


@pytest.mark.parametrize(
    ("metadata", "checkpoint", "message"),
    [
        (
            TableMetadata("table-b", 2, partition_spec()),
            SourceCheckpoint("table-a", 1, 1, 7),
            "UUID changed",
        ),
        (
            TableMetadata("table-a", 2, partition_spec(8)),
            SourceCheckpoint("table-a", 1, 1, 7),
            "partition specification changed",
        ),
    ],
)
def test_checkpoint_contract_rejects_identity_drift(
    metadata: TableMetadata,
    checkpoint: SourceCheckpoint,
    message: str,
) -> None:
    """Durable table identity and partition identity cannot drift.

    Args:
        metadata: Current table contract under test.
        checkpoint: Durable source checkpoint.
        message: Expected contract failure text.
    """
    with pytest.raises(SourceContractError, match=message):
        validate_table_contract(metadata, checkpoint)


def test_lineage_follows_parent_and_sequence_instead_of_time_or_identifier() -> None:
    """Parent ancestry orders same-time commits even when identifiers decrease."""
    stop = snapshot(100, None, 10, committed_at_ms=9_000)
    first = snapshot(80, 100, 11, committed_at_ms=9_999)
    second = snapshot(2, 80, 12, committed_at_ms=9_999)

    ordered = walk_snapshot_lineage((second, stop, first), 2, 100, "table-a", 7)

    assert tuple(item.snapshot_id for item in ordered) == (80, 2)


def test_lineage_rejects_cycle() -> None:
    """A cyclic parent chain blocks planning instead of looping."""
    records = (snapshot(3, 2, 3), snapshot(2, 3, 2), snapshot(1, None, 1))

    with pytest.raises(SourceLineageError, match="cycle"):
        walk_snapshot_lineage(records, 3, 1, "table-a", 7)


def test_lineage_rejects_fork_from_recorded_tip() -> None:
    """A pinned head on another branch cannot advance the recorded audit tip."""
    records = (snapshot(1, None, 1), snapshot(9, None, 2), snapshot(10, 9, 3))

    with pytest.raises(SourceLineageError, match="not an ancestor"):
        walk_snapshot_lineage(records, 10, 1, "table-a", 7)


def test_unchanged_head_requires_retained_checkpoint_metadata() -> None:
    """An unchanged head is untrusted when its immutable metadata disappeared."""
    with pytest.raises(SourceLineageError, match="missing from bounded metadata"):
        walk_snapshot_lineage((), 1, 1, "table-a", 7)


@pytest.mark.parametrize(
    ("records", "message"),
    [
        ((snapshot(1, None, 1), snapshot(2, 1, 2, spec_id=6)), "partition specification changed"),
        ((snapshot(1, None, 1), snapshot(2, 1, 2, table_uuid="table-b")), "table UUID changed"),
        ((snapshot(1, None, 5), snapshot(2, 1, 4)), "sequence"),
    ],
)
def test_lineage_rejects_snapshot_identity_or_sequence_drift(
    records: tuple[SnapshotRecord, ...],
    message: str,
) -> None:
    """Historical identity and sequence evidence must match the pinned contract.

    Args:
        records: Snapshot ancestry under test.
        message: Expected lineage failure text.
    """
    with pytest.raises(SourceLineageError, match=message):
        walk_snapshot_lineage(records, 2, 1, "table-a", 7)


def test_added_target_discovery_is_unique_sorted_and_ignores_existing_files() -> None:
    """Planning retains only unique logical targets, not unused pruning-hour state."""
    record = snapshot(2, 1, 2)
    entries = (
        manifest_entry(1, 10, tenant_id="ignored", status=ManifestStatus.EXISTING),
        manifest_entry(2, 50, tenant_id="tenant-z"),
        manifest_entry(2, 12, tenant_id="tenant-a"),
        manifest_entry(2, 50, tenant_id="tenant-a"),
    )

    assert discover_added_targets(record, entries) == (
        TargetKey("tenant-a", "vectors", "org-a"),
        TargetKey("tenant-z", "vectors", "org-a"),
    )


def test_baseline_target_discovery_includes_all_live_data_files() -> None:
    """A baseline includes existing and added data targets but not deleted files."""
    record = snapshot(9, None, 9)
    entries = (
        manifest_entry(1, 1, tenant_id="tenant-a", status=ManifestStatus.EXISTING),
        manifest_entry(9, 2, tenant_id="tenant-b"),
        manifest_entry(8, 3, tenant_id="tenant-c", status=ManifestStatus.DELETED),
    )

    assert discover_baseline_targets(record, entries) == (
        TargetKey("tenant-a", "vectors", "org-a"),
        TargetKey("tenant-b", "vectors", "org-a"),
    )


def test_nonempty_append_requires_added_manifest_evidence() -> None:
    """A declared nonempty append cannot silently become a no-work checkpoint."""
    record = snapshot(2, 1, 2, summary=(("added-data-files", "1"),))

    with pytest.raises(SourceSnapshotBlockedError) as raised:
        classify_snapshot(record, (), None)

    assert raised.value.error_code == "MANIFEST_EVIDENCE_MISSING"


def test_authenticated_logical_rewrite_is_accepted_without_targets() -> None:
    """Complete catalog trust evidence accepts a physical maintenance rewrite."""
    record = snapshot(3, 2, 3, operation="replace")
    entries = (
        manifest_entry(3, 2, status=ManifestStatus.ADDED),
        manifest_entry(3, 2, status=ManifestStatus.DELETED),
    )

    assert classify_snapshot(record, entries, trusted_rewrite()) is WindowKind.TRUSTED_MAINTENANCE


def test_untrusted_overwrite_is_blocked() -> None:
    """An overwrite summary without authenticated catalog evidence is insufficient."""
    record = snapshot(2, 1, 2, operation="overwrite")

    with pytest.raises(SourceSnapshotBlockedError) as raised:
        classify_snapshot(record, (manifest_entry(2, 2),), None)

    assert raised.value.error_code == "UNTRUSTED_REWRITE"


@pytest.mark.parametrize("operation", ["delete", "row_delta"])
def test_physical_delete_operations_are_blocked(operation: str) -> None:
    """Physical row deletions cannot be mistaken for append-only CDC.

    Args:
        operation: Iceberg delete-like operation under test.
    """
    with pytest.raises(SourceSnapshotBlockedError) as raised:
        classify_snapshot(snapshot(2, 1, 2, operation=operation), (), None)

    assert raised.value.error_code == "PHYSICAL_DELETE"


@pytest.mark.parametrize(
    ("entry", "error_code"),
    [
        (manifest_entry(2, 2, status=ManifestStatus.DELETED), "PHYSICAL_DELETE"),
        (manifest_entry(2, 2, content=ManifestContent.POSITION_DELETES), "DELETE_FILE"),
        (manifest_entry(3, 2), "MANIFEST_SNAPSHOT_MISMATCH"),
    ],
)
def test_append_rejects_unsafe_manifest_changes(entry: ManifestEntry, error_code: str) -> None:
    """Append classification fails closed on removed, delete, or foreign files.

    Args:
        entry: Unsafe manifest entry under test.
        error_code: Expected bounded rejection classification.
    """
    with pytest.raises(SourceSnapshotBlockedError) as raised:
        classify_snapshot(snapshot(2, 1, 2), (entry,), None)

    assert raised.value.error_code == error_code


def test_target_discovery_rejects_partition_spec_drift() -> None:
    """A changed manifest partition specification cannot create routing work."""
    with pytest.raises(SourceSnapshotBlockedError) as raised:
        discover_added_targets(snapshot(2, 1, 2), (manifest_entry(2, 2, spec_id=8),))

    assert raised.value.error_code == "PARTITION_SPEC_CHANGED"


def test_scan_options_are_exact_and_deterministic() -> None:
    """Baseline and append reads remain pinned after the catalog head advances."""
    baseline = snapshot_scan_options(snapshot(1, None, 1), WindowKind.BASELINE)
    record = snapshot(2, 1, 2)
    original = snapshot_scan_options(record, WindowKind.APPEND)
    replay = snapshot_scan_options(record, WindowKind.APPEND)

    assert baseline == (("snapshot-id", "1"),)
    assert replay == original
    assert replay == (("start-snapshot-id", "1"), ("end-snapshot-id", "2"))


@pytest.mark.parametrize(
    ("record", "kind", "message"),
    [
        (snapshot(2, None, 2), WindowKind.APPEND, "direct parent"),
        (snapshot(2, 1, 2), WindowKind.TRUSTED_MAINTENANCE, "do not produce"),
    ],
)
def test_scan_plan_rejects_non_data_windows(
    record: SnapshotRecord,
    kind: WindowKind,
    message: str,
) -> None:
    """An invalid append or maintenance no-op cannot become a Spark data read.

    Args:
        record: Snapshot under test.
        kind: Source window classification.
        message: Expected planning failure text.
    """
    with pytest.raises(SourcePlanningError, match=message):
        snapshot_scan_options(record, kind)
