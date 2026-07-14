"""Unit tests for exact parent-linked Iceberg source-window planning."""

from __future__ import annotations

from dataclasses import dataclass, field, replace

import pytest

from lance_etl.source import (
    REQUIRED_PARTITION_FIELDS,
    BaselineProof,
    MaintenanceTrust,
    ManifestContent,
    ManifestEntry,
    ManifestStatus,
    PartitionField,
    PartitionSpec,
    PartitionValues,
    SnapshotRecord,
    SourceBaselineError,
    SourceCheckpoint,
    SourceContractError,
    SourceLineageError,
    SourcePlanner,
    SourceSnapshotBlockedError,
    TableMetadata,
    TargetKey,
    WindowKind,
    build_spark_scan,
    walk_snapshot_lineage,
)


@dataclass(slots=True)
class FakeCatalog:
    """In-memory bounded metadata provider used by planner unit tests."""

    metadata: TableMetadata
    snapshots: tuple[SnapshotRecord, ...]
    entries: dict[int, tuple[ManifestEntry, ...]] = field(default_factory=dict)
    trusts: dict[int, MaintenanceTrust] = field(default_factory=dict)
    snapshot_requests: list[tuple[int, int]] = field(default_factory=list)
    manifest_requests: list[int] = field(default_factory=list)
    table_requests: list[str] = field(default_factory=list)

    def table_metadata(self, table: str) -> TableMetadata:
        """Return the configured metadata.

        Args:
            table: Ignored catalog-qualified table name.

        Returns:
            Configured metadata.
        """
        self.table_requests.append(table)
        return self.metadata

    def snapshots_through(
        self, table: str, pinned_head_snapshot_id: int, stop_snapshot_id: int
    ) -> tuple[SnapshotRecord, ...]:
        """Return the configured bounded snapshot records.

        Args:
            table: Ignored catalog-qualified table name.
            pinned_head_snapshot_id: Pinned head requested by the planner.
            stop_snapshot_id: Inclusive stopping point requested by the planner.

        Returns:
            Configured snapshots.
        """
        self.table_requests.append(table)
        self.snapshot_requests.append((pinned_head_snapshot_id, stop_snapshot_id))
        return self.snapshots

    def manifest_entries(self, table: str, snapshot_id: int) -> tuple[ManifestEntry, ...]:
        """Return entries for one exact snapshot.

        Args:
            table: Ignored catalog-qualified table name.
            snapshot_id: Snapshot whose entries are requested.

        Returns:
            Configured entries or an empty tuple.
        """
        self.table_requests.append(table)
        self.manifest_requests.append(snapshot_id)
        return self.entries.get(snapshot_id, ())

    def maintenance_trust(self, table: str, snapshot_id: int) -> MaintenanceTrust | None:
        """Return configured authenticated rewrite evidence.

        Args:
            table: Ignored catalog-qualified table name.
            snapshot_id: Snapshot whose evidence is requested.

        Returns:
            Configured trust evidence or ``None``.
        """
        self.table_requests.append(table)
        return self.trusts.get(snapshot_id)


def partition_spec(spec_id: int = 7) -> PartitionSpec:
    """Return the exact production partition contract.

    Args:
        spec_id: Iceberg partition specification id.

    Returns:
        Required specification.
    """
    return PartitionSpec(spec_id, REQUIRED_PARTITION_FIELDS)


def table_metadata(head: int = 3, table_uuid: str = "table-a", spec_id: int = 7) -> TableMetadata:
    """Return standard table metadata.

    Args:
        head: Current main snapshot id.
        table_uuid: Stable table identity.
        spec_id: Active partition specification id.

    Returns:
        Table metadata fixture.
    """
    return TableMetadata(table_uuid, head, partition_spec(spec_id))


def snapshot(
    snapshot_id: int,
    parent_snapshot_id: int | None,
    sequence_number: int,
    operation: str = "append",
    committed_at_ms: int = 1_000,
    table_uuid: str = "table-a",
    spec_id: int = 7,
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
    """Return one normalized manifest entry in the production partition layout.

    Args:
        snapshot_id: Snapshot that changed the entry.
        hour: Iceberg hour-transform integer.
        tenant_id: Tenant partition value.
        namespace: Namespace partition value.
        org_id: Organization partition value.
        status: Manifest status.
        content: Data or delete-file content type.
        spec_id: Partition specification id.

    Returns:
        Manifest entry fixture.
    """
    values = PartitionValues(tenant_id, namespace, org_id, hour)
    return ManifestEntry(snapshot_id, spec_id, status, content, values)


def checkpoint(
    snapshot_id: int = 1,
    sequence_number: int | None = None,
    spec_id: int = 7,
    table_uuid: str = "table-a",
) -> SourceCheckpoint:
    """Return a durable source checkpoint fixture.

    Args:
        snapshot_id: Recorded source tip.
        sequence_number: Recorded Iceberg sequence number, defaulting to the fixture snapshot id.
        spec_id: Recorded partition specification id.
        table_uuid: Recorded table UUID.

    Returns:
        Checkpoint fixture.
    """
    resolved_sequence = snapshot_id if sequence_number is None else sequence_number
    return SourceCheckpoint(table_uuid, snapshot_id, resolved_sequence, spec_id)


def canonical_baseline(snapshot_id: int = 1) -> BaselineProof:
    """Return a valid canonical-baseline proof.

    Args:
        snapshot_id: Pinned baseline snapshot id.

    Returns:
        Canonical proof fixture.
    """
    return BaselineProof("table-a", snapshot_id, 7, True, 0)


def trusted_rewrite() -> MaintenanceTrust:
    """Return complete authenticated logical-maintenance evidence.

    Returns:
        Accepted trust evidence.
    """
    return MaintenanceTrust("optimizer-service", True, True, False, True)


def test_partition_contract_is_exact() -> None:
    """A renamed hour partition field is rejected even when its source and transform match."""
    renamed_fields = (*REQUIRED_PARTITION_FIELDS[:3], PartitionField("hour", "processing_timestamp", "hour"))
    catalog = FakeCatalog(TableMetadata("table-a", 1, PartitionSpec(7, renamed_fields)), (snapshot(1, None, 1),))
    with pytest.raises(SourceContractError, match="active partition spec"):
        SourcePlanner(catalog).plan("catalog.db.events", None, canonical_baseline())


def test_late_append_into_old_hour_is_discovered_from_snapshot_manifest() -> None:
    """A newly added file in an old hour remains owned by its append snapshot."""
    records = (snapshot(1, None, 1), snapshot(2, 1, 2))
    old_hour = 24
    catalog = FakeCatalog(table_metadata(2), records, {2: (manifest_entry(2, old_hour),)})
    plan = SourcePlanner(catalog).plan("catalog.db.events", checkpoint())
    assert len(plan.windows) == 1
    assert plan.windows[0].touched_targets[0].hours == (old_hour,)
    assert plan.windows[0].scans[0].options == (("start-snapshot-id", "1"), ("end-snapshot-id", "2"))


def test_target_discovery_reads_added_entries_only_and_unions_hours() -> None:
    """Existing files do not create work and several added hours produce one target work item."""
    records = (snapshot(1, None, 1), snapshot(2, 1, 2))
    entries = (
        manifest_entry(1, 10, tenant_id="ignored", status=ManifestStatus.EXISTING),
        manifest_entry(2, 50),
        manifest_entry(2, 12),
        manifest_entry(2, 50),
    )
    catalog = FakeCatalog(table_metadata(2), records, {2: entries})
    window = SourcePlanner(catalog).plan("catalog.db.events", checkpoint()).windows[0]
    assert window.touched_targets == (window.touched_targets[0],)
    assert window.touched_targets[0].target == TargetKey("tenant-a", "vectors", "org-a")
    assert window.touched_targets[0].hours == (12, 50)


def test_same_millisecond_snapshots_follow_parent_and_sequence() -> None:
    """Parent ancestry orders same-millisecond commits regardless of input record order."""
    first = snapshot(91, 10, 11, committed_at_ms=9_999)
    second = snapshot(4, 91, 12, committed_at_ms=9_999)
    stop = snapshot(10, None, 10, committed_at_ms=9_000)
    ordered = walk_snapshot_lineage((second, stop, first), 4, 10, "table-a", 7)
    assert tuple(item.snapshot_id for item in ordered) == (91, 4)


def test_snapshot_identifier_order_never_overrides_ancestry() -> None:
    """A smaller child snapshot id remains later than its larger parent snapshot id."""
    records = (snapshot(100, None, 1), snapshot(80, 100, 2), snapshot(2, 80, 3))
    catalog = FakeCatalog(table_metadata(2), records)
    plan = SourcePlanner(catalog).plan("catalog.db.events", checkpoint(100, sequence_number=1))
    assert tuple(window.snapshot.snapshot_id for window in plan.windows) == (80, 2)


def test_cycle_is_rejected() -> None:
    """A cyclic parent chain blocks planning instead of looping."""
    records = (snapshot(3, 2, 3), snapshot(2, 3, 2), snapshot(1, None, 1))
    with pytest.raises(SourceLineageError, match="cycle"):
        walk_snapshot_lineage(records, 3, 1, "table-a", 7)


def test_fork_from_recorded_tip_is_rejected() -> None:
    """A pinned head on another branch cannot advance the recorded audit tip."""
    records = (snapshot(1, None, 1), snapshot(9, None, 2), snapshot(10, 9, 3))
    catalog = FakeCatalog(table_metadata(10), records)
    with pytest.raises(SourceLineageError, match="not an ancestor"):
        SourcePlanner(catalog).plan("catalog.db.events", checkpoint(1))


def test_changed_table_uuid_is_rejected_before_lineage_read() -> None:
    """Replacement of the catalog table under the same name fails closed."""
    catalog = FakeCatalog(table_metadata(2, table_uuid="table-b"), (snapshot(2, 1, 2, table_uuid="table-b"),))
    with pytest.raises(SourceContractError, match="UUID changed"):
        SourcePlanner(catalog).plan("catalog.db.events", checkpoint())
    assert catalog.snapshot_requests == []


def test_changed_active_partition_spec_is_rejected_before_lineage_read() -> None:
    """Partition-spec evolution cannot silently alter source ownership."""
    catalog = FakeCatalog(table_metadata(2, spec_id=8), (snapshot(2, 1, 2, spec_id=8),))
    with pytest.raises(SourceContractError, match="partition specification changed"):
        SourcePlanner(catalog).plan("catalog.db.events", checkpoint())
    assert catalog.snapshot_requests == []


def test_snapshot_with_changed_spec_is_rejected() -> None:
    """Historical metadata inconsistent with the pinned active spec fails lineage validation."""
    records = (snapshot(1, None, 1), snapshot(2, 1, 2, spec_id=6))
    catalog = FakeCatalog(table_metadata(2), records)
    with pytest.raises(SourceLineageError, match="partition specification changed"):
        SourcePlanner(catalog).plan("catalog.db.events", checkpoint())


def test_historical_snapshot_with_changed_uuid_is_rejected() -> None:
    """A UUID mismatch inside returned ancestry fails even when current metadata looks unchanged."""
    records = (snapshot(1, None, 1), snapshot(2, 1, 2, table_uuid="table-b"))
    catalog = FakeCatalog(table_metadata(2), records)
    with pytest.raises(SourceLineageError, match="table UUID changed"):
        SourcePlanner(catalog).plan("catalog.db.events", checkpoint())


def test_nonmonotonic_sequence_number_is_rejected() -> None:
    """Direct ancestry cannot compensate for invalid or forked Iceberg sequence ordering."""
    records = (snapshot(1, None, 5), snapshot(2, 1, 4))
    catalog = FakeCatalog(table_metadata(2), records)
    with pytest.raises(SourceLineageError, match="sequence"):
        SourcePlanner(catalog).plan("catalog.db.events", checkpoint(1, sequence_number=5))


def test_arbitrary_current_snapshot_is_not_an_implicit_baseline() -> None:
    """A current CDC snapshot with distinct per-key histories requires a rejected proof."""
    records = (snapshot(1, None, 1),)
    proof = replace(canonical_baseline(), distinct_mutation_conflicts=3)
    catalog = FakeCatalog(table_metadata(1), records, {1: (manifest_entry(1, 20),)})
    with pytest.raises(SourceBaselineError, match="canonical proof"):
        SourcePlanner(catalog).plan("catalog.db.events", None, proof)


def test_missing_baseline_proof_is_rejected() -> None:
    """The planner never treats an arbitrary current snapshot as the initial baseline."""
    catalog = FakeCatalog(table_metadata(1), (snapshot(1, None, 1),))
    with pytest.raises(SourceBaselineError, match="canonical proof"):
        SourcePlanner(catalog).plan("catalog.db.events", None)


def test_validated_baseline_replays_retained_descendants_in_order() -> None:
    """A canonical ancestor is scanned once before each retained append increment."""
    records = (snapshot(3, 2, 3), snapshot(1, None, 1), snapshot(2, 1, 2))
    entries = {
        1: (manifest_entry(1, 1, status=ManifestStatus.EXISTING),),
        2: (manifest_entry(2, 2),),
        3: (manifest_entry(3, 3),),
    }
    catalog = FakeCatalog(table_metadata(3), records, entries)
    plan = SourcePlanner(catalog).plan("catalog.db.events", None, canonical_baseline())
    assert tuple(window.kind for window in plan.windows) == (
        WindowKind.BASELINE,
        WindowKind.APPEND,
        WindowKind.APPEND,
    )
    assert plan.windows[0].scans[0].options == (("snapshot-id", "1"),)
    assert plan.windows[1].scans[0].options == (("start-snapshot-id", "1"), ("end-snapshot-id", "2"))
    assert plan.windows[2].scans[0].options == (("start-snapshot-id", "2"), ("end-snapshot-id", "3"))


def test_append_trusted_replace_append_emits_only_append_scans() -> None:
    """An authenticated logical rewrite is a durable no-op between append windows."""
    records = (
        snapshot(1, None, 1),
        snapshot(2, 1, 2),
        snapshot(3, 2, 3, operation="replace"),
        snapshot(4, 3, 4),
    )
    entries = {
        2: (manifest_entry(2, 2),),
        3: (
            manifest_entry(3, 2, status=ManifestStatus.ADDED),
            manifest_entry(3, 2, status=ManifestStatus.DELETED),
        ),
        4: (manifest_entry(4, 4),),
    }
    catalog = FakeCatalog(table_metadata(4), records, entries, {3: trusted_rewrite()})
    plan = SourcePlanner(catalog).plan("catalog.db.events", checkpoint())
    assert tuple(window.kind for window in plan.windows) == (
        WindowKind.APPEND,
        WindowKind.TRUSTED_MAINTENANCE,
        WindowKind.APPEND,
    )
    assert tuple(len(window.scans) for window in plan.windows) == (1, 0, 1)


def test_untrusted_overwrite_is_blocked() -> None:
    """An overwrite summary without authenticated catalog evidence is insufficient."""
    records = (snapshot(1, None, 1), snapshot(2, 1, 2, operation="overwrite"))
    catalog = FakeCatalog(table_metadata(2), records, {2: (manifest_entry(2, 2),)})
    with pytest.raises(SourceSnapshotBlockedError) as raised:
        SourcePlanner(catalog).plan("catalog.db.events", checkpoint())
    assert raised.value.snapshot_id == 2
    assert raised.value.error_code == "UNTRUSTED_REWRITE"


@pytest.mark.parametrize("operation", ["delete", "row_delta"])
def test_physical_delete_operations_are_blocked(operation: str) -> None:
    """Physical row deletions cannot be mistaken for append-only CDC.

    Args:
        operation: Iceberg delete-like operation under test.
    """
    records = (snapshot(1, None, 1), snapshot(2, 1, 2, operation=operation))
    catalog = FakeCatalog(table_metadata(2), records)
    with pytest.raises(SourceSnapshotBlockedError) as raised:
        SourcePlanner(catalog).plan("catalog.db.events", checkpoint())
    assert raised.value.error_code == "PHYSICAL_DELETE"


def test_append_with_removed_data_file_is_blocked() -> None:
    """A mislabeled append that removes a physical file fails closed."""
    records = (snapshot(1, None, 1), snapshot(2, 1, 2))
    catalog = FakeCatalog(
        table_metadata(2),
        records,
        {2: (manifest_entry(2, 2, status=ManifestStatus.DELETED),)},
    )
    with pytest.raises(SourceSnapshotBlockedError) as raised:
        SourcePlanner(catalog).plan("catalog.db.events", checkpoint())
    assert raised.value.error_code == "PHYSICAL_DELETE"


def test_append_with_delete_file_is_blocked() -> None:
    """Iceberg position and equality delete files are outside the append CDC contract."""
    records = (snapshot(1, None, 1), snapshot(2, 1, 2))
    catalog = FakeCatalog(
        table_metadata(2),
        records,
        {2: (manifest_entry(2, 2, content=ManifestContent.POSITION_DELETES),)},
    )
    with pytest.raises(SourceSnapshotBlockedError) as raised:
        SourcePlanner(catalog).plan("catalog.db.events", checkpoint())
    assert raised.value.error_code == "DELETE_FILE"


def test_scan_plan_is_deterministic_after_catalog_head_advances() -> None:
    """A retry can execute the stored exact parent-to-snapshot scan after newer commits arrive."""
    record = snapshot(2, 1, 2)
    original = build_spark_scan("catalog.db.events", record, WindowKind.APPEND, TargetKey("t", "n", "o"))
    newer_snapshot = snapshot(3, 2, 3)
    replay = build_spark_scan("catalog.db.events", record, WindowKind.APPEND, TargetKey("t", "n", "o"))
    assert newer_snapshot.snapshot_id == 3
    assert replay == original
    assert replay.options == (("start-snapshot-id", "1"), ("end-snapshot-id", "2"))


def test_repeated_planning_produces_byte_stable_target_and_window_order() -> None:
    """Catalog entry ordering cannot change an idempotent planner retry."""
    records = (snapshot(1, None, 1), snapshot(2, 1, 2), snapshot(3, 2, 3))
    unordered_entries = {
        2: (
            manifest_entry(2, 9, tenant_id="tenant-z"),
            manifest_entry(2, 2, tenant_id="tenant-a"),
            manifest_entry(2, 1, tenant_id="tenant-a"),
        ),
        3: (manifest_entry(3, 10, tenant_id="tenant-m"),),
    }
    first_catalog = FakeCatalog(table_metadata(3), records, unordered_entries)
    second_entries = {snapshot_id: tuple(reversed(entries)) for snapshot_id, entries in unordered_entries.items()}
    second_catalog = FakeCatalog(table_metadata(3), tuple(reversed(records)), second_entries)
    first = SourcePlanner(first_catalog).plan("catalog.db.events", checkpoint())
    second = SourcePlanner(second_catalog).plan("catalog.db.events", checkpoint())
    assert first == second


def test_planner_requests_only_checkpoint_to_pinned_head_history() -> None:
    """The catalog API is bounded by the recorded tip instead of collecting complete history."""
    records = (snapshot(40, 39, 40), snapshot(39, 38, 39), snapshot(38, None, 38))
    catalog = FakeCatalog(table_metadata(40), records)
    SourcePlanner(catalog).plan("catalog.db.events", checkpoint(38))
    assert catalog.snapshot_requests == [(40, 38)]


def test_plan_retention_floor_is_first_append_parent() -> None:
    """The immutable plan exposes the parent snapshot needed for exact retry reads."""
    records = (snapshot(1, None, 1), snapshot(2, 1, 2), snapshot(3, 2, 3))
    catalog = FakeCatalog(table_metadata(3), records)
    plan = SourcePlanner(catalog).plan("catalog.db.events", checkpoint())
    assert plan.retention_snapshot_id() == 1
