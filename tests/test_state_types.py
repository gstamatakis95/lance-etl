"""Unit tests for normalized control-plane entities and typed identities."""

from __future__ import annotations

import uuid
from dataclasses import replace
from datetime import UTC, datetime, timedelta

import pytest

from lance_etl.state.repository import dataset_uri_matches_root
from lance_etl.state.specs import DatasetSpecRevision, IndexType, production_default_spec_revision
from lance_etl.state.tables import metadata
from lance_etl.state.types import (
    DatasetPlan,
    IcebergSource,
    PublicationEvidence,
    PublicationIndexEvidence,
    RoutingIdentity,
    SourceLifecycleState,
    SourceSnapshotKind,
    SourceSnapshotPlan,
    deterministic_dataset_id,
    deterministic_ingest_work_id,
    deterministic_publication_id,
    deterministic_publish_work_id,
    deterministic_rebuild_work_id,
    ingest_uri,
    rebuild_uri,
    validate_routing_segment,
)

APPLICATION_TABLES: set[str] = {
    "dataset_spec_revisions",
    "dataset_fields",
    "index_definitions",
    "iceberg_sources",
    "datasets",
    "source_snapshots",
    "dataset_work",
    "dataset_publications",
    "publication_indexes",
}
"""Exact normalized PostgreSQL application entity set."""


def source_registration() -> IcebergSource:
    """Build one valid database-owned source registration.

    Returns:
        Valid source configuration for unit tests.
    """
    return IcebergSource(
        source_id=uuid.uuid4(),
        source_name="vectors",
        spark_catalog="local",
        table_namespace="lake.raw",
        table_name="embeddings",
        table_uuid=uuid.uuid4(),
        lance_base_uri="file:///tmp/lance",
        lifecycle_state=SourceLifecycleState.ACTIVE,
        default_spec_id=production_default_spec_revision().spec_id,
        canonical_baseline_snapshot_id=10,
        replay_horizon=timedelta(days=30),
    )


def test_metadata_contains_exact_normalized_entities() -> None:
    """SQLAlchemy metadata contains only the nine approved entities."""
    assert set(metadata.tables) == APPLICATION_TABLES


@pytest.mark.parametrize(
    "value",
    ["", ".", "..", "../escape", "a/b", "a\\b", "a.b", "a b", "x\ncontrol", "a" * 129],
)
def test_routing_validation_rejects_unsafe_segments(value: str) -> None:
    """Routing validation rejects traversal, punctuation, and oversized values.

    Args:
        value: Invalid segment under test.
    """
    with pytest.raises(ValueError, match="must match"):
        validate_routing_segment(value, "tenant_id")


def test_routing_and_source_configuration_validate() -> None:
    """Routes and database-owned source mappings accept the shared contract."""
    identity: RoutingIdentity = RoutingIdentity(tenant_id="Tenant-1_A", namespace="vectors", org_id="org1")
    assert DatasetPlan(identity=identity).validate().identity == identity
    source: IcebergSource = source_registration()
    assert source.validate().spark_table == "local.lake.raw.embeddings"
    with pytest.raises(ValueError, match="projection columns"):
        replace(source, vectors_column="bad/column").validate()
    with pytest.raises(ValueError, match="replay_horizon"):
        replace(source, replay_horizon=timedelta(0)).validate()


def test_snapshot_plans_require_timezone_and_direct_lineage() -> None:
    """Snapshot plans reject naive commit times and missing incremental parents."""
    source_id: uuid.UUID = uuid.uuid4()
    valid: SourceSnapshotPlan = SourceSnapshotPlan(
        source_id=source_id,
        snapshot_id=10,
        parent_snapshot_id=None,
        iceberg_sequence_number=10,
        partition_spec_id=7,
        committed_at=datetime(2026, 1, 1, tzinfo=UTC),
        iceberg_operation="append",
        kind=SourceSnapshotKind.BASELINE,
    )
    assert valid.validate() == valid
    with pytest.raises(ValueError, match="only BASELINE or REJECTED"):
        replace(valid, snapshot_id=11, kind=SourceSnapshotKind.APPEND).validate()
    with pytest.raises(ValueError, match="timezone-aware"):
        replace(valid, committed_at=datetime(2026, 1, 1)).validate()


def test_dataset_work_and_publication_ids_are_stable_and_scoped() -> None:
    """Deterministic identities replay exactly and remain source or generation scoped."""
    identity: RoutingIdentity = RoutingIdentity(tenant_id="tenant1", namespace="vectors", org_id="org1")
    first_source: uuid.UUID = uuid.uuid4()
    second_source: uuid.UUID = uuid.uuid4()
    dataset_id: uuid.UUID = deterministic_dataset_id(first_source, identity)
    assert dataset_id == deterministic_dataset_id(first_source, identity)
    assert dataset_id != deterministic_dataset_id(second_source, identity)
    assert deterministic_ingest_work_id(dataset_id, 7) == deterministic_ingest_work_id(dataset_id, 7)
    assert deterministic_ingest_work_id(dataset_id, 7) != deterministic_ingest_work_id(dataset_id, 8)
    spec_revision_id: uuid.UUID = production_default_spec_revision().spec_revision_id
    publish_id: uuid.UUID = deterministic_publish_work_id(dataset_id, 7, spec_revision_id)
    request_id: uuid.UUID = uuid.uuid4()
    rebuild_id: uuid.UUID = deterministic_rebuild_work_id(dataset_id, request_id)
    assert publish_id != rebuild_id
    assert deterministic_publication_id(publish_id) == deterministic_publication_id(publish_id)


def test_dataset_uri_validation_accepts_only_owned_layouts() -> None:
    """Publication cannot broaden a dataset to an arbitrary URI below its source root."""
    dataset_id: uuid.UUID = uuid.uuid4()
    work_id: uuid.UUID = uuid.uuid4()
    base_uri: str = "s3://bucket/lance"
    assert dataset_uri_matches_root(base_uri, dataset_id, ingest_uri(base_uri, dataset_id))
    assert dataset_uri_matches_root(base_uri, dataset_id, rebuild_uri(base_uri, dataset_id, work_id))
    assert not dataset_uri_matches_root(base_uri, dataset_id, f"{base_uri}/other/{work_id}.lance")
    assert not dataset_uri_matches_root(base_uri, dataset_id, f"{base_uri}/rebuild/{dataset_id}/../bad.lance")


def test_publication_evidence_requires_exact_cardinality_and_index_coverage() -> None:
    """Typed evidence rejects duplicate, partial, and contradictory qualification results."""
    index_id: uuid.UUID = uuid.uuid4()
    index_evidence: PublicationIndexEvidence = PublicationIndexEvidence(
        index_definition_id=index_id,
        actual_index_type=IndexType.BTREE,
        indexed_fragment_count=2,
        unindexed_fragment_count=0,
    )
    evidence: PublicationEvidence = PublicationEvidence(
        schema_digest=b"s" * 32,
        total_row_count=10,
        distinct_row_count=10,
        live_row_count=8,
        distinct_live_row_count=8,
        fragment_count=2,
        indexes=(index_evidence,),
    )
    assert evidence.validate() == evidence
    with pytest.raises(ValueError, match="cover every fragment"):
        replace(index_evidence, unindexed_fragment_count=1).validate()
    with pytest.raises(ValueError, match="repeats an index definition"):
        replace(evidence, indexes=(index_evidence, index_evidence)).validate()
    with pytest.raises(ValueError, match="rows must be distinct"):
        replace(evidence, distinct_row_count=11).validate()
    with pytest.raises(ValueError, match="rows must be distinct"):
        replace(evidence, distinct_row_count=9).validate()
    with pytest.raises(ValueError, match="live rows must be distinct"):
        replace(evidence, distinct_live_row_count=7).validate()


def test_default_spec_exposes_typed_ingest_compaction_and_index_options() -> None:
    """The bundled immutable revision carries every major per-dataset option family."""
    revision: DatasetSpecRevision = production_default_spec_revision().validate()
    assert revision.expected_configuration_digest() == revision.configuration_digest
    assert revision.ingest_shuffle_partitions > 0
    assert revision.merge_rows_per_chunk > 0
    assert revision.compaction_enabled
    assert revision.target_rows_per_fragment > 0
    assert revision.fragments_per_index_task > 0
    assert revision.retained_publications > 0
    assert len(revision.fields) == 9
    assert len(revision.indexes) == 6
    assert revision.vector_options_for_field("vector").maximum_partitions >= 1
    assert revision.fts_options_for_field("text").max_unindexed_fragments >= 0
