"""Unit tests for control-plane identities and three-table metadata."""

from __future__ import annotations

import uuid

import pytest

from lance_etl.state.tables import metadata
from lance_etl.state.types import (
    RoutingIdentity,
    SourceWindowKind,
    SourceWindowPlan,
    TargetPlan,
    deterministic_ingest_work_id,
    deterministic_target_id,
    ingest_uri,
    validate_routing_segment,
)


def test_metadata_contains_exactly_three_application_tables() -> None:
    """The SQLAlchemy application metadata contains only the three approved entities."""
    assert set(metadata.tables) == {"source_windows", "targets", "target_work"}


@pytest.mark.parametrize(
    "value",
    ["", ".", "..", "../escape", "a/b", "a\\b", "a.b", "a b", "x\ncontrol", "a" * 129],
)
def test_routing_validation_rejects_unsafe_segments(value: str) -> None:
    """Python rejects the same traversal and punctuation cases as Rust.

    Args:
        value: Invalid segment under test.
    """
    with pytest.raises(ValueError, match="must be non-empty"):
        validate_routing_segment(value, "tenant_id")


def test_routing_validation_accepts_bounded_ascii() -> None:
    """The shared routing contract accepts alphanumerics, hyphen, and underscore."""
    assert validate_routing_segment("Tenant-1_A", "tenant_id") == "Tenant-1_A"


def test_target_and_work_ids_are_stable_and_opaque() -> None:
    """Deterministic IDs replay exactly without exposing routing strings in the URI."""
    identity = RoutingIdentity(tenant_id="tenant1", namespace="namespace1", org_id="org1")
    first: uuid.UUID = deterministic_target_id(identity)
    second: uuid.UUID = deterministic_target_id(identity)
    assert first == second
    uri: str = ingest_uri("s3://bucket/lance/", first)
    assert uri == f"s3://bucket/lance/{first}.lance"
    assert "tenant1" not in uri
    assert deterministic_ingest_work_id(first, 7) == deterministic_ingest_work_id(first, 7)
    assert deterministic_ingest_work_id(first, 7) != deterministic_ingest_work_id(first, 8)


def test_source_and_target_plans_validate_contracts() -> None:
    """Typed plans reject missing lineage and invalid release profiles before database access."""
    identity = RoutingIdentity(tenant_id="tenant1", namespace="namespace1", org_id="org1")
    assert TargetPlan(identity=identity, profile_id="production_v1").validate().identity == identity
    with pytest.raises(ValueError, match="profile_id"):
        TargetPlan(identity=identity, profile_id="bad/profile").validate()
    with pytest.raises(ValueError, match="only a BASELINE"):
        SourceWindowPlan(
            table_uuid=uuid.uuid4(),
            snapshot_id=2,
            parent_snapshot_id=None,
            iceberg_sequence_number=2,
            partition_spec_id=7,
            kind=SourceWindowKind.APPEND,
        ).validate()
    with pytest.raises(ValueError, match="only a BASELINE"):
        SourceWindowPlan(
            table_uuid=uuid.uuid4(),
            snapshot_id=3,
            parent_snapshot_id=None,
            iceberg_sequence_number=3,
            partition_spec_id=7,
            kind=SourceWindowKind.REJECTED,
        ).validate()
