"""Typed control-plane values and deterministic identities."""

from __future__ import annotations

import re
import uuid
from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum

ROUTING_SEGMENT_PATTERN: re.Pattern[str] = re.compile(r"^[A-Za-z0-9_-]{1,128}$")
"""Bounded routing-segment contract shared with the search service."""

TARGET_ID_NAMESPACE: uuid.UUID = uuid.UUID("8fdbb20c-a980-4cab-a544-b22b699f8a4e")
"""Stable namespace used to derive opaque target identifiers."""

WORK_ID_NAMESPACE: uuid.UUID = uuid.UUID("c73b9521-68b9-479f-aa19-7f646f129091")
"""Stable namespace used to derive replay-safe work identifiers."""


class SourceWindowKind(StrEnum):
    """Kinds of accepted or durably rejected Iceberg source windows."""

    BASELINE = "BASELINE"
    APPEND = "APPEND"
    TRUSTED_MAINTENANCE = "TRUSTED_MAINTENANCE"
    REJECTED = "REJECTED"


class SourceWindowState(StrEnum):
    """Durable source-window states."""

    SEALED = "SEALED"
    COMPLETE = "COMPLETE"
    BLOCKED = "BLOCKED"


class WorkKind(StrEnum):
    """Kinds of target-lane work."""

    INGEST = "INGEST"
    SERVE = "SERVE"
    REBUILD = "REBUILD"


class WorkState(StrEnum):
    """Durable target-work states."""

    PENDING = "PENDING"
    RUNNING = "RUNNING"
    RETRY_WAIT = "RETRY_WAIT"
    SUCCEEDED = "SUCCEEDED"
    BLOCKED = "BLOCKED"


class WorkPhase(StrEnum):
    """Checkpointed phases of target work."""

    INGEST = "INGEST"
    MAINTAIN = "MAINTAIN"
    INDEX = "INDEX"
    VALIDATE = "VALIDATE"
    PREWARM = "PREWARM"
    PUBLISH = "PUBLISH"


def validate_routing_segment(value: str, field: str) -> str:
    """Validate one bounded ASCII routing segment.

    Args:
        value: Segment value supplied by source metadata or a lookup request.
        field: Field name included in a bounded validation error.

    Returns:
        The validated value unchanged.

    Raises:
        ValueError: If the value is empty, too long, or contains a disallowed character.
    """
    if ROUTING_SEGMENT_PATTERN.fullmatch(value) is None:
        raise ValueError(f"{field} must be non-empty and match [A-Za-z0-9_-]{{1,128}}")
    return value


@dataclass(frozen=True)
class RoutingIdentity:
    """Validated identity of one physical Lance target."""

    tenant_id: str
    namespace: str
    org_id: str

    def validate(self) -> RoutingIdentity:
        """Validate all routing values and return this identity.

        Returns:
            This identity after every segment passes the shared contract.
        """
        validate_routing_segment(self.tenant_id, "tenant_id")
        validate_routing_segment(self.namespace, "namespace")
        validate_routing_segment(self.org_id, "org_id")
        return self

    def canonical(self) -> str:
        """Return the unambiguous identity used for deterministic IDs.

        Returns:
            A length-prefixed canonical identity string.
        """
        self.validate()
        values: tuple[str, str, str] = (self.tenant_id, self.namespace, self.org_id)
        return "".join(f"{len(value)}:{value}" for value in values)


def deterministic_target_id(identity: RoutingIdentity) -> uuid.UUID:
    """Derive the opaque target ID for a routing identity.

    Args:
        identity: Validated source routing identity.

    Returns:
        Stable opaque UUID for that identity.
    """
    return uuid.uuid5(TARGET_ID_NAMESPACE, identity.canonical())


def deterministic_ingest_work_id(target_id: uuid.UUID, source_window_seq: int) -> uuid.UUID:
    """Derive one INGEST work ID from its durable idempotency key.

    Args:
        target_id: Opaque target identifier.
        source_window_seq: Monotonic control-plane source-window sequence.

    Returns:
        Stable UUID for the target and source-window pair.
    """
    if source_window_seq < 1:
        raise ValueError("source_window_seq must be positive")
    return uuid.uuid5(WORK_ID_NAMESPACE, f"INGEST:{target_id}:{source_window_seq}")


def ingest_uri(base_uri: str, target_id: uuid.UUID) -> str:
    """Build a target URI without exposing raw routing values in the path.

    Args:
        base_uri: Deployment-owned Lance storage root.
        target_id: Opaque target identifier.

    Returns:
        URI under the deployment root named only by the target UUID.

    Raises:
        ValueError: If the deployment root is empty.
    """
    normalized: str = base_uri.rstrip("/")
    if not normalized:
        raise ValueError("base_uri must be non-empty")
    return f"{normalized}/{target_id}.lance"


@dataclass(frozen=True)
class TargetPlan:
    """Target identity and release-owned profile selected during source planning."""

    identity: RoutingIdentity
    profile_id: str

    def validate(self) -> TargetPlan:
        """Validate a target plan before database or storage access.

        Returns:
            This target plan after validation.

        Raises:
            ValueError: If its routing identity or profile identifier is invalid.
        """
        self.identity.validate()
        if ROUTING_SEGMENT_PATTERN.fullmatch(self.profile_id) is None:
            raise ValueError("profile_id must be non-empty and match [A-Za-z0-9_-]{1,128}")
        return self


@dataclass(frozen=True)
class SourceWindowPlan:
    """Immutable source snapshot metadata sealed by the serial planner."""

    table_uuid: uuid.UUID
    snapshot_id: int
    parent_snapshot_id: int | None
    iceberg_sequence_number: int
    partition_spec_id: int
    kind: SourceWindowKind

    def validate(self) -> SourceWindowPlan:
        """Validate source-window invariants before insertion.

        Returns:
            This source-window plan after validation.

        Raises:
            ValueError: If IDs are invalid or a non-baseline omits its parent.
        """
        if self.snapshot_id < 0:
            raise ValueError("snapshot_id must be non-negative")
        if self.iceberg_sequence_number < 0:
            raise ValueError("iceberg_sequence_number must be non-negative")
        if self.partition_spec_id < 0:
            raise ValueError("partition_spec_id must be non-negative")
        if self.kind != SourceWindowKind.BASELINE and self.parent_snapshot_id is None:
            raise ValueError("only a BASELINE source window may omit parent_snapshot_id")
        return self


@dataclass(frozen=True)
class WorkClaim:
    """Fenced lease returned to one target worker."""

    work_id: uuid.UUID
    target_id: uuid.UUID
    kind: WorkKind
    phase: WorkPhase
    lease_token: uuid.UUID
    fence_epoch: int
    attempt_count: int
    source_window_seq: int | None
    expected_ingest_lance_uri: str
    data_lance_version: int | None


@dataclass(frozen=True)
class WorkExecutionContext:
    """Immutable source and target context resolved for one fenced claim.

    Attributes:
        claim: Fenced durable work identity.
        identity: Validated logical target identity.
        profile_id: Release-owned target profile.
        snapshot_id: Exact Iceberg source snapshot for INGEST.
        parent_snapshot_id: Exact incremental scan parent for INGEST.
        iceberg_sequence_number: Arrival-order watermark for INGEST rows.
        partition_spec_id: Exact source partition specification for the snapshot.
        source_window_kind: Accepted source classification for INGEST.
        candidate_lance_uri: Frozen SERVE, REBUILD, or rollback candidate URI.
        indexed_lance_version: Exact candidate version when already known.
        artifact_manifest_uri: Retained immutable validation manifest when already known.
        artifact_digest: Retained manifest digest when already known.
    """

    claim: WorkClaim
    identity: RoutingIdentity
    profile_id: str
    snapshot_id: int | None
    parent_snapshot_id: int | None
    iceberg_sequence_number: int | None
    partition_spec_id: int | None
    source_window_kind: SourceWindowKind | None
    candidate_lance_uri: str | None
    indexed_lance_version: int | None
    artifact_manifest_uri: str | None
    artifact_digest: bytes | None


@dataclass(frozen=True)
class ControlPlaneStatus:
    """Bounded operational snapshot used by reconciliation and SLO reporting."""

    pending_work: int
    running_work: int
    retry_wait_work: int
    blocked_work: int
    due_work: int
    blocked_source_windows: int
    oldest_open_work_at: datetime | None
    retention_window_seq: int | None
    retention_snapshot_id: int | None
    retention_parent_snapshot_id: int | None
    retention_state: SourceWindowState | None
    retention_created_at: datetime | None


@dataclass(frozen=True)
class ServingTarget:
    """Exact authenticated serving-catalog result.

    Attributes:
        target_id: Opaque target identifier.
        identity: Validated logical target identity.
        lance_uri: Deployment-owned physical dataset URI.
        lance_version: Exact validated Lance version.
        profile_id: Release-owned query policy identifier.
    """

    target_id: uuid.UUID
    identity: RoutingIdentity
    lance_uri: str
    lance_version: int
    profile_id: str
