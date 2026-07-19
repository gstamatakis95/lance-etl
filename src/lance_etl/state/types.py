"""Typed PostgreSQL control-plane identities, state values, and evidence."""

from __future__ import annotations

import re
import uuid
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import StrEnum

from lance_etl.state.specs import DatasetSpecRevision, IndexType

ROUTING_SEGMENT_PATTERN: re.Pattern[str] = re.compile(r"^[A-Za-z0-9_-]{1,128}$")
"""Bounded routing-segment contract shared with the search service."""

SOURCE_NAME_PATTERN: re.Pattern[str] = re.compile(r"^[A-Za-z][A-Za-z0-9_-]{0,127}$")
"""Bounded source-name contract shared with PostgreSQL."""

IDENTIFIER_PATTERN: re.Pattern[str] = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,127}$")
"""Bounded source-column and table identifier contract."""

TABLE_NAMESPACE_PATTERN: re.Pattern[str] = re.compile(r"^[A-Za-z_][A-Za-z0-9_.]{0,255}$")
"""Bounded dotted Spark namespace contract."""

DATASET_ID_NAMESPACE: uuid.UUID = uuid.UUID("8fdbb20c-a980-4cab-a544-b22b699f8a4e")
"""Namespace used to derive source-scoped dataset identities."""

WORK_ID_NAMESPACE: uuid.UUID = uuid.UUID("c73b9521-68b9-479f-aa19-7f646f129091")
"""Namespace used to derive replay-safe work identities."""

PUBLICATION_ID_NAMESPACE: uuid.UUID = uuid.UUID("b6348ee2-861d-4a42-a8ae-8135204f5c40")
"""Namespace used to derive immutable publication identities."""


class SourceLifecycleState(StrEnum):
    """Lifecycle states of an Iceberg source registration."""

    DRAFT = "DRAFT"
    ACTIVE = "ACTIVE"
    PAUSED = "PAUSED"


class DatasetLifecycleState(StrEnum):
    """Lifecycle states of one logical dataset."""

    ACTIVE = "ACTIVE"
    PAUSED = "PAUSED"
    BLOCKED = "BLOCKED"


class SourceSnapshotKind(StrEnum):
    """Kinds of accepted or durably rejected Iceberg snapshots."""

    BASELINE = "BASELINE"
    APPEND = "APPEND"
    TRUSTED_MAINTENANCE = "TRUSTED_MAINTENANCE"
    REJECTED = "REJECTED"


class SourceSnapshotState(StrEnum):
    """Durable source-snapshot states."""

    SEALED = "SEALED"
    COMPLETE = "COMPLETE"
    BLOCKED = "BLOCKED"


class WorkKind(StrEnum):
    """Kinds of work serialized within one dataset lane."""

    INGEST = "INGEST"
    PUBLISH = "PUBLISH"
    REBUILD = "REBUILD"


class WorkState(StrEnum):
    """Durable dataset-work states."""

    PENDING = "PENDING"
    RUNNING = "RUNNING"
    RETRY_WAIT = "RETRY_WAIT"
    SUCCEEDED = "SUCCEEDED"
    BLOCKED = "BLOCKED"


class WorkPhase(StrEnum):
    """Checkpointed phases of dataset work."""

    INGEST = "INGEST"
    COMPACT = "COMPACT"
    INDEX = "INDEX"
    VALIDATE = "VALIDATE"
    PREWARM = "PREWARM"
    PUBLISH = "PUBLISH"


class WorkLauncherKind(StrEnum):
    """Launch origins recorded on the latest durable work claim."""

    LOCAL = "LOCAL"
    AIRFLOW = "AIRFLOW"


@dataclass(frozen=True, slots=True)
class WorkProvenance:
    """Optional launch provenance that never participates in work ordering."""

    launcher_kind: WorkLauncherKind = WorkLauncherKind.LOCAL
    airflow_ctx_dag_id: str | None = None
    airflow_ctx_dag_run_id: str | None = None
    airflow_ctx_task_id: str | None = None
    airflow_ctx_map_index: int | None = None
    airflow_ctx_try_number: int | None = None

    @classmethod
    def from_environment(cls, environment: Mapping[str, str]) -> WorkProvenance:
        """Parse standard Airflow context variables once at process startup.

        Args:
            environment: Process environment mapping.

        Returns:
            Validated local or Airflow launch provenance.

        Raises:
            ValueError: If integer context values are malformed or context is partial.
        """
        dag_id: str | None = environment.get("AIRFLOW_CTX_DAG_ID") or None
        dag_run_id: str | None = environment.get("AIRFLOW_CTX_DAG_RUN_ID") or None
        task_id: str | None = environment.get("AIRFLOW_CTX_TASK_ID") or None
        raw_map_index: str | None = environment.get("AIRFLOW_CTX_MAP_INDEX") or None
        raw_try_number: str | None = environment.get("AIRFLOW_CTX_TRY_NUMBER") or None
        map_index: int | None = int(raw_map_index) if raw_map_index is not None else None
        try_number: int | None = int(raw_try_number) if raw_try_number is not None else None
        airflow_selected: bool = any(
            value is not None for value in (dag_id, dag_run_id, task_id, map_index, try_number)
        )
        return cls(
            launcher_kind=WorkLauncherKind.AIRFLOW if airflow_selected else WorkLauncherKind.LOCAL,
            airflow_ctx_dag_id=dag_id,
            airflow_ctx_dag_run_id=dag_run_id,
            airflow_ctx_task_id=task_id,
            airflow_ctx_map_index=map_index,
            airflow_ctx_try_number=try_number,
        ).validate()

    def validate(self) -> WorkProvenance:
        """Validate local emptiness or complete bounded Airflow identity.

        Returns:
            This validated provenance.

        Raises:
            ValueError: If the launcher kind and context values disagree.
        """
        context: tuple[str | int | None, ...] = (
            self.airflow_ctx_dag_id,
            self.airflow_ctx_dag_run_id,
            self.airflow_ctx_task_id,
            self.airflow_ctx_map_index,
            self.airflow_ctx_try_number,
        )
        if self.launcher_kind is WorkLauncherKind.LOCAL:
            if any(value is not None for value in context):
                raise ValueError("LOCAL work provenance cannot carry Airflow context")
            return self
        if self.launcher_kind is not WorkLauncherKind.AIRFLOW:
            raise ValueError("work launcher kind is unsupported")
        required: tuple[str | None, ...] = (
            self.airflow_ctx_dag_id,
            self.airflow_ctx_dag_run_id,
            self.airflow_ctx_task_id,
        )
        if any(value is None or not value.strip() for value in required):
            raise ValueError("AIRFLOW work provenance requires DAG, run, and task identities")
        if len(self.airflow_ctx_dag_id or "") > 250 or len(self.airflow_ctx_task_id or "") > 250:
            raise ValueError("Airflow DAG and task identities must contain at most 250 characters")
        if len(self.airflow_ctx_dag_run_id or "") > 512:
            raise ValueError("Airflow run identity must contain at most 512 characters")
        if self.airflow_ctx_map_index is not None and self.airflow_ctx_map_index < -1:
            raise ValueError("Airflow map index must be at least -1")
        if self.airflow_ctx_try_number is not None and self.airflow_ctx_try_number < 1:
            raise ValueError("Airflow try number must be positive")
        return self


def validate_routing_segment(value: str, field_name: str) -> str:
    """Validate one shared serving-route segment.

    Args:
        value: Candidate segment.
        field_name: Field name used in the error.

    Returns:
        Validated value unchanged.

    Raises:
        ValueError: If the value is not a bounded allowlisted segment.
    """
    if ROUTING_SEGMENT_PATTERN.fullmatch(value) is None:
        raise ValueError(f"{field_name} must match [A-Za-z0-9_-]{{1,128}}")
    return value


@dataclass(frozen=True, slots=True)
class RoutingIdentity:
    """Authenticated serving identity of one logical dataset."""

    tenant_id: str
    namespace: str
    org_id: str

    def validate(self) -> RoutingIdentity:
        """Validate every routing segment.

        Returns:
            This validated identity.
        """
        validate_routing_segment(self.tenant_id, "tenant_id")
        validate_routing_segment(self.namespace, "namespace")
        validate_routing_segment(self.org_id, "org_id")
        return self

    def canonical(self) -> str:
        """Return an unambiguous representation for deterministic IDs.

        Returns:
            Length-prefixed identity string.
        """
        self.validate()
        return "".join(f"{len(value)}:{value}" for value in (self.tenant_id, self.namespace, self.org_id))


@dataclass(frozen=True, slots=True)
class IcebergSource:
    """Database-owned Iceberg source and projection configuration."""

    source_id: uuid.UUID
    source_name: str
    spark_catalog: str
    table_namespace: str
    table_name: str
    table_uuid: uuid.UUID
    lance_base_uri: str
    lifecycle_state: SourceLifecycleState
    default_spec_id: uuid.UUID
    canonical_baseline_snapshot_id: int | None
    replay_horizon: timedelta
    tenant_column: str = "tenant_id"
    namespace_column: str = "namespace"
    org_column: str = "org_id"
    record_id_column: str = "vector_id"
    operation_column: str = "op"
    event_time_column: str = "event_timestamp"
    vectors_column: str = "vectors"
    texts_column: str = "texts"
    metadata_column: str = "metadata"
    ttl_column: str | None = "ttl"

    @property
    def spark_table(self) -> str:
        """Return the fully qualified Spark table identifier.

        Returns:
            Catalog, namespace, and table joined with dots.
        """
        return f"{self.spark_catalog}.{self.table_namespace}.{self.table_name}"

    def validate(self) -> IcebergSource:
        """Validate database-owned source identity and projection names.

        Returns:
            This validated source registration.
        """
        if SOURCE_NAME_PATTERN.fullmatch(self.source_name) is None:
            raise ValueError("source_name is invalid")
        if IDENTIFIER_PATTERN.fullmatch(self.spark_catalog) is None:
            raise ValueError("spark_catalog is invalid")
        if TABLE_NAMESPACE_PATTERN.fullmatch(self.table_namespace) is None:
            raise ValueError("table_namespace is invalid")
        if IDENTIFIER_PATTERN.fullmatch(self.table_name) is None:
            raise ValueError("table_name is invalid")
        if not self.lance_base_uri.strip():
            raise ValueError("lance_base_uri must be non-empty")
        if self.canonical_baseline_snapshot_id is not None and self.canonical_baseline_snapshot_id < 0:
            raise ValueError("canonical_baseline_snapshot_id must be non-negative")
        if self.replay_horizon <= timedelta(0):
            raise ValueError("replay_horizon must be positive")
        columns: tuple[str, ...] = (
            self.tenant_column,
            self.namespace_column,
            self.org_column,
            self.record_id_column,
            self.operation_column,
            self.event_time_column,
            self.vectors_column,
            self.texts_column,
            self.metadata_column,
        )
        if any(IDENTIFIER_PATTERN.fullmatch(value) is None for value in columns):
            raise ValueError("source projection columns must be valid identifiers")
        if self.ttl_column is not None and IDENTIFIER_PATTERN.fullmatch(self.ttl_column) is None:
            raise ValueError("ttl_column must be a valid identifier")
        return self


@dataclass(frozen=True, slots=True)
class DatasetPlan:
    """One logical dataset discovered in an exact Iceberg snapshot."""

    identity: RoutingIdentity

    def validate(self) -> DatasetPlan:
        """Validate the discovered route.

        Returns:
            This validated plan.
        """
        self.identity.validate()
        return self


@dataclass(frozen=True, slots=True)
class SourceSnapshotPlan:
    """Immutable source snapshot metadata sealed by the serial planner."""

    source_id: uuid.UUID
    snapshot_id: int
    parent_snapshot_id: int | None
    iceberg_sequence_number: int
    partition_spec_id: int
    committed_at: datetime
    iceberg_operation: str
    kind: SourceSnapshotKind

    def validate(self) -> SourceSnapshotPlan:
        """Validate snapshot identity and lineage.

        Returns:
            This validated source snapshot.

        Raises:
            ValueError: If identifiers, operation, or lineage are invalid.
        """
        if min(self.snapshot_id, self.iceberg_sequence_number, self.partition_spec_id) < 0:
            raise ValueError("snapshot, sequence, and partition specification IDs must be non-negative")
        if self.parent_snapshot_id is not None and self.parent_snapshot_id < 0:
            raise ValueError("parent_snapshot_id must be non-negative")
        if (
            self.kind not in (SourceSnapshotKind.BASELINE, SourceSnapshotKind.REJECTED)
            and self.parent_snapshot_id is None
        ):
            raise ValueError("only BASELINE or REJECTED snapshots may omit their parent")
        if not self.iceberg_operation.strip():
            raise ValueError("iceberg_operation must be non-empty")
        if self.committed_at.tzinfo is None:
            raise ValueError("committed_at must be timezone-aware")
        return self


def deterministic_dataset_id(source_id: uuid.UUID, identity: RoutingIdentity) -> uuid.UUID:
    """Derive one opaque dataset ID.

    Args:
        source_id: Owning source identity.
        identity: Logical route.

    Returns:
        Stable source-scoped UUID.
    """
    return uuid.uuid5(DATASET_ID_NAMESPACE, f"{source_id}:{identity.canonical()}")


def deterministic_ingest_work_id(dataset_id: uuid.UUID, source_snapshot_seq: int) -> uuid.UUID:
    """Derive the idempotent INGEST work identity.

    Args:
        dataset_id: Owning dataset.
        source_snapshot_seq: Positive control-plane source sequence.

    Returns:
        Stable work UUID.
    """
    if source_snapshot_seq < 1:
        raise ValueError("source_snapshot_seq must be positive")
    return uuid.uuid5(WORK_ID_NAMESPACE, f"INGEST:{dataset_id}:{source_snapshot_seq}")


def deterministic_publish_work_id(
    dataset_id: uuid.UUID,
    source_snapshot_seq: int,
    spec_revision_id: uuid.UUID,
) -> uuid.UUID:
    """Derive the coalescible PUBLISH work identity.

    Args:
        dataset_id: Owning dataset.
        source_snapshot_seq: Latest source state included in the publication.
        spec_revision_id: Frozen configuration revision.

    Returns:
        Stable work UUID.
    """
    return uuid.uuid5(WORK_ID_NAMESPACE, f"PUBLISH:{dataset_id}:{source_snapshot_seq}:{spec_revision_id}")


def deterministic_rebuild_work_id(dataset_id: uuid.UUID, request_id: uuid.UUID) -> uuid.UUID:
    """Derive one operator-requested REBUILD work identity.

    Args:
        dataset_id: Owning dataset.
        request_id: Operator idempotency identity.

    Returns:
        Stable work UUID.
    """
    return uuid.uuid5(WORK_ID_NAMESPACE, f"REBUILD:{dataset_id}:{request_id}")


def deterministic_publication_id(work_id: uuid.UUID) -> uuid.UUID:
    """Derive the immutable publication identity for one work generation.

    Args:
        work_id: Publication-producing work identity.

    Returns:
        Stable publication UUID.
    """
    return uuid.uuid5(PUBLICATION_ID_NAMESPACE, str(work_id))


def ingest_uri(base_uri: str, dataset_id: uuid.UUID) -> str:
    """Build the mutable ingest dataset URI.

    Args:
        base_uri: Non-empty Lance root.
        dataset_id: Opaque dataset identity.

    Returns:
        Dataset URI containing no route values.
    """
    normalized: str = base_uri.rstrip("/")
    if not normalized:
        raise ValueError("base_uri must be non-empty")
    return f"{normalized}/{dataset_id}.lance"


def rebuild_uri(base_uri: str, dataset_id: uuid.UUID, work_id: uuid.UUID) -> str:
    """Build one isolated rebuild candidate URI.

    Args:
        base_uri: Non-empty Lance root.
        dataset_id: Opaque dataset identity.
        work_id: Rebuild work identity.

    Returns:
        Candidate URI containing no route values.
    """
    normalized: str = base_uri.rstrip("/")
    if not normalized:
        raise ValueError("base_uri must be non-empty")
    return f"{normalized}/rebuild/{dataset_id}/{work_id}.lance"


@dataclass(frozen=True, slots=True)
class WorkClaim:
    """Fenced lease returned to one local worker."""

    work_id: uuid.UUID
    dataset_id: uuid.UUID
    kind: WorkKind
    phase: WorkPhase
    lease_token: uuid.UUID
    fence_epoch: int
    attempt_count: int
    source_snapshot_seq: int
    spec_revision_id: uuid.UUID
    ingest_lance_uri: str
    ingest_lance_version: int | None
    lease_expires_at: datetime | None = None


@dataclass(frozen=True, slots=True)
class WorkExecutionContext:
    """Immutable source, dataset, and specification context for one claim."""

    claim: WorkClaim
    identity: RoutingIdentity
    source_table: str
    source: IcebergSource
    spec_revision: DatasetSpecRevision
    snapshot_id: int | None
    parent_snapshot_id: int | None
    iceberg_sequence_number: int | None
    partition_spec_id: int | None
    source_snapshot_kind: SourceSnapshotKind | None
    candidate_lance_uri: str | None
    candidate_lance_version: int | None
    artifact_manifest_uri: str | None
    artifact_digest: bytes | None


@dataclass(frozen=True, slots=True)
class PublicationIndexEvidence:
    """Exact coverage evidence for one configured index."""

    index_definition_id: uuid.UUID
    actual_index_type: IndexType
    indexed_fragment_count: int
    unindexed_fragment_count: int
    artifact_generation_digest: bytes | None = None

    def validate(self) -> PublicationIndexEvidence:
        """Validate coverage counts and optional artifact digest.

        Returns:
            This validated evidence.
        """
        if min(self.indexed_fragment_count, self.unindexed_fragment_count) < 0:
            raise ValueError("publication index fragment counts must be non-negative")
        if self.unindexed_fragment_count != 0:
            raise ValueError("a published index must cover every fragment")
        if self.artifact_generation_digest is not None and len(self.artifact_generation_digest) != 32:
            raise ValueError("artifact_generation_digest must contain 32 bytes")
        return self


@dataclass(frozen=True, slots=True)
class PublicationEvidence:
    """Exact schema, cardinality, fragment, and index evidence for publication."""

    schema_digest: bytes
    total_row_count: int
    distinct_row_count: int
    live_row_count: int
    distinct_live_row_count: int
    fragment_count: int
    indexes: tuple[PublicationIndexEvidence, ...]

    def validate(self) -> PublicationEvidence:
        """Validate publication qualification evidence.

        Returns:
            This validated evidence.
        """
        if len(self.schema_digest) != 32:
            raise ValueError("schema_digest must contain 32 bytes")
        counts: tuple[int, ...] = (
            self.total_row_count,
            self.distinct_row_count,
            self.live_row_count,
            self.distinct_live_row_count,
            self.fragment_count,
        )
        if min(counts) < 0:
            raise ValueError("publication counts must be non-negative")
        if self.distinct_row_count != self.total_row_count:
            raise ValueError("publication rows must be distinct")
        if self.live_row_count > self.total_row_count:
            raise ValueError("publication live rows exceed total rows")
        if self.distinct_live_row_count != self.live_row_count:
            raise ValueError("publication live rows must be distinct")
        seen: set[uuid.UUID] = set()
        evidence: PublicationIndexEvidence
        for evidence in self.indexes:
            evidence.validate()
            if evidence.index_definition_id in seen:
                raise ValueError("publication evidence repeats an index definition")
            seen.add(evidence.index_definition_id)
        return self


@dataclass(frozen=True, slots=True)
class ControlPlaneStatus:
    """Bounded operational snapshot used by local status and SLO reporting."""

    pending_work: int
    running_work: int
    retry_wait_work: int
    blocked_work: int
    due_work: int
    blocked_source_snapshots: int
    oldest_open_work_at: datetime | None
    retention_source_snapshot_seq: int | None
    retention_snapshot_id: int | None
    retention_parent_snapshot_id: int | None
    retention_state: SourceSnapshotState | None
    retention_created_at: datetime | None


@dataclass(frozen=True, slots=True)
class ServingDataset:
    """Exact active serving-catalog result."""

    dataset_id: uuid.UUID
    identity: RoutingIdentity
    lance_uri: str
    lance_version: int
    publication_id: uuid.UUID


@dataclass(frozen=True, slots=True)
class PublicationCleanup:
    """Retired publication whose external artifacts may be removed."""

    publication_id: uuid.UUID
    work_id: uuid.UUID
    dataset_id: uuid.UUID
    lance_uri: str
    lance_version: int
    manifest_uri: str
    pin_name: str
