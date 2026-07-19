"""Transactional PostgreSQL repository for the local dataset control plane."""

from __future__ import annotations

import re
import uuid
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql
from sqlalchemy.engine import Connection, Engine, RowMapping

from lance_etl.publication.manifest import candidate_pin_name
from lance_etl.state.settings import ReconcilerSettings
from lance_etl.state.specs import (
    DEFAULT_SPEC_ID,
    DatasetField,
    DatasetSpecRevision,
    FtsIndexOptions,
    IndexDefinition,
    IndexType,
    SpecRevisionState,
    VectorIndexOptions,
    decode_dataset_spec_revision,
)
from lance_etl.state.tables import (
    dataset_fields,
    dataset_publications,
    dataset_spec_revisions,
    dataset_specs,
    dataset_state,
    dataset_work,
    datasets,
    fts_index_options,
    iceberg_sources,
    index_definitions,
    publication_indexes,
    reconciler_settings,
    source_snapshots,
    vector_index_options,
)
from lance_etl.state.types import (
    ControlPlaneStatus,
    DatasetLifecycleState,
    DatasetPlan,
    IcebergSource,
    PublicationCleanup,
    PublicationEvidence,
    RoutingIdentity,
    ServingDataset,
    SourceLifecycleState,
    SourceSnapshotKind,
    SourceSnapshotPlan,
    SourceSnapshotState,
    WorkClaim,
    WorkExecutionContext,
    WorkKind,
    WorkPhase,
    WorkProvenance,
    WorkState,
    deterministic_dataset_id,
    deterministic_ingest_work_id,
    deterministic_publication_id,
    deterministic_publish_work_id,
    deterministic_rebuild_work_id,
    ingest_uri,
    rebuild_uri,
)

ERROR_CODE_LIMIT: int = 128
"""Maximum persisted error-code length."""

ERROR_MESSAGE_LIMIT: int = 2000
"""Maximum persisted diagnostic length."""

SPEC_NAME_PATTERN: re.Pattern[str] = re.compile(r"^[A-Za-z][A-Za-z0-9_-]{0,127}$")
"""Exact PostgreSQL-compatible dataset specification name pattern."""

PHASE_ORDER: dict[WorkPhase, int] = {
    WorkPhase.INGEST: 0,
    WorkPhase.COMPACT: 1,
    WorkPhase.INDEX: 2,
    WorkPhase.VALIDATE: 3,
    WorkPhase.PREWARM: 4,
    WorkPhase.PUBLISH: 5,
}
"""Forward-only durable phase order."""


class StateTransitionError(RuntimeError):
    """Raised when durable state contradicts a requested transition."""


def utc_now() -> datetime:
    """Return the current timezone-aware UTC instant.

    Returns:
        Current UTC time.
    """
    return datetime.now(UTC)


def bounded_error(value: str | None, limit: int) -> str | None:
    """Bound an optional diagnostic for its database column.

    Args:
        value: Optional diagnostic.
        limit: Maximum character count.

    Returns:
        Original or truncated value.
    """
    return value[:limit] if value is not None else None


def build_control_plane_engine(database_url: str) -> Engine:
    """Build the direct psycopg-backed SQLAlchemy engine.

    Args:
        database_url: PostgreSQL URL using psycopg 3.

    Returns:
        Pool-pre-ping SQLAlchemy engine.

    Raises:
        ValueError: If the URL does not select PostgreSQL with psycopg 3.
    """
    engine: Engine = sa.create_engine(database_url, pool_pre_ping=True)
    if engine.dialect.name != "postgresql" or engine.dialect.driver != "psycopg":
        engine.dispose()
        raise ValueError("control plane requires a postgresql+psycopg database URL")
    return engine


def parse_source_table(source_table: str) -> tuple[str, str, str]:
    """Split a fully qualified Spark table identifier.

    Args:
        source_table: Catalog, namespace, and table separated by dots.

    Returns:
        Catalog, namespace, and table components.

    Raises:
        ValueError: If fewer than three components are present.
    """
    components: list[str] = source_table.split(".")
    if len(components) < 3 or any(not component for component in components):
        raise ValueError("source_table must contain catalog, namespace, and table")
    return components[0], ".".join(components[1:-1]), components[-1]


def source_from_row(row: Mapping[str, Any]) -> IcebergSource:
    """Decode one Iceberg source row.

    Args:
        row: Source table result mapping.

    Returns:
        Typed source configuration.
    """
    return IcebergSource(
        source_id=row["source_id"],
        source_name=str(row["source_name"]),
        spark_catalog=str(row["spark_catalog"]),
        table_namespace=str(row["table_namespace"]),
        table_name=str(row["table_name"]),
        table_uuid=row["table_uuid"],
        lifecycle_state=SourceLifecycleState(str(row["lifecycle_state"])),
        default_spec_id=row["default_spec_id"],
        lance_base_uri=str(row["lance_base_uri"]),
        canonical_baseline_snapshot_id=row["canonical_baseline_snapshot_id"],
        replay_horizon=timedelta(seconds=int(row["replay_horizon_seconds"])),
        tenant_column=str(row["tenant_column"]),
        namespace_column=str(row["namespace_column"]),
        org_column=str(row["org_column"]),
        record_id_column=str(row["record_id_column"]),
        operation_column=str(row["operation_column"]),
        event_time_column=str(row["event_time_column"]),
        vectors_column=str(row["vectors_column"]),
        texts_column=str(row["texts_column"]),
        metadata_column=str(row["metadata_column"]),
        ttl_column=row["ttl_column"],
    ).validate()


def source_registration_matches(
    source: IcebergSource,
    source_table: str,
    table_uuid: uuid.UUID,
    canonical_baseline_snapshot_id: int | None,
    lance_base_uri: str,
) -> bool:
    """Check immutable bootstrap values against a database-owned source.

    Args:
        source: Persisted source.
        source_table: Bootstrap Spark identifier.
        table_uuid: Bootstrap Iceberg table UUID.
        canonical_baseline_snapshot_id: Bootstrap baseline pin.
        lance_base_uri: Bootstrap Lance storage root.

    Returns:
        Whether immutable source values match.
    """
    return (
        source.spark_table == source_table
        and source.table_uuid == table_uuid
        and (
            canonical_baseline_snapshot_id is None
            or source.canonical_baseline_snapshot_id == canonical_baseline_snapshot_id
        )
        and source.lance_base_uri.rstrip("/") == lance_base_uri.rstrip("/")
    )


def dataset_uri_matches_root(base_uri: str, dataset_id: uuid.UUID, candidate_uri: str) -> bool:
    """Validate an ingest or rebuild URI owned by one dataset.

    Args:
        base_uri: Repository-owned Lance root.
        dataset_id: Opaque dataset identity.
        candidate_uri: Candidate dataset URI.

    Returns:
        Whether the URI uses one of the code-owned layouts.
    """
    if candidate_uri == ingest_uri(base_uri, dataset_id):
        return True
    prefix: str = f"{base_uri.rstrip('/')}/rebuild/{dataset_id}/"
    if not candidate_uri.startswith(prefix) or not candidate_uri.endswith(".lance"):
        return False
    work_segment: str = candidate_uri[len(prefix) : -len(".lance")]
    if not work_segment or "/" in work_segment:
        return False
    try:
        return str(uuid.UUID(work_segment)) == work_segment
    except ValueError:
        return False


def validate_publication_request(
    claim: WorkClaim,
    candidate_lance_version: int,
    artifact_manifest_uri: str,
    artifact_digest: bytes,
    evidence: PublicationEvidence,
) -> None:
    """Validate publication values before opening a transaction.

    Args:
        claim: Candidate publication claim.
        candidate_lance_version: Exact Lance version.
        artifact_manifest_uri: Immutable manifest URI.
        artifact_digest: Manifest digest.
        evidence: Qualification evidence.

    Raises:
        ValueError: If required publication values are invalid.
    """
    if claim.kind not in (WorkKind.PUBLISH, WorkKind.REBUILD):
        raise ValueError("publication requires PUBLISH or REBUILD work")
    if candidate_lance_version < 0 or not artifact_manifest_uri or len(artifact_digest) != 32:
        raise ValueError("publication requires exact version and immutable 32-byte artifact evidence")
    evidence.validate()


def validate_database_decimal(value: float, field_name: str) -> None:
    """Require a floating setting to fit PostgreSQL's four-decimal scale exactly.

    Args:
        value: Validated finite domain value.
        field_name: Diagnostic setting name.

    Raises:
        ValueError: If PostgreSQL would round the value during insertion.
    """
    decimal_value: Decimal = Decimal(str(value))
    if decimal_value != decimal_value.quantize(Decimal("0.0001")):
        raise ValueError(f"{field_name} must have at most four decimal places")


def prepared_draft_revision(revision: DatasetSpecRevision) -> DatasetSpecRevision:
    """Recompute and validate one complete draft before persistence.

    Args:
        revision: Typed candidate graph whose supplied digest is not trusted.

    Returns:
        Complete validated draft carrying its recomputed semantic digest.

    Raises:
        ValueError: If the graph is not DRAFT or cannot round-trip through PostgreSQL numeric columns.
    """
    if revision.state is not SpecRevisionState.DRAFT:
        raise ValueError("new specification revisions must be DRAFT")
    validate_database_decimal(revision.materialize_deletions_threshold, "materialize_deletions_threshold")
    index: IndexDefinition
    for index in revision.indexes:
        if index.vector_options is not None:
            validate_database_decimal(index.vector_options.retrain_growth_factor, "retrain_growth_factor")
    candidate: DatasetSpecRevision = replace(
        revision,
        configuration_digest=revision.expected_configuration_digest(),
    )
    return candidate.validate()


def revision_row_values(revision: DatasetSpecRevision) -> dict[str, object]:
    """Encode one validated revision parent row.

    Args:
        revision: Complete validated revision.

    Returns:
        SQLAlchemy insert values for ``dataset_spec_revisions``.
    """
    return {
        "spec_revision_id": revision.spec_revision_id,
        "spec_id": revision.spec_id,
        "revision_number": revision.revision_number,
        "state": revision.state.value,
        "supersedes_revision_id": revision.supersedes_revision_id,
        "configuration_digest": revision.configuration_digest,
        "ingest_shuffle_partitions": revision.ingest_shuffle_partitions,
        "merge_rows_per_chunk": revision.merge_rows_per_chunk,
        "merge_batch_bytes": revision.merge_batch_bytes,
        "write_rows_per_fragment": revision.write_rows_per_fragment,
        "compaction_enabled": revision.compaction_enabled,
        "compaction_mode": revision.compaction_mode.value,
        "target_rows_per_fragment": revision.target_rows_per_fragment,
        "max_source_fragments": revision.max_source_fragments,
        "compaction_threads": revision.compaction_threads,
        "defer_index_remap": revision.defer_index_remap,
        "materialize_deletions": revision.materialize_deletions,
        "materialize_deletions_threshold": revision.materialize_deletions_threshold,
        "cleanup_older_than_seconds": revision.cleanup_older_than_seconds,
        "retain_versions": revision.retain_versions,
        "fragments_per_index_task": revision.fragments_per_index_task,
        "max_index_deltas": revision.max_index_deltas,
        "max_stale_replans": revision.max_stale_replans,
        "prewarm_required": revision.prewarm_required,
        "retained_publications": revision.retained_publications,
        "artifact_retention_seconds": revision.artifact_retention_seconds,
    }


def field_row_values(field_value: DatasetField) -> dict[str, object]:
    """Encode one normalized dataset field.

    Args:
        field_value: Validated field owned by the draft.

    Returns:
        SQLAlchemy insert values for ``dataset_fields``.
    """
    return {
        "field_id": field_value.field_id,
        "spec_revision_id": field_value.spec_revision_id,
        "ordinal": field_value.ordinal,
        "target_name": field_value.target_name,
        "role": field_value.role.value,
        "source_kind": field_value.source_kind.value,
        "source_column": field_value.source_column,
        "source_key": field_value.source_key,
        "data_type": field_value.data_type,
        "nullable": field_value.nullable,
        "required_on_upsert": field_value.required_on_upsert,
        "vector_dimension": field_value.vector_dimension,
    }


def index_row_values(index: IndexDefinition) -> dict[str, object]:
    """Encode one normalized index definition.

    Args:
        index: Validated index owned by the draft.

    Returns:
        SQLAlchemy insert values for ``index_definitions``.
    """
    return {
        "index_definition_id": index.index_definition_id,
        "spec_revision_id": index.spec_revision_id,
        "field_id": index.field_id,
        "ordinal": index.ordinal,
        "index_name": index.index_name,
        "index_type": index.index_type.value,
    }


def vector_option_row_values(index: IndexDefinition) -> dict[str, object]:
    """Encode IVF_RQ options from one validated index.

    Args:
        index: IVF_RQ index carrying vector options.

    Returns:
        SQLAlchemy insert values for ``vector_index_options``.

    Raises:
        ValueError: If the index lacks vector options.
    """
    options: VectorIndexOptions | None = index.vector_options
    if options is None:
        raise ValueError("IVF_RQ index lacks vector options")
    return {
        "index_definition_id": index.index_definition_id,
        "index_type": index.index_type.value,
        "metric": options.metric.value,
        "num_partitions": options.num_partitions,
        "minimum_partitions": options.minimum_partitions,
        "maximum_partitions": options.maximum_partitions,
        "target_rows_per_partition": options.target_rows_per_partition,
        "minimum_rows": options.minimum_rows,
        "num_bits": options.num_bits,
        "streaming_sample_rate": options.streaming_sample_rate,
        "streaming_refine_passes": options.streaming_refine_passes,
        "retrain_growth_factor": options.retrain_growth_factor,
    }


def fts_option_row_values(index: IndexDefinition) -> dict[str, object]:
    """Encode INVERTED options from one validated index.

    Args:
        index: INVERTED index carrying full-text options.

    Returns:
        SQLAlchemy insert values for ``fts_index_options``.

    Raises:
        ValueError: If the index lacks full-text options.
    """
    options: FtsIndexOptions | None = index.fts_options
    if options is None:
        raise ValueError("INVERTED index lacks full-text options")
    return {
        "index_definition_id": index.index_definition_id,
        "index_type": index.index_type.value,
        "with_position": options.with_position,
        "base_tokenizer": options.base_tokenizer,
        "language": options.language,
        "max_unindexed_fragments": options.max_unindexed_fragments,
    }


@dataclass(frozen=True)
class ControlPlaneRepository:
    """Own visible transactions and fenced dataset-work transitions."""

    engine: Engine

    def reconciler_settings(self) -> ReconcilerSettings:
        """Load the singleton operational settings.

        Returns:
            Validated local reconciler settings.

        Raises:
            StateTransitionError: If the singleton is missing.
        """
        with self.engine.connect() as connection:
            row: RowMapping | None = (
                connection.execute(sa.select(reconciler_settings).where(reconciler_settings.c.singleton_id == 1))
                .mappings()
                .one_or_none()
            )
        if row is None:
            raise StateTransitionError("reconciler settings singleton is missing")
        return ReconcilerSettings(
            poll_interval=timedelta(seconds=int(row["poll_interval_seconds"])),
            claim_batch_size=int(row["claim_batch_size"]),
            max_drain_batches=int(row["max_drain_batches"]),
            max_snapshots_per_plan=int(row["max_snapshots_per_plan"]),
            lease_duration=timedelta(seconds=int(row["lease_duration_seconds"])),
            lease_heartbeat_interval=timedelta(seconds=int(row["lease_heartbeat_seconds"])),
            retry_base_delay=timedelta(seconds=int(row["retry_base_delay_seconds"])),
            retry_max_delay=timedelta(seconds=int(row["retry_max_delay_seconds"])),
            max_attempts=int(row["max_attempts"]),
            max_due_work=int(row["max_due_work"]),
            max_open_work_age=timedelta(seconds=int(row["max_open_work_age_seconds"])),
            max_retention_age=timedelta(seconds=int(row["max_retention_age_seconds"])),
            audit_retention=timedelta(seconds=int(row["audit_retention_seconds"])),
            cleanup_batch_size=int(row["cleanup_batch_size"]),
        ).validate()

    def source_by_name(self, source_name: str) -> IcebergSource | None:
        """Resolve one registered source by its stable name.

        Args:
            source_name: Exact source name.

        Returns:
            Typed source or ``None``.
        """
        with self.engine.connect() as connection:
            row: RowMapping | None = (
                connection.execute(sa.select(iceberg_sources).where(iceberg_sources.c.source_name == source_name))
                .mappings()
                .one_or_none()
            )
        return source_from_row(row) if row is not None else None

    def ensure_source_registration(
        self,
        source_name: str,
        source_table: str,
        table_uuid: uuid.UUID,
        canonical_baseline_snapshot_id: int | None,
        lance_base_uri: str,
    ) -> IcebergSource:
        """Create the first source registration or reject bootstrap drift.

        Args:
            source_name: Stable local source name.
            source_table: Fully qualified Spark table.
            table_uuid: Iceberg table UUID read from metadata.
            canonical_baseline_snapshot_id: Optional immutable baseline pin.
            lance_base_uri: First-run Lance root that must match this repository.

        Returns:
            Database-owned source configuration.

        Raises:
            StateTransitionError: If immutable bootstrap inputs drift.
        """
        if not lance_base_uri.rstrip("/"):
            raise ValueError("lance_base_uri must be non-empty")
        catalog: str
        namespace: str
        table_name: str
        catalog, namespace, table_name = parse_source_table(source_table)
        with self.engine.begin() as connection:
            spec_id: uuid.UUID | None = connection.scalar(
                sa.select(dataset_spec_revisions.c.spec_id)
                .where(
                    dataset_spec_revisions.c.spec_id == DEFAULT_SPEC_ID,
                    dataset_spec_revisions.c.state == "ACTIVE",
                )
                .limit(1)
            )
            if spec_id is None:
                raise StateTransitionError("no active dataset specification exists")
            source_id: uuid.UUID = uuid.uuid5(uuid.NAMESPACE_URL, f"lance-etl:{source_name}")
            connection.execute(
                postgresql.insert(iceberg_sources)
                .values(
                    source_id=source_id,
                    source_name=source_name,
                    spark_catalog=catalog,
                    table_namespace=namespace,
                    table_name=table_name,
                    table_uuid=table_uuid,
                    lifecycle_state=SourceLifecycleState.ACTIVE.value,
                    default_spec_id=spec_id,
                    lance_base_uri=lance_base_uri.rstrip("/"),
                    canonical_baseline_snapshot_id=canonical_baseline_snapshot_id,
                )
                .on_conflict_do_nothing(index_elements=[iceberg_sources.c.source_name])
            )
            row: RowMapping = (
                connection.execute(
                    sa.select(iceberg_sources).where(iceberg_sources.c.source_name == source_name).with_for_update()
                )
                .mappings()
                .one()
            )
            source: IcebergSource = source_from_row(row)
            if not source_registration_matches(
                source,
                source_table,
                table_uuid,
                canonical_baseline_snapshot_id,
                lance_base_uri,
            ):
                raise StateTransitionError("source bootstrap values differ from PostgreSQL truth")
            return source

    def create_spec(self, name: str, description: str | None = None, spec_id: uuid.UUID | None = None) -> uuid.UUID:
        """Create one named dataset specification identity.

        Args:
            name: Unique bounded specification name.
            description: Optional operator-facing description.
            spec_id: Optional caller-owned identity for deterministic imports.

        Returns:
            Existing or newly created specification identity.

        Raises:
            ValueError: If the values cannot satisfy the database contract.
            StateTransitionError: If a replayed name carries different immutable values.
        """
        if SPEC_NAME_PATTERN.fullmatch(name) is None:
            raise ValueError("specification name must match [A-Za-z][A-Za-z0-9_-]{0,127}")
        if description is not None and not description.strip():
            raise ValueError("specification description must be nonblank when present")
        identity: uuid.UUID = spec_id or uuid.uuid5(uuid.NAMESPACE_URL, f"lance-etl:dataset-spec:{name}")
        if identity.int == 0:
            raise ValueError("specification identity must be non-nil")
        with self.engine.begin() as connection:
            connection.execute(
                postgresql.insert(dataset_specs)
                .values(spec_id=identity, name=name, description=description)
                .on_conflict_do_nothing(index_elements=[dataset_specs.c.name])
            )
            row: RowMapping = (
                connection.execute(sa.select(dataset_specs).where(dataset_specs.c.name == name).with_for_update())
                .mappings()
                .one()
            )
            actual: tuple[uuid.UUID, str | None] = (row["spec_id"], row["description"])
            expected: tuple[uuid.UUID, str | None] = (identity, description)
            if actual != expected:
                raise StateTransitionError("specification name was replayed with different immutable values")
            return identity

    def create_draft_spec_revision(self, revision: DatasetSpecRevision) -> DatasetSpecRevision:
        """Atomically persist a complete normalized DRAFT specification graph.

        Args:
            revision: Typed parent, fields, indexes, and options whose digest is recomputed.

        Returns:
            Persisted validated draft revision.

        Raises:
            StateTransitionError: If the identity already carries different content.
        """
        candidate: DatasetSpecRevision = prepared_draft_revision(revision)
        with self.engine.begin() as connection:
            spec_row: RowMapping | None = (
                connection.execute(
                    sa.select(dataset_specs).where(dataset_specs.c.spec_id == candidate.spec_id).with_for_update()
                )
                .mappings()
                .one_or_none()
            )
            if spec_row is None:
                raise StateTransitionError("parent dataset specification is missing")
            existing_id: uuid.UUID | None = connection.scalar(
                sa.select(dataset_spec_revisions.c.spec_revision_id).where(
                    dataset_spec_revisions.c.spec_revision_id == candidate.spec_revision_id
                )
            )
            if existing_id is not None:
                existing: DatasetSpecRevision = self.load_spec_revision(connection, existing_id)
                if existing != candidate:
                    raise StateTransitionError("specification revision identity carries different content")
                return existing
            connection.execute(sa.insert(dataset_spec_revisions).values(**revision_row_values(candidate)))
            field_rows: list[dict[str, object]] = [field_row_values(field_value) for field_value in candidate.fields]
            if field_rows:
                connection.execute(sa.insert(dataset_fields), field_rows)
            index_rows: list[dict[str, object]] = [index_row_values(index) for index in candidate.indexes]
            if index_rows:
                connection.execute(sa.insert(index_definitions), index_rows)
            vector_rows: list[dict[str, object]] = [
                vector_option_row_values(index) for index in candidate.indexes if index.index_type is IndexType.IVF_RQ
            ]
            if vector_rows:
                connection.execute(sa.insert(vector_index_options), vector_rows)
            fts_rows: list[dict[str, object]] = [
                fts_option_row_values(index) for index in candidate.indexes if index.index_type is IndexType.INVERTED
            ]
            if fts_rows:
                connection.execute(sa.insert(fts_index_options), fts_rows)
            persisted: DatasetSpecRevision = self.load_spec_revision(connection, candidate.spec_revision_id)
            if persisted != candidate:
                raise StateTransitionError("persisted draft does not round-trip through normalized storage")
            return persisted

    def activate_spec_revision(self, revision_id: uuid.UUID) -> DatasetSpecRevision:
        """Activate one complete DRAFT and retire the former ACTIVE revision atomically.

        Args:
            revision_id: Exact draft identity.

        Returns:
            Newly active validated revision.

        Raises:
            StateTransitionError: If the revision is absent or cannot be activated.
        """
        current: datetime = utc_now()
        with self.engine.begin() as connection:
            candidate_row: RowMapping | None = (
                connection.execute(
                    sa.select(dataset_spec_revisions)
                    .where(dataset_spec_revisions.c.spec_revision_id == revision_id)
                    .with_for_update()
                )
                .mappings()
                .one_or_none()
            )
            if candidate_row is None:
                raise StateTransitionError("dataset specification revision is missing")
            if candidate_row["state"] == SpecRevisionState.ACTIVE.value:
                return self.load_spec_revision(connection, revision_id)
            if candidate_row["state"] != SpecRevisionState.DRAFT.value:
                raise StateTransitionError("only a DRAFT specification revision may be activated")
            spec_id: uuid.UUID = candidate_row["spec_id"]
            locked_ids: list[uuid.UUID] = list(
                connection.scalars(
                    sa.select(dataset_spec_revisions.c.spec_revision_id)
                    .where(dataset_spec_revisions.c.spec_id == spec_id)
                    .order_by(dataset_spec_revisions.c.revision_number)
                    .with_for_update()
                )
            )
            if revision_id not in locked_ids:
                raise StateTransitionError("dataset specification revision disappeared during activation")
            self.load_spec_revision(connection, revision_id)
            connection.execute(
                sa.update(dataset_spec_revisions)
                .where(
                    dataset_spec_revisions.c.spec_id == spec_id,
                    dataset_spec_revisions.c.state == SpecRevisionState.ACTIVE.value,
                )
                .values(state=SpecRevisionState.RETIRED.value)
            )
            result: sa.CursorResult[Any] = connection.execute(
                sa.update(dataset_spec_revisions)
                .where(
                    dataset_spec_revisions.c.spec_revision_id == revision_id,
                    dataset_spec_revisions.c.state == SpecRevisionState.DRAFT.value,
                )
                .values(state=SpecRevisionState.ACTIVE.value, activated_at=current)
            )
            if result.rowcount != 1:
                raise StateTransitionError("draft activation lost its lifecycle fence")
            return self.load_spec_revision(connection, revision_id)

    def set_source_default_spec(self, source_id: uuid.UUID, spec_id: uuid.UUID) -> IcebergSource:
        """Point one source at a specification that currently has an ACTIVE revision.

        Args:
            source_id: Registered Iceberg source identity.
            spec_id: Named specification identity.

        Returns:
            Updated database-owned source.

        Raises:
            StateTransitionError: If the source or an active revision is absent.
        """
        with self.engine.begin() as connection:
            source_row: RowMapping | None = (
                connection.execute(
                    sa.select(iceberg_sources).where(iceberg_sources.c.source_id == source_id).with_for_update()
                )
                .mappings()
                .one_or_none()
            )
            if source_row is None:
                raise StateTransitionError("Iceberg source is missing")
            self.active_spec_revision_id(connection, spec_id)
            connection.execute(
                sa.update(iceberg_sources)
                .where(iceberg_sources.c.source_id == source_id)
                .values(default_spec_id=spec_id)
            )
            updated: RowMapping = (
                connection.execute(sa.select(iceberg_sources).where(iceberg_sources.c.source_id == source_id))
                .mappings()
                .one()
            )
            return source_from_row(updated)

    def assign_dataset_spec_revision(self, dataset_id: uuid.UUID, revision_id: uuid.UUID) -> uuid.UUID | None:
        """Assign an ACTIVE desired revision and enqueue deterministic convergence work.

        Args:
            dataset_id: Existing logical dataset identity.
            revision_id: ACTIVE desired specification revision.

        Returns:
            Deterministic REBUILD work identity, or ``None`` when no materialized state needs convergence.

        Raises:
            StateTransitionError: If the dataset or active revision is absent or replayed work differs.
        """
        current: datetime = utc_now()
        with self.engine.begin() as connection:
            dataset_row: RowMapping | None = (
                connection.execute(sa.select(datasets).where(datasets.c.dataset_id == dataset_id).with_for_update())
                .mappings()
                .one_or_none()
            )
            if dataset_row is None:
                raise StateTransitionError("dataset is missing")
            revision_row: RowMapping | None = (
                connection.execute(
                    sa.select(dataset_spec_revisions)
                    .where(dataset_spec_revisions.c.spec_revision_id == revision_id)
                    .with_for_update(read=True)
                )
                .mappings()
                .one_or_none()
            )
            if revision_row is None or revision_row["state"] != SpecRevisionState.ACTIVE.value:
                raise StateTransitionError("desired dataset specification revision must be ACTIVE")
            state_row: RowMapping = (
                connection.execute(
                    sa.select(dataset_state).where(dataset_state.c.dataset_id == dataset_id).with_for_update()
                )
                .mappings()
                .one()
            )
            connection.execute(
                sa.update(datasets)
                .where(datasets.c.dataset_id == dataset_id)
                .values(desired_spec_revision_id=revision_id, updated_at=current)
            )
            if state_row["materialized_spec_revision_id"] == revision_id:
                return None
            if state_row["last_applied_source_snapshot_seq"] is None or state_row["ingest_lance_version"] is None:
                return None
            work_id: uuid.UUID = deterministic_rebuild_work_id(dataset_id, revision_id)
            source: IcebergSource = self.source_for_dataset(connection, dataset_id)
            candidate_uri: str = rebuild_uri(source.lance_base_uri, dataset_id, work_id)
            values: dict[str, object] = {
                "work_id": work_id,
                "dataset_id": dataset_id,
                "source_id": dataset_row["source_id"],
                "source_snapshot_seq": int(state_row["last_applied_source_snapshot_seq"]),
                "spec_revision_id": revision_id,
                "kind": WorkKind.REBUILD.value,
                "state": WorkState.PENDING.value,
                "phase": WorkPhase.COMPACT.value,
                "next_attempt_at": current,
                "expected_ingest_lance_uri": state_row["ingest_lance_uri"],
                "expected_ingest_lance_version": state_row["ingest_lance_version"],
                "expected_active_publication_id": state_row["active_publication_id"],
                "candidate_lance_uri": candidate_uri,
                "candidate_lance_version": 0,
            }
            connection.execute(
                postgresql.insert(dataset_work)
                .values(**values)
                .on_conflict_do_nothing(index_elements=[dataset_work.c.work_id])
            )
            work_row: RowMapping = (
                connection.execute(sa.select(dataset_work).where(dataset_work.c.work_id == work_id)).mappings().one()
            )
            actual: tuple[object, ...] = (
                work_row["dataset_id"],
                work_row["source_id"],
                work_row["source_snapshot_seq"],
                work_row["spec_revision_id"],
                work_row["kind"],
                work_row["expected_ingest_lance_uri"],
                work_row["expected_ingest_lance_version"],
                work_row["expected_active_publication_id"],
                work_row["candidate_lance_uri"],
            )
            expected: tuple[object, ...] = (
                values["dataset_id"],
                values["source_id"],
                values["source_snapshot_seq"],
                values["spec_revision_id"],
                values["kind"],
                values["expected_ingest_lance_uri"],
                values["expected_ingest_lance_version"],
                values["expected_active_publication_id"],
                values["candidate_lance_uri"],
            )
            if actual != expected:
                raise StateTransitionError("dataset revision assignment rebuild carries different frozen inputs")
            return work_id

    def load_spec_revision(self, connection: Connection, revision_id: uuid.UUID) -> DatasetSpecRevision:
        """Load one complete normalized immutable specification.

        Args:
            connection: Current transaction or read connection.
            revision_id: Exact frozen revision identity.

        Returns:
            Validated specification revision.

        Raises:
            StateTransitionError: If the revision or a required child is absent.
        """
        revision_row: RowMapping | None = (
            connection.execute(
                sa.select(dataset_spec_revisions).where(dataset_spec_revisions.c.spec_revision_id == revision_id)
            )
            .mappings()
            .one_or_none()
        )
        if revision_row is None:
            raise StateTransitionError("dataset specification revision is missing")
        spec_row: RowMapping = (
            connection.execute(sa.select(dataset_specs).where(dataset_specs.c.spec_id == revision_row["spec_id"]))
            .mappings()
            .one()
        )
        field_rows: list[RowMapping] = list(
            connection.execute(
                sa.select(dataset_fields)
                .where(dataset_fields.c.spec_revision_id == revision_id)
                .order_by(dataset_fields.c.ordinal)
            ).mappings()
        )
        index_rows: list[RowMapping] = list(
            connection.execute(
                sa.select(index_definitions)
                .where(index_definitions.c.spec_revision_id == revision_id)
                .order_by(index_definitions.c.ordinal)
            ).mappings()
        )
        index_ids: list[uuid.UUID] = [row["index_definition_id"] for row in index_rows]
        vector_rows: list[RowMapping] = []
        fts_rows: list[RowMapping] = []
        if index_ids:
            vector_rows = list(
                connection.execute(
                    sa.select(vector_index_options).where(vector_index_options.c.index_definition_id.in_(index_ids))
                ).mappings()
            )
            fts_rows = list(
                connection.execute(
                    sa.select(fts_index_options).where(fts_index_options.c.index_definition_id.in_(index_ids))
                ).mappings()
            )
        return decode_dataset_spec_revision(
            spec_row,
            revision_row,
            field_rows,
            index_rows,
            vector_rows,
            fts_rows,
        )

    def active_spec_revision_id(self, connection: Connection, spec_id: uuid.UUID) -> uuid.UUID:
        """Resolve the sole active revision of one specification.

        Args:
            connection: Current transaction.
            spec_id: Parent specification identity.

        Returns:
            Active revision identity.

        Raises:
            StateTransitionError: If no active revision exists.
        """
        revision_id: uuid.UUID | None = connection.scalar(
            sa.select(dataset_spec_revisions.c.spec_revision_id).where(
                dataset_spec_revisions.c.spec_id == spec_id,
                dataset_spec_revisions.c.state == "ACTIVE",
            )
        )
        if revision_id is None:
            raise StateTransitionError("source default specification has no active revision")
        return revision_id

    def enqueue_source_snapshot(self, plan: SourceSnapshotPlan, dataset_plans: Sequence[DatasetPlan]) -> int:
        """Seal one source snapshot and enqueue its dataset INGEST work.

        Args:
            plan: Exact immutable Iceberg snapshot metadata.
            dataset_plans: Logical datasets touched by the snapshot.

        Returns:
            Existing or newly allocated source-snapshot sequence.

        Raises:
            StateTransitionError: If an idempotency key carries different metadata or datasets.
        """
        dataset_plan: DatasetPlan
        plan.validate()
        if plan.kind is SourceSnapshotKind.REJECTED:
            raise ValueError("rejected snapshots require enqueue_blocked_source_snapshot")
        validated: list[DatasetPlan] = [dataset_plan.validate() for dataset_plan in dataset_plans]
        with self.engine.begin() as connection:
            snapshot_seq: int
            inserted: bool
            snapshot_seq, inserted = self.insert_or_validate_snapshot(connection, plan)
            source_row: RowMapping = (
                connection.execute(
                    sa.select(iceberg_sources).where(iceberg_sources.c.source_id == plan.source_id).with_for_update()
                )
                .mappings()
                .one()
            )
            if source_row["lifecycle_state"] != SourceLifecycleState.ACTIVE.value:
                raise StateTransitionError("source must be ACTIVE before planning")
            default_revision_id: uuid.UUID = self.active_spec_revision_id(connection, source_row["default_spec_id"])
            expected_dataset_ids: set[uuid.UUID] = set()
            for dataset_plan in validated:
                dataset_row: RowMapping = self.insert_or_validate_dataset(
                    connection,
                    source_from_row(source_row),
                    dataset_plan,
                    default_revision_id,
                )
                expected_dataset_ids.add(dataset_row["dataset_id"])
                self.insert_ingest_work(connection, dataset_row, snapshot_seq)
            if not inserted:
                actual_dataset_ids: set[uuid.UUID] = set(
                    connection.scalars(
                        sa.select(dataset_work.c.dataset_id).where(
                            dataset_work.c.source_snapshot_seq == snapshot_seq,
                            dataset_work.c.kind == WorkKind.INGEST.value,
                        )
                    )
                )
                if actual_dataset_ids != expected_dataset_ids:
                    raise StateTransitionError("source snapshot was replayed with a different dataset set")
            if not expected_dataset_ids:
                connection.execute(
                    sa.update(source_snapshots)
                    .where(source_snapshots.c.source_snapshot_seq == snapshot_seq)
                    .values(state=SourceSnapshotState.COMPLETE.value, updated_at=utc_now())
                )
            return snapshot_seq

    def enqueue_blocked_source_snapshot(
        self,
        plan: SourceSnapshotPlan,
        error_code: str,
        error_message: str | None = None,
    ) -> int:
        """Persist a rejected source snapshot without dataset work.

        Args:
            plan: Rejected exact source snapshot metadata.
            error_code: Bounded contract-failure classification.
            error_message: Optional bounded diagnostic.

        Returns:
            Existing or newly allocated source-snapshot sequence.
        """
        plan.validate()
        if plan.kind is not SourceSnapshotKind.REJECTED:
            raise ValueError("blocked source snapshots must use REJECTED kind")
        with self.engine.begin() as connection:
            snapshot_seq: int
            inserted: bool
            snapshot_seq, inserted = self.insert_or_validate_snapshot(
                connection,
                plan,
                state=SourceSnapshotState.BLOCKED,
                error_code=error_code,
                error_message=error_message,
            )
            if not inserted:
                row: RowMapping = (
                    connection.execute(
                        sa.select(source_snapshots).where(source_snapshots.c.source_snapshot_seq == snapshot_seq)
                    )
                    .mappings()
                    .one()
                )
                expected: tuple[str, str | None, str | None] = (
                    SourceSnapshotState.BLOCKED.value,
                    bounded_error(error_code, ERROR_CODE_LIMIT),
                    bounded_error(error_message, ERROR_MESSAGE_LIMIT),
                )
                actual: tuple[str, str | None, str | None] = (
                    str(row["state"]),
                    row["error_code"],
                    row["error_message"],
                )
                if actual != expected:
                    raise StateTransitionError("rejected snapshot replay differs from durable state")
            return snapshot_seq

    def insert_or_validate_snapshot(
        self,
        connection: Connection,
        plan: SourceSnapshotPlan,
        state: SourceSnapshotState = SourceSnapshotState.SEALED,
        error_code: str | None = None,
        error_message: str | None = None,
    ) -> tuple[int, bool]:
        """Insert one snapshot idempotency key or validate its immutable payload.

        Args:
            connection: Current transaction.
            plan: Validated source plan.
            state: Initial durable state.
            error_code: Optional bounded rejection code.
            error_message: Optional bounded rejection diagnostic.

        Returns:
            Source-snapshot sequence and whether insertion occurred.
        """
        statement: Any = (
            postgresql.insert(source_snapshots)
            .values(
                source_id=plan.source_id,
                snapshot_id=plan.snapshot_id,
                parent_snapshot_id=plan.parent_snapshot_id,
                iceberg_sequence_number=plan.iceberg_sequence_number,
                partition_spec_id=plan.partition_spec_id,
                committed_at=plan.committed_at,
                iceberg_operation=plan.iceberg_operation,
                kind=plan.kind.value,
                state=state.value,
                error_code=bounded_error(error_code, ERROR_CODE_LIMIT),
                error_message=bounded_error(error_message, ERROR_MESSAGE_LIMIT),
            )
            .on_conflict_do_nothing(index_elements=[source_snapshots.c.source_id, source_snapshots.c.snapshot_id])
            .returning(source_snapshots.c.source_snapshot_seq)
        )
        inserted_seq: int | None = connection.scalar(statement)
        inserted: bool = inserted_seq is not None
        row: RowMapping = (
            connection.execute(
                sa.select(source_snapshots)
                .where(
                    source_snapshots.c.source_id == plan.source_id,
                    source_snapshots.c.snapshot_id == plan.snapshot_id,
                )
                .with_for_update()
            )
            .mappings()
            .one()
        )
        expected: tuple[object, ...] = (
            plan.parent_snapshot_id,
            plan.iceberg_sequence_number,
            plan.partition_spec_id,
            plan.committed_at,
            plan.iceberg_operation,
            plan.kind.value,
        )
        actual: tuple[object, ...] = (
            row["parent_snapshot_id"],
            row["iceberg_sequence_number"],
            row["partition_spec_id"],
            row["committed_at"],
            row["iceberg_operation"],
            row["kind"],
        )
        if actual != expected:
            raise StateTransitionError("source snapshot idempotency key carries different metadata")
        return int(row["source_snapshot_seq"]), inserted

    def insert_or_validate_dataset(
        self,
        connection: Connection,
        source: IcebergSource,
        plan: DatasetPlan,
        default_revision_id: uuid.UUID,
    ) -> RowMapping:
        """Insert a first-class dataset and mutable state or validate its identity.

        Args:
            connection: Current transaction.
            source: Owning source configuration.
            plan: Validated logical route.
            default_revision_id: Active source-default revision for a new dataset.

        Returns:
            Locked dataset row.
        """
        dataset_id: uuid.UUID = deterministic_dataset_id(source.source_id, plan.identity)
        initial_ingest_uri: str = ingest_uri(source.lance_base_uri, dataset_id)
        connection.execute(
            postgresql.insert(datasets)
            .values(
                dataset_id=dataset_id,
                source_id=source.source_id,
                tenant_id=plan.identity.tenant_id,
                namespace=plan.identity.namespace,
                org_id=plan.identity.org_id,
                lifecycle_state=DatasetLifecycleState.ACTIVE.value,
                desired_spec_revision_id=default_revision_id,
            )
            .on_conflict_do_nothing(index_elements=[datasets.c.dataset_id])
        )
        connection.execute(
            postgresql.insert(dataset_state)
            .values(dataset_id=dataset_id, ingest_lance_uri=initial_ingest_uri)
            .on_conflict_do_nothing(index_elements=[dataset_state.c.dataset_id])
        )
        row: RowMapping = (
            connection.execute(sa.select(datasets).where(datasets.c.dataset_id == dataset_id).with_for_update())
            .mappings()
            .one()
        )
        actual_identity: tuple[object, ...] = (row["source_id"], row["tenant_id"], row["namespace"], row["org_id"])
        expected_identity: tuple[object, ...] = (
            source.source_id,
            plan.identity.tenant_id,
            plan.identity.namespace,
            plan.identity.org_id,
        )
        if actual_identity != expected_identity:
            raise StateTransitionError("deterministic dataset identity conflicts with durable configuration")
        if row["lifecycle_state"] != DatasetLifecycleState.ACTIVE.value:
            raise StateTransitionError("touched dataset must be ACTIVE")
        return row

    def insert_ingest_work(self, connection: Connection, dataset_row: RowMapping, snapshot_seq: int) -> None:
        """Insert replay-safe INGEST work with a frozen specification revision.

        Args:
            connection: Current transaction.
            dataset_row: Locked owning dataset.
            snapshot_seq: Durable source-snapshot sequence.
        """
        state_row: RowMapping = (
            connection.execute(
                sa.select(dataset_state)
                .where(dataset_state.c.dataset_id == dataset_row["dataset_id"])
                .with_for_update()
            )
            .mappings()
            .one()
        )
        work_id: uuid.UUID = deterministic_ingest_work_id(dataset_row["dataset_id"], snapshot_seq)
        values: dict[str, Any] = {
            "work_id": work_id,
            "dataset_id": dataset_row["dataset_id"],
            "source_id": dataset_row["source_id"],
            "source_snapshot_seq": snapshot_seq,
            "spec_revision_id": dataset_row["desired_spec_revision_id"],
            "kind": WorkKind.INGEST.value,
            "state": WorkState.PENDING.value,
            "phase": WorkPhase.INGEST.value,
            "expected_ingest_lance_uri": state_row["ingest_lance_uri"],
            "expected_ingest_lance_version": state_row["ingest_lance_version"],
            "expected_active_publication_id": state_row["active_publication_id"],
        }
        connection.execute(
            postgresql.insert(dataset_work)
            .values(**values)
            .on_conflict_do_nothing(index_elements=[dataset_work.c.work_id])
        )
        row: RowMapping = (
            connection.execute(sa.select(dataset_work).where(dataset_work.c.work_id == work_id)).mappings().one()
        )
        immutable: tuple[object, ...] = (
            row["dataset_id"],
            row["source_id"],
            row["source_snapshot_seq"],
            row["spec_revision_id"],
            row["kind"],
            row["expected_ingest_lance_uri"],
            row["expected_ingest_lance_version"],
            row["expected_active_publication_id"],
        )
        expected: tuple[object, ...] = (
            values["dataset_id"],
            values["source_id"],
            snapshot_seq,
            values["spec_revision_id"],
            WorkKind.INGEST.value,
            values["expected_ingest_lance_uri"],
            values["expected_ingest_lance_version"],
            values["expected_active_publication_id"],
        )
        if immutable != expected:
            raise StateTransitionError("INGEST work idempotency key carries different frozen inputs")

    def latest_source_snapshot(self, source_id: uuid.UUID | None = None) -> RowMapping | None:
        """Read the latest durable source snapshot.

        Args:
            source_id: Optional source identity. Omit only when one source exists.

        Returns:
            Latest snapshot mapping or ``None``.
        """
        with self.engine.connect() as connection:
            selected_source_id: uuid.UUID | None = source_id
            if selected_source_id is None:
                source_ids: list[uuid.UUID] = list(connection.scalars(sa.select(iceberg_sources.c.source_id).limit(2)))
                if len(source_ids) > 1:
                    raise StateTransitionError("source_id is required when multiple sources exist")
                selected_source_id = source_ids[0] if source_ids else None
            if selected_source_id is None:
                return None
            return (
                connection.execute(
                    sa.select(source_snapshots)
                    .where(source_snapshots.c.source_id == selected_source_id)
                    .order_by(source_snapshots.c.iceberg_sequence_number.desc())
                    .limit(1)
                )
                .mappings()
                .one_or_none()
            )

    def block_source_snapshot(
        self,
        source_snapshot_seq: int,
        error_code: str,
        error_message: str | None = None,
    ) -> bool:
        """Block one unapplied snapshot and its unclaimed INGEST work.

        Args:
            source_snapshot_seq: Durable source sequence.
            error_code: Bounded contract failure.
            error_message: Optional bounded diagnostic.

        Returns:
            Whether the snapshot is blocked afterward.

        Raises:
            StateTransitionError: If work is running or the snapshot completed.
        """
        current: datetime = utc_now()
        with self.engine.begin() as connection:
            row: RowMapping | None = (
                connection.execute(
                    sa.select(source_snapshots)
                    .where(source_snapshots.c.source_snapshot_seq == source_snapshot_seq)
                    .with_for_update()
                )
                .mappings()
                .one_or_none()
            )
            if row is None:
                return False
            if row["state"] == SourceSnapshotState.COMPLETE.value:
                raise StateTransitionError("completed source snapshot cannot be blocked")
            running_count: int = int(
                connection.scalar(
                    sa.select(sa.func.count())
                    .select_from(dataset_work)
                    .where(
                        dataset_work.c.source_snapshot_seq == source_snapshot_seq,
                        dataset_work.c.state == WorkState.RUNNING.value,
                    )
                )
                or 0
            )
            if running_count:
                raise StateTransitionError("source snapshot has running work")
            connection.execute(
                sa.update(dataset_work)
                .where(
                    dataset_work.c.source_snapshot_seq == source_snapshot_seq,
                    dataset_work.c.kind == WorkKind.INGEST.value,
                    dataset_work.c.state.in_(
                        (WorkState.PENDING.value, WorkState.RETRY_WAIT.value, WorkState.BLOCKED.value)
                    ),
                )
                .values(
                    state=WorkState.BLOCKED.value,
                    lease_token=None,
                    lease_expires_at=None,
                    error_code=bounded_error(error_code, ERROR_CODE_LIMIT),
                    error_message=bounded_error(error_message, ERROR_MESSAGE_LIMIT),
                    updated_at=current,
                )
            )
            connection.execute(
                sa.update(source_snapshots)
                .where(source_snapshots.c.source_snapshot_seq == source_snapshot_seq)
                .values(
                    state=SourceSnapshotState.BLOCKED.value,
                    error_code=bounded_error(error_code, ERROR_CODE_LIMIT),
                    error_message=bounded_error(error_message, ERROR_MESSAGE_LIMIT),
                    updated_at=current,
                )
            )
            return True

    def due_predicate(self, current: datetime) -> sa.ColumnElement[bool]:
        """Build the reusable due-work predicate.

        Args:
            current: Transaction clock.

        Returns:
            SQL expression selecting pending or retryable due work.
        """
        return sa.and_(
            dataset_work.c.state.in_((WorkState.PENDING.value, WorkState.RETRY_WAIT.value)),
            dataset_work.c.next_attempt_at <= current,
        )

    def lane_order_predicate(self) -> sa.ColumnElement[bool]:
        """Require all earlier source generations in a dataset lane to finish.

        Returns:
            Correlated SQL expression implementing source order.
        """
        earlier: Any = dataset_work.alias("earlier_dataset_work")
        current_seq: Any = sa.func.coalesce(dataset_work.c.source_snapshot_seq, 9223372036854775807)
        earlier_seq: Any = sa.func.coalesce(earlier.c.source_snapshot_seq, 9223372036854775807)
        earlier_kind_rank: Any = sa.case((earlier.c.kind == WorkKind.INGEST.value, 0), else_=1)
        current_kind_rank: Any = sa.case((dataset_work.c.kind == WorkKind.INGEST.value, 0), else_=1)
        precedes: Any = sa.or_(
            earlier_seq < current_seq,
            sa.and_(earlier_seq == current_seq, earlier_kind_rank < current_kind_rank),
        )
        return ~sa.exists(
            sa.select(sa.literal(1)).where(
                earlier.c.dataset_id == dataset_work.c.dataset_id,
                earlier.c.work_id != dataset_work.c.work_id,
                earlier.c.state != WorkState.SUCCEEDED.value,
                precedes,
            )
        )

    def expected_state_predicate(self) -> sa.ColumnElement[bool]:
        """Require frozen work expectations to match current dataset state.

        Returns:
            SQL expression protecting claims from stale generations.
        """
        exact_state: Any = sa.and_(
            dataset_work.c.expected_ingest_lance_uri == dataset_state.c.ingest_lance_uri,
            dataset_work.c.expected_ingest_lance_version.is_not_distinct_from(dataset_state.c.ingest_lance_version),
            dataset_work.c.expected_active_publication_id.is_not_distinct_from(dataset_state.c.active_publication_id),
        )
        return sa.or_(dataset_work.c.kind == WorkKind.INGEST.value, exact_state)

    def claim_due_work(
        self,
        limit: int,
        lease_duration: timedelta,
        now: datetime | None = None,
        provenance: WorkProvenance | None = None,
    ) -> list[WorkClaim]:
        """Claim a bounded dataset-disjoint batch with monotonic fences.

        Args:
            limit: Maximum returned claims.
            lease_duration: Positive lease duration.
            now: Optional deterministic transaction clock.
            provenance: Optional process launch provenance stored on claimed rows.

        Returns:
            Fenced claims.
        """
        if limit < 1 or lease_duration <= timedelta(0):
            raise ValueError("claim limit and lease duration must be positive")
        row: RowMapping
        current: datetime = now or utc_now()
        work_provenance: WorkProvenance = (provenance or WorkProvenance()).validate()
        with self.engine.begin() as connection:
            self.release_expired_leases(connection, current)
            rows: list[RowMapping] = list(
                connection.execute(
                    sa.select(dataset_work)
                    .join(dataset_state, dataset_state.c.dataset_id == dataset_work.c.dataset_id)
                    .join(datasets, datasets.c.dataset_id == dataset_work.c.dataset_id)
                    .where(
                        self.due_predicate(current),
                        self.lane_order_predicate(),
                        self.expected_state_predicate(),
                        datasets.c.lifecycle_state == DatasetLifecycleState.ACTIVE.value,
                    )
                    .order_by(
                        dataset_work.c.source_snapshot_seq.asc().nullslast(),
                        sa.case((dataset_work.c.kind == WorkKind.INGEST.value, 0), else_=1),
                        dataset_work.c.created_at,
                    )
                    .with_for_update(of=dataset_work, skip_locked=True)
                    .limit(limit * 4)
                ).mappings()
            )
            claims: list[WorkClaim] = []
            claimed_datasets: set[uuid.UUID] = set()
            for row in rows:
                if len(claims) >= limit:
                    break
                dataset_id: uuid.UUID = row["dataset_id"]
                if dataset_id in claimed_datasets:
                    continue
                state_row: RowMapping | None = (
                    connection.execute(
                        sa.select(dataset_state)
                        .where(dataset_state.c.dataset_id == dataset_id)
                        .with_for_update(skip_locked=True)
                    )
                    .mappings()
                    .one_or_none()
                )
                if state_row is None:
                    continue
                claim: WorkClaim | None = self.claim_locked_row(
                    connection,
                    row,
                    state_row,
                    current,
                    lease_duration,
                    work_provenance,
                )
                if claim is not None:
                    claims.append(claim)
                    claimed_datasets.add(dataset_id)
            return claims

    def release_expired_leases(self, connection: Connection, current: datetime) -> None:
        """Return expired running work to retry state.

        Args:
            connection: Current transaction.
            current: Transaction clock.
        """
        connection.execute(
            sa.update(dataset_work)
            .where(
                dataset_work.c.state == WorkState.RUNNING.value,
                dataset_work.c.lease_expires_at <= current,
            )
            .values(
                state=WorkState.RETRY_WAIT.value,
                lease_token=None,
                lease_expires_at=None,
                next_attempt_at=current,
                error_code="LEASE_EXPIRED",
                error_message="previous worker lease expired",
                updated_at=current,
            )
        )

    def claim_locked_row(
        self,
        connection: Connection,
        work_row: RowMapping,
        state_row: RowMapping,
        current: datetime,
        lease_duration: timedelta,
        provenance: WorkProvenance,
    ) -> WorkClaim | None:
        """Fence and claim one already locked due row.

        Args:
            connection: Current transaction.
            work_row: Locked work row.
            state_row: Locked dataset-state row.
            current: Transaction clock.
            lease_duration: Positive lease duration.
            provenance: Validated local or Airflow launch provenance.

        Returns:
            Claim or ``None`` if the row ceased to be due.
        """
        if work_row["state"] not in (WorkState.PENDING.value, WorkState.RETRY_WAIT.value):
            return None
        lease_token: uuid.UUID = uuid.uuid4()
        fence_epoch: int = int(state_row["fence_epoch"]) + 1
        attempt_count: int = int(work_row["attempt_count"]) + 1
        lease_expires_at: datetime = current + lease_duration
        refreshed_expectations: dict[str, Any] = {}
        if work_row["kind"] == WorkKind.INGEST.value:
            refreshed_expectations = {
                "expected_ingest_lance_uri": state_row["ingest_lance_uri"],
                "expected_ingest_lance_version": state_row["ingest_lance_version"],
                "expected_active_publication_id": state_row["active_publication_id"],
            }
        connection.execute(
            sa.update(dataset_state)
            .where(dataset_state.c.dataset_id == work_row["dataset_id"])
            .values(fence_epoch=fence_epoch, updated_at=current)
        )
        result: sa.CursorResult[Any] = connection.execute(
            sa.update(dataset_work)
            .where(
                dataset_work.c.work_id == work_row["work_id"],
                dataset_work.c.state.in_((WorkState.PENDING.value, WorkState.RETRY_WAIT.value)),
            )
            .values(
                state=WorkState.RUNNING.value,
                lease_token=lease_token,
                lease_expires_at=lease_expires_at,
                attempt_count=attempt_count,
                error_code=None,
                error_message=None,
                launcher_kind=provenance.launcher_kind.value,
                airflow_ctx_dag_id=provenance.airflow_ctx_dag_id,
                airflow_ctx_dag_run_id=provenance.airflow_ctx_dag_run_id,
                airflow_ctx_task_id=provenance.airflow_ctx_task_id,
                airflow_ctx_map_index=provenance.airflow_ctx_map_index,
                airflow_ctx_try_number=provenance.airflow_ctx_try_number,
                updated_at=current,
                **refreshed_expectations,
            )
        )
        if result.rowcount != 1:
            raise StateTransitionError("locked due work changed during claim")
        return WorkClaim(
            work_id=work_row["work_id"],
            dataset_id=work_row["dataset_id"],
            kind=WorkKind(str(work_row["kind"])),
            phase=WorkPhase(str(work_row["phase"])),
            lease_token=lease_token,
            fence_epoch=fence_epoch,
            attempt_count=attempt_count,
            source_snapshot_seq=work_row["source_snapshot_seq"],
            spec_revision_id=work_row["spec_revision_id"],
            ingest_lance_uri=state_row["ingest_lance_uri"],
            ingest_lance_version=state_row["ingest_lance_version"],
            lease_expires_at=lease_expires_at,
        )

    def lease_is_current(self, claim: WorkClaim, current: datetime) -> sa.ColumnElement[bool]:
        """Build the complete lease and fence predicate for one claim.

        Args:
            claim: Worker claim.
            current: Transaction clock.

        Returns:
            SQL expression requiring state, token, expiry, and dataset fence.
        """
        return sa.and_(
            dataset_work.c.work_id == claim.work_id,
            dataset_work.c.dataset_id == claim.dataset_id,
            dataset_work.c.state == WorkState.RUNNING.value,
            dataset_work.c.lease_token == claim.lease_token,
            dataset_work.c.lease_expires_at > current,
            sa.exists(
                sa.select(sa.literal(1)).where(
                    dataset_state.c.dataset_id == claim.dataset_id,
                    dataset_state.c.fence_epoch == claim.fence_epoch,
                )
            ),
        )

    def work_execution_context(
        self,
        claim: WorkClaim,
        now: datetime | None = None,
    ) -> WorkExecutionContext | None:
        """Load immutable execution inputs only for a live fenced claim.

        Args:
            claim: Worker claim.
            now: Optional deterministic clock.

        Returns:
            Typed execution context or ``None`` for a stale claim.
        """
        current: datetime = now or utc_now()
        work_and_dataset: Any = dataset_work.join(datasets, datasets.c.dataset_id == dataset_work.c.dataset_id)
        snapshot_join: Any = work_and_dataset.join(
            source_snapshots,
            sa.and_(
                source_snapshots.c.source_snapshot_seq == dataset_work.c.source_snapshot_seq,
                source_snapshots.c.source_id == datasets.c.source_id,
            ),
        )
        joined: Any = snapshot_join.join(dataset_state, dataset_state.c.dataset_id == dataset_work.c.dataset_id).join(
            iceberg_sources, iceberg_sources.c.source_id == datasets.c.source_id
        )
        with self.engine.connect() as connection:
            row: RowMapping | None = (
                connection.execute(
                    sa.select(
                        dataset_work,
                        datasets.c.tenant_id,
                        datasets.c.namespace,
                        datasets.c.org_id,
                        source_snapshots.c.snapshot_id,
                        source_snapshots.c.parent_snapshot_id,
                        source_snapshots.c.iceberg_sequence_number,
                        source_snapshots.c.partition_spec_id,
                        source_snapshots.c.kind.label("source_snapshot_kind"),
                        *[column.label(f"source_{column.name}") for column in iceberg_sources.c],
                    )
                    .select_from(joined)
                    .where(self.lease_is_current(claim, current))
                )
                .mappings()
                .one_or_none()
            )
            if row is None:
                return None
            source_mapping: dict[str, Any] = {column.name: row[f"source_{column.name}"] for column in iceberg_sources.c}
            source: IcebergSource = source_from_row(source_mapping)
            spec_revision: DatasetSpecRevision = self.load_spec_revision(connection, row["spec_revision_id"])
        snapshot_kind_value: str | None = row["source_snapshot_kind"]
        return WorkExecutionContext(
            claim=claim,
            identity=RoutingIdentity(
                tenant_id=str(row["tenant_id"]),
                namespace=str(row["namespace"]),
                org_id=str(row["org_id"]),
            ).validate(),
            source_table=source.spark_table,
            source=source,
            spec_revision=spec_revision,
            snapshot_id=row["snapshot_id"],
            parent_snapshot_id=row["parent_snapshot_id"],
            iceberg_sequence_number=row["iceberg_sequence_number"],
            partition_spec_id=row["partition_spec_id"],
            source_snapshot_kind=SourceSnapshotKind(snapshot_kind_value) if snapshot_kind_value is not None else None,
            candidate_lance_uri=row["candidate_lance_uri"],
            candidate_lance_version=row["candidate_lance_version"],
            artifact_manifest_uri=row["artifact_manifest_uri"],
            artifact_digest=bytes(row["artifact_digest"]) if row["artifact_digest"] is not None else None,
        )

    def renew_lease(
        self,
        claim: WorkClaim,
        lease_duration: timedelta,
        now: datetime | None = None,
    ) -> bool:
        """Extend a live claim without changing its fence.

        Args:
            claim: Worker claim.
            lease_duration: Positive extension from now.
            now: Optional deterministic clock.

        Returns:
            Whether the live claim was renewed.
        """
        if lease_duration <= timedelta(0):
            raise ValueError("lease_duration must be positive")
        current: datetime = now or utc_now()
        with self.engine.begin() as connection:
            lease_expires_at: datetime = current + lease_duration
            result: sa.CursorResult[Any] = connection.execute(
                sa.update(dataset_work)
                .where(self.lease_is_current(claim, current))
                .values(lease_expires_at=lease_expires_at, updated_at=current)
            )
            return result.rowcount == 1

    def advance_phase(
        self,
        claim: WorkClaim,
        phase: WorkPhase,
        candidate_lance_uri: str | None = None,
        candidate_lance_version: int | None = None,
        artifact_manifest_uri: str | None = None,
        artifact_digest: bytes | None = None,
        now: datetime | None = None,
    ) -> bool:
        """Persist a forward-only phase checkpoint for a live claim.

        Args:
            claim: Worker claim.
            phase: Later durable phase.
            candidate_lance_uri: Optional exact candidate URI.
            candidate_lance_version: Optional exact candidate version.
            artifact_manifest_uri: Optional immutable manifest URI.
            artifact_digest: Optional 32-byte artifact digest.
            now: Optional deterministic clock.

        Returns:
            Whether the live fence accepted the checkpoint.
        """
        if PHASE_ORDER[phase] < PHASE_ORDER[claim.phase]:
            raise StateTransitionError("work phase cannot move backward")
        if (candidate_lance_uri is None) != (candidate_lance_version is None):
            raise ValueError("candidate URI and version must be supplied together")
        if artifact_digest is not None and (artifact_manifest_uri is None or len(artifact_digest) != 32):
            raise ValueError("artifact digest requires a manifest URI and 32 bytes")
        current: datetime = now or utc_now()
        values: dict[str, Any] = {"phase": phase.value, "updated_at": current}
        if candidate_lance_uri is not None:
            values.update(
                candidate_lance_uri=candidate_lance_uri,
                candidate_lance_version=candidate_lance_version,
            )
        if artifact_manifest_uri is not None:
            values["artifact_manifest_uri"] = artifact_manifest_uri
        if artifact_digest is not None:
            values["artifact_digest"] = artifact_digest
        with self.engine.begin() as connection:
            row: RowMapping | None = (
                connection.execute(
                    sa.select(dataset_work).where(dataset_work.c.work_id == claim.work_id).with_for_update()
                )
                .mappings()
                .one_or_none()
            )
            if row is None or PHASE_ORDER[WorkPhase(str(row["phase"]))] > PHASE_ORDER[phase]:
                return False
            result: sa.CursorResult[Any] = connection.execute(
                sa.update(dataset_work).where(self.lease_is_current(claim, current)).values(**values)
            )
            return result.rowcount == 1

    def retry_work(
        self,
        claim: WorkClaim,
        delay: timedelta,
        error_code: str,
        error_message: str,
        now: datetime | None = None,
    ) -> bool:
        """Return transient failure to retry or block it at the attempt bound.

        Args:
            claim: Worker claim.
            delay: Non-negative retry delay.
            error_code: Bounded failure classification.
            error_message: Bounded diagnostic.
            now: Optional deterministic clock.

        Returns:
            Whether the live fence accepted the transition.
        """
        if delay < timedelta(0):
            raise ValueError("retry delay must be non-negative")
        current: datetime = now or utc_now()
        with self.engine.begin() as connection:
            max_attempts: int = int(
                connection.scalar(
                    sa.select(reconciler_settings.c.max_attempts).where(reconciler_settings.c.singleton_id == 1)
                )
                or 0
            )
            exhausted: bool = claim.attempt_count >= max_attempts
            state: WorkState = WorkState.BLOCKED if exhausted else WorkState.RETRY_WAIT
            persisted_code: str = "MAX_ATTEMPTS_EXHAUSTED" if exhausted else error_code
            persisted_message: str = f"{error_code}: {error_message}" if exhausted else error_message
            result: sa.CursorResult[Any] = connection.execute(
                sa.update(dataset_work)
                .where(self.lease_is_current(claim, current))
                .values(
                    state=state.value,
                    lease_token=None,
                    lease_expires_at=None,
                    next_attempt_at=current if exhausted else current + delay,
                    error_code=bounded_error(persisted_code, ERROR_CODE_LIMIT),
                    error_message=bounded_error(persisted_message, ERROR_MESSAGE_LIMIT),
                    updated_at=current,
                )
            )
            return result.rowcount == 1

    def block_work(
        self,
        claim: WorkClaim,
        error_code: str,
        error_message: str,
        now: datetime | None = None,
    ) -> bool:
        """Persist a terminal contract failure for a live claim.

        Args:
            claim: Worker claim.
            error_code: Bounded failure classification.
            error_message: Bounded diagnostic.
            now: Optional deterministic clock.

        Returns:
            Whether the live fence accepted the transition.
        """
        current: datetime = now or utc_now()
        with self.engine.begin() as connection:
            result: sa.CursorResult[Any] = connection.execute(
                sa.update(dataset_work)
                .where(self.lease_is_current(claim, current))
                .values(
                    state=WorkState.BLOCKED.value,
                    lease_token=None,
                    lease_expires_at=None,
                    error_code=bounded_error(error_code, ERROR_CODE_LIMIT),
                    error_message=bounded_error(error_message, ERROR_MESSAGE_LIMIT),
                    updated_at=current,
                )
            )
            return result.rowcount == 1

    def retry_blocked_work(self, work_id: uuid.UUID, now: datetime | None = None) -> bool:
        """Return one explicitly selected blocked work item to pending.

        Args:
            work_id: Durable work identity.
            now: Optional deterministic clock.

        Returns:
            Whether a blocked row was retried.
        """
        current: datetime = now or utc_now()
        with self.engine.begin() as connection:
            result: sa.CursorResult[Any] = connection.execute(
                sa.update(dataset_work)
                .where(
                    dataset_work.c.work_id == work_id,
                    dataset_work.c.state == WorkState.BLOCKED.value,
                )
                .values(
                    state=WorkState.PENDING.value,
                    lease_token=None,
                    lease_expires_at=None,
                    next_attempt_at=current,
                    error_code=None,
                    error_message=None,
                    updated_at=current,
                )
            )
            return result.rowcount == 1

    def complete_ingest(
        self,
        claim: WorkClaim,
        ingest_lance_version: int,
        source_row_count: int,
        source_digest: bytes,
        now: datetime | None = None,
    ) -> bool:
        """Atomically advance mutable ingest state and enqueue publication.

        Args:
            claim: Live INGEST claim.
            ingest_lance_version: Exact committed Lance version.
            source_row_count: Terminal source mutation count.
            source_digest: Frozen 32-byte source digest.
            now: Optional deterministic clock.

        Returns:
            Whether the result was accepted or already completed identically.
        """
        if claim.kind is not WorkKind.INGEST:
            raise ValueError("complete_ingest requires an INGEST claim")
        if ingest_lance_version < 0 or source_row_count < 0 or len(source_digest) != 32:
            raise ValueError("ingest completion requires non-negative values and a 32-byte digest")
        current: datetime = now or utc_now()
        with self.engine.begin() as connection:
            work_row: RowMapping | None = (
                connection.execute(
                    sa.select(dataset_work).where(dataset_work.c.work_id == claim.work_id).with_for_update()
                )
                .mappings()
                .one_or_none()
            )
            state_row: RowMapping | None = (
                connection.execute(
                    sa.select(dataset_state).where(dataset_state.c.dataset_id == claim.dataset_id).with_for_update()
                )
                .mappings()
                .one_or_none()
            )
            if work_row is None or state_row is None:
                return False
            if self.completed_ingest_matches(
                work_row,
                state_row,
                ingest_lance_version,
                source_row_count,
                source_digest,
            ):
                return True
            if not self.locked_lease_matches(work_row, state_row, claim, current):
                return False
            expected: tuple[object, ...] = (
                work_row["expected_ingest_lance_uri"],
                work_row["expected_ingest_lance_version"],
                work_row["expected_active_publication_id"],
            )
            actual: tuple[object, ...] = (
                state_row["ingest_lance_uri"],
                state_row["ingest_lance_version"],
                state_row["active_publication_id"],
            )
            if expected != actual:
                raise StateTransitionError("INGEST work expectations no longer match dataset state")
            connection.execute(
                sa.update(dataset_state)
                .where(dataset_state.c.dataset_id == claim.dataset_id)
                .values(
                    materialized_spec_revision_id=work_row["spec_revision_id"],
                    last_applied_source_snapshot_seq=work_row["source_snapshot_seq"],
                    ingest_lance_uri=work_row["expected_ingest_lance_uri"],
                    ingest_lance_version=ingest_lance_version,
                    updated_at=current,
                )
            )
            connection.execute(
                sa.update(dataset_work)
                .where(dataset_work.c.work_id == claim.work_id)
                .values(
                    state=WorkState.SUCCEEDED.value,
                    lease_token=None,
                    lease_expires_at=None,
                    source_applied_at=current,
                    source_row_count=source_row_count,
                    source_digest=source_digest,
                    candidate_lance_uri=work_row["expected_ingest_lance_uri"],
                    candidate_lance_version=ingest_lance_version,
                    updated_at=current,
                )
            )
            self.enqueue_publish_work(
                connection,
                claim.dataset_id,
                work_row["source_id"],
                int(work_row["source_snapshot_seq"]),
                work_row["spec_revision_id"],
                work_row["expected_ingest_lance_uri"],
                ingest_lance_version,
                state_row["active_publication_id"],
                current,
            )
            self.complete_snapshot_if_applied(connection, int(work_row["source_snapshot_seq"]), current)
            return True

    def completed_ingest_matches(
        self,
        work_row: RowMapping,
        state_row: RowMapping,
        ingest_lance_version: int,
        source_row_count: int,
        source_digest: bytes,
    ) -> bool:
        """Validate an idempotent replay of a completed INGEST result.

        Args:
            work_row: Locked work row.
            state_row: Locked mutable dataset state.
            ingest_lance_version: Replayed exact version.
            source_row_count: Replayed row count.
            source_digest: Replayed source digest.

        Returns:
            Whether the row is not completed or is completed identically.

        Raises:
            StateTransitionError: If completed output differs.
        """
        if work_row["state"] != WorkState.SUCCEEDED.value:
            return False
        stored_digest: object | None = work_row["source_digest"]
        last_applied_seq: int | None = state_row["last_applied_source_snapshot_seq"]
        actual: tuple[object, ...] = (
            work_row["candidate_lance_version"],
            work_row["source_row_count"],
            bytes(stored_digest) if stored_digest is not None else None,
        )
        expected: tuple[object, ...] = (
            ingest_lance_version,
            source_row_count,
            source_digest,
        )
        source_snapshot_seq: int = int(work_row["source_snapshot_seq"])
        if actual != expected or last_applied_seq is None or last_applied_seq < source_snapshot_seq:
            raise StateTransitionError("completed INGEST replay differs from durable output")
        return True

    def locked_lease_matches(
        self,
        work_row: RowMapping,
        state_row: RowMapping,
        claim: WorkClaim,
        current: datetime,
    ) -> bool:
        """Check state, token, expiry, and fence on locked rows.

        Args:
            work_row: Locked work row.
            state_row: Locked dataset-state row.
            claim: Worker claim.
            current: Transaction clock.

        Returns:
            Whether the claim still owns the work.
        """
        expiry: datetime | None = work_row["lease_expires_at"]
        return (
            work_row["state"] == WorkState.RUNNING.value
            and work_row["lease_token"] == claim.lease_token
            and int(state_row["fence_epoch"]) == claim.fence_epoch
            and expiry is not None
            and expiry > current
        )

    def enqueue_publish_work(
        self,
        connection: Connection,
        dataset_id: uuid.UUID,
        source_id: uuid.UUID,
        source_snapshot_seq: int,
        spec_revision_id: uuid.UUID,
        ingest_lance_uri: str,
        ingest_lance_version: int,
        active_publication_id: uuid.UUID | None,
        current: datetime,
    ) -> None:
        """Insert the immutable publication generation for completed ingest.

        Args:
            connection: Current transaction.
            dataset_id: Owning dataset.
            source_id: Owning Iceberg source.
            source_snapshot_seq: Exact materialized source generation.
            spec_revision_id: Frozen specification revision.
            ingest_lance_uri: Exact mutable ingest URI.
            ingest_lance_version: Exact materialized version.
            active_publication_id: Expected current catalog pointer.
            current: Transaction clock.
        """
        existing_open: RowMapping | None = (
            connection.execute(
                sa.select(dataset_work)
                .where(
                    dataset_work.c.dataset_id == dataset_id,
                    dataset_work.c.kind == WorkKind.PUBLISH.value,
                    dataset_work.c.state.in_(
                        (WorkState.PENDING.value, WorkState.RUNNING.value, WorkState.RETRY_WAIT.value)
                    ),
                )
                .with_for_update()
            )
            .mappings()
            .one_or_none()
        )
        if existing_open is not None:
            raise StateTransitionError("dataset already has an open publication generation")
        work_id: uuid.UUID = deterministic_publish_work_id(dataset_id, source_snapshot_seq, spec_revision_id)
        connection.execute(
            postgresql.insert(dataset_work)
            .values(
                work_id=work_id,
                dataset_id=dataset_id,
                source_id=source_id,
                source_snapshot_seq=source_snapshot_seq,
                spec_revision_id=spec_revision_id,
                kind=WorkKind.PUBLISH.value,
                state=WorkState.PENDING.value,
                phase=WorkPhase.COMPACT.value,
                next_attempt_at=current,
                expected_ingest_lance_uri=ingest_lance_uri,
                expected_ingest_lance_version=ingest_lance_version,
                expected_active_publication_id=active_publication_id,
                candidate_lance_uri=ingest_lance_uri,
                candidate_lance_version=ingest_lance_version,
            )
            .on_conflict_do_nothing(index_elements=[dataset_work.c.work_id])
        )

    def complete_snapshot_if_applied(
        self,
        connection: Connection,
        source_snapshot_seq: int,
        current: datetime,
    ) -> None:
        """Mark a snapshot complete once every INGEST row succeeded.

        Args:
            connection: Current transaction.
            source_snapshot_seq: Durable source sequence.
            current: Transaction clock.
        """
        incomplete: Any = sa.exists(
            sa.select(sa.literal(1)).where(
                dataset_work.c.source_snapshot_seq == source_snapshot_seq,
                dataset_work.c.kind == WorkKind.INGEST.value,
                dataset_work.c.state != WorkState.SUCCEEDED.value,
            )
        )
        connection.execute(
            sa.update(source_snapshots)
            .where(
                source_snapshots.c.source_snapshot_seq == source_snapshot_seq,
                ~incomplete,
            )
            .values(state=SourceSnapshotState.COMPLETE.value, updated_at=current)
        )

    def publish_dataset(
        self,
        claim: WorkClaim,
        candidate_lance_uri: str,
        candidate_lance_version: int,
        artifact_manifest_uri: str,
        artifact_digest: bytes,
        evidence: PublicationEvidence,
        now: datetime | None = None,
    ) -> bool:
        """Atomically insert qualification evidence and swap the serving pointer.

        Args:
            claim: Live PUBLISH or REBUILD claim.
            candidate_lance_uri: Exact qualified dataset URI.
            candidate_lance_version: Exact qualified Lance version.
            artifact_manifest_uri: Immutable manifest URI.
            artifact_digest: Frozen manifest digest.
            evidence: Exact schema, row, fragment, and index evidence.
            now: Optional deterministic clock.

        Returns:
            Whether the result was accepted or already published identically.
        """
        validate_publication_request(
            claim,
            candidate_lance_version,
            artifact_manifest_uri,
            artifact_digest,
            evidence,
        )
        index_evidence: Any
        current: datetime = now or utc_now()
        with self.engine.begin() as connection:
            work_row: RowMapping | None = (
                connection.execute(
                    sa.select(dataset_work).where(dataset_work.c.work_id == claim.work_id).with_for_update()
                )
                .mappings()
                .one_or_none()
            )
            state_row: RowMapping | None = (
                connection.execute(
                    sa.select(dataset_state).where(dataset_state.c.dataset_id == claim.dataset_id).with_for_update()
                )
                .mappings()
                .one_or_none()
            )
            if work_row is None or state_row is None:
                return False
            publication_id: uuid.UUID = deterministic_publication_id(claim.work_id)
            existing: RowMapping | None = (
                connection.execute(
                    sa.select(dataset_publications).where(dataset_publications.c.publication_id == publication_id)
                )
                .mappings()
                .one_or_none()
            )
            if existing is not None:
                self.validate_completed_publication(
                    connection,
                    existing,
                    state_row,
                    candidate_lance_uri,
                    candidate_lance_version,
                    artifact_manifest_uri,
                    artifact_digest,
                    evidence,
                )
                return True
            if not self.locked_lease_matches(work_row, state_row, claim, current):
                return False
            source_snapshot_seq: int = self.validate_live_publication(
                connection,
                claim,
                work_row,
                state_row,
                candidate_lance_uri,
                candidate_lance_version,
            )
            self.validate_publication_indexes(connection, work_row["spec_revision_id"], evidence)
            connection.execute(
                sa.insert(dataset_publications).values(
                    publication_id=publication_id,
                    dataset_id=claim.dataset_id,
                    work_id=claim.work_id,
                    spec_revision_id=work_row["spec_revision_id"],
                    source_snapshot_seq=source_snapshot_seq,
                    lance_uri=candidate_lance_uri,
                    lance_version=candidate_lance_version,
                    schema_digest=evidence.schema_digest,
                    total_row_count=evidence.total_row_count,
                    distinct_row_count=evidence.distinct_row_count,
                    live_row_count=evidence.live_row_count,
                    distinct_live_row_count=evidence.distinct_live_row_count,
                    fragment_count=evidence.fragment_count,
                    manifest_uri=artifact_manifest_uri,
                    manifest_digest=artifact_digest,
                    published_at=current,
                )
            )
            for index_evidence in evidence.indexes:
                connection.execute(
                    sa.insert(publication_indexes).values(
                        publication_id=publication_id,
                        spec_revision_id=work_row["spec_revision_id"],
                        index_definition_id=index_evidence.index_definition_id,
                        actual_index_type=index_evidence.actual_index_type.value,
                        indexed_fragment_count=index_evidence.indexed_fragment_count,
                        unindexed_fragment_count=index_evidence.unindexed_fragment_count,
                        artifact_generation_digest=index_evidence.artifact_generation_digest,
                    )
                )
            previous_publication_id: uuid.UUID | None = state_row["active_publication_id"]
            if previous_publication_id is not None:
                connection.execute(
                    sa.update(dataset_publications)
                    .where(dataset_publications.c.publication_id == previous_publication_id)
                    .values(retired_at=current)
                )
            state_values: dict[str, Any] = {
                "active_publication_id": publication_id,
                "updated_at": current,
            }
            if claim.kind is WorkKind.REBUILD:
                state_values.update(
                    ingest_lance_uri=candidate_lance_uri,
                    ingest_lance_version=candidate_lance_version,
                    materialized_spec_revision_id=work_row["spec_revision_id"],
                )
            connection.execute(
                sa.update(dataset_state).where(dataset_state.c.dataset_id == claim.dataset_id).values(**state_values)
            )
            connection.execute(
                sa.update(dataset_work)
                .where(dataset_work.c.work_id == claim.work_id)
                .values(
                    state=WorkState.SUCCEEDED.value,
                    phase=WorkPhase.PUBLISH.value,
                    lease_token=None,
                    lease_expires_at=None,
                    candidate_lance_uri=candidate_lance_uri,
                    candidate_lance_version=candidate_lance_version,
                    artifact_manifest_uri=artifact_manifest_uri,
                    artifact_digest=artifact_digest,
                    updated_at=current,
                )
            )
            return True

    def validate_live_publication(
        self,
        connection: Connection,
        claim: WorkClaim,
        work_row: RowMapping,
        state_row: RowMapping,
        candidate_lance_uri: str,
        candidate_lance_version: int,
    ) -> int:
        """Validate frozen state and candidate ownership for publication.

        Args:
            connection: Current transaction.
            claim: Live publication claim.
            work_row: Locked work row.
            state_row: Locked dataset state.
            candidate_lance_uri: Qualified candidate URI.
            candidate_lance_version: Qualified candidate version.

        Returns:
            Exact materialized source-snapshot sequence.

        Raises:
            StateTransitionError: If frozen state or candidate identity changed.
        """
        expected_state: tuple[object, ...] = (
            work_row["expected_ingest_lance_uri"],
            work_row["expected_ingest_lance_version"],
            work_row["expected_active_publication_id"],
        )
        actual_state: tuple[object, ...] = (
            state_row["ingest_lance_uri"],
            state_row["ingest_lance_version"],
            state_row["active_publication_id"],
        )
        if expected_state != actual_state:
            raise StateTransitionError("publication expectations no longer match dataset state")
        source_snapshot_seq: int = int(work_row["source_snapshot_seq"])
        source: IcebergSource = self.source_for_dataset(connection, claim.dataset_id)
        if not dataset_uri_matches_root(source.lance_base_uri, claim.dataset_id, candidate_lance_uri):
            raise StateTransitionError("candidate URI is outside the source-owned Lance root")
        stored_candidate: tuple[object, object] = (
            work_row["candidate_lance_uri"],
            work_row["candidate_lance_version"],
        )
        if stored_candidate[0] is not None and stored_candidate != (candidate_lance_uri, candidate_lance_version):
            raise StateTransitionError("qualified candidate differs from the checkpointed generation")
        return source_snapshot_seq

    def validate_publication_indexes(
        self,
        connection: Connection,
        spec_revision_id: uuid.UUID,
        evidence: PublicationEvidence,
    ) -> None:
        """Require exact configured index coverage before publication.

        Args:
            connection: Current transaction.
            spec_revision_id: Frozen specification revision.
            evidence: Candidate index coverage.

        Raises:
            StateTransitionError: If evidence omits, adds, or changes an index.
        """
        configured: dict[uuid.UUID, str] = {
            row["index_definition_id"]: str(row["index_type"])
            for row in connection.execute(
                sa.select(index_definitions.c.index_definition_id, index_definitions.c.index_type).where(
                    index_definitions.c.spec_revision_id == spec_revision_id
                )
            ).mappings()
        }
        actual: dict[uuid.UUID, str] = {
            item.index_definition_id: item.actual_index_type.value for item in evidence.indexes
        }
        if actual != configured:
            raise StateTransitionError("publication index evidence differs from the frozen specification")

    def validate_completed_publication(
        self,
        connection: Connection,
        publication_row: RowMapping,
        state_row: RowMapping,
        candidate_lance_uri: str,
        candidate_lance_version: int,
        artifact_manifest_uri: str,
        artifact_digest: bytes,
        evidence: PublicationEvidence,
    ) -> None:
        """Validate an idempotent replay after publication succeeded.

        Args:
            connection: Current transaction.
            publication_row: Existing immutable publication.
            state_row: Current dataset state.
            candidate_lance_uri: Replayed URI.
            candidate_lance_version: Replayed version.
            artifact_manifest_uri: Replayed manifest URI.
            artifact_digest: Replayed manifest digest.
            evidence: Replayed qualification evidence.

        Raises:
            StateTransitionError: If any immutable output differs.
        """
        actual: tuple[object, ...] = (
            publication_row["lance_uri"],
            publication_row["lance_version"],
            publication_row["manifest_uri"],
            bytes(publication_row["manifest_digest"]),
            bytes(publication_row["schema_digest"]),
            publication_row["total_row_count"],
            publication_row["distinct_row_count"],
            publication_row["live_row_count"],
            publication_row["distinct_live_row_count"],
            publication_row["fragment_count"],
        )
        expected: tuple[object, ...] = (
            candidate_lance_uri,
            candidate_lance_version,
            artifact_manifest_uri,
            artifact_digest,
            evidence.schema_digest,
            evidence.total_row_count,
            evidence.distinct_row_count,
            evidence.live_row_count,
            evidence.distinct_live_row_count,
            evidence.fragment_count,
        )
        if actual != expected or state_row["active_publication_id"] != publication_row["publication_id"]:
            raise StateTransitionError("completed publication replay differs from durable output")
        stored_indexes: dict[uuid.UUID, tuple[str, int, int, bytes | None]] = {
            row["index_definition_id"]: (
                str(row["actual_index_type"]),
                int(row["indexed_fragment_count"]),
                int(row["unindexed_fragment_count"]),
                bytes(row["artifact_generation_digest"]) if row["artifact_generation_digest"] is not None else None,
            )
            for row in connection.execute(
                sa.select(publication_indexes).where(
                    publication_indexes.c.publication_id == publication_row["publication_id"]
                )
            ).mappings()
        }
        replayed_indexes: dict[uuid.UUID, tuple[str, int, int, bytes | None]] = {
            item.index_definition_id: (
                item.actual_index_type.value,
                item.indexed_fragment_count,
                item.unindexed_fragment_count,
                item.artifact_generation_digest,
            )
            for item in evidence.indexes
        }
        if stored_indexes != replayed_indexes:
            raise StateTransitionError("completed publication index evidence differs")

    def source_for_dataset(self, connection: Connection, dataset_id: uuid.UUID) -> IcebergSource:
        """Load the source owning one dataset.

        Args:
            connection: Current transaction.
            dataset_id: Dataset identity.

        Returns:
            Typed source.
        """
        row: RowMapping = (
            connection.execute(
                sa.select(iceberg_sources)
                .join(datasets, datasets.c.source_id == iceberg_sources.c.source_id)
                .where(datasets.c.dataset_id == dataset_id)
            )
            .mappings()
            .one()
        )
        return source_from_row(row)

    def resolve_serving_dataset(self, identity: RoutingIdentity) -> ServingDataset | None:
        """Resolve a route through the active immutable publication pointer.

        Args:
            identity: Validated authenticated route.

        Returns:
            Exact serving dataset or ``None``.

        Raises:
            StateTransitionError: If multiple active sources expose the same route.
        """
        identity.validate()
        joined: Any = (
            datasets.join(dataset_state, dataset_state.c.dataset_id == datasets.c.dataset_id)
            .join(
                dataset_publications,
                sa.and_(
                    dataset_publications.c.dataset_id == dataset_state.c.dataset_id,
                    dataset_publications.c.publication_id == dataset_state.c.active_publication_id,
                ),
            )
            .join(iceberg_sources, iceberg_sources.c.source_id == datasets.c.source_id)
        )
        with self.engine.connect() as connection:
            rows: list[RowMapping] = list(
                connection.execute(
                    sa.select(
                        datasets.c.dataset_id,
                        dataset_publications.c.publication_id,
                        dataset_publications.c.lance_uri,
                        dataset_publications.c.lance_version,
                    )
                    .select_from(joined)
                    .where(
                        datasets.c.tenant_id == identity.tenant_id,
                        datasets.c.namespace == identity.namespace,
                        datasets.c.org_id == identity.org_id,
                        datasets.c.lifecycle_state == DatasetLifecycleState.ACTIVE.value,
                        iceberg_sources.c.lifecycle_state == SourceLifecycleState.ACTIVE.value,
                    )
                    .limit(2)
                ).mappings()
            )
        if len(rows) > 1:
            raise StateTransitionError("serving route is ambiguous across active sources")
        if not rows:
            return None
        row: RowMapping = rows[0]
        return ServingDataset(
            dataset_id=row["dataset_id"],
            identity=identity,
            lance_uri=str(row["lance_uri"]),
            lance_version=int(row["lance_version"]),
            publication_id=row["publication_id"],
        )

    def dataset_and_state_for_identity(
        self,
        connection: Connection,
        identity: RoutingIdentity,
    ) -> tuple[RowMapping, RowMapping]:
        """Lock the sole active dataset and mutable state for a route.

        Args:
            connection: Current transaction.
            identity: Validated logical route.

        Returns:
            Locked dataset and state rows.

        Raises:
            StateTransitionError: If the route is absent or ambiguous.
        """
        rows: list[RowMapping] = list(
            connection.execute(
                sa.select(datasets)
                .join(iceberg_sources, iceberg_sources.c.source_id == datasets.c.source_id)
                .where(
                    datasets.c.tenant_id == identity.tenant_id,
                    datasets.c.namespace == identity.namespace,
                    datasets.c.org_id == identity.org_id,
                    datasets.c.lifecycle_state == DatasetLifecycleState.ACTIVE.value,
                    iceberg_sources.c.lifecycle_state == SourceLifecycleState.ACTIVE.value,
                )
                .with_for_update(of=datasets)
                .limit(2)
            ).mappings()
        )
        if len(rows) != 1:
            raise StateTransitionError("dataset route is absent or ambiguous")
        dataset_row: RowMapping = rows[0]
        state_row: RowMapping = (
            connection.execute(
                sa.select(dataset_state)
                .where(dataset_state.c.dataset_id == dataset_row["dataset_id"])
                .with_for_update()
            )
            .mappings()
            .one()
        )
        return dataset_row, state_row

    def enqueue_rebuild(self, identity: RoutingIdentity, request_id: uuid.UUID) -> uuid.UUID:
        """Enqueue one idempotent isolated rebuild of the current source state.

        Args:
            identity: Validated logical dataset route.
            request_id: Stable operator request identity.

        Returns:
            Deterministic rebuild work identity.
        """
        identity.validate()
        current: datetime = utc_now()
        with self.engine.begin() as connection:
            dataset_row: RowMapping
            state_row: RowMapping
            dataset_row, state_row = self.dataset_and_state_for_identity(connection, identity)
            if state_row["last_applied_source_snapshot_seq"] is None or state_row["ingest_lance_version"] is None:
                raise StateTransitionError("dataset has no materialized source state to rebuild")
            work_id: uuid.UUID = deterministic_rebuild_work_id(dataset_row["dataset_id"], request_id)
            source: IcebergSource = self.source_for_dataset(connection, dataset_row["dataset_id"])
            candidate_uri: str = rebuild_uri(source.lance_base_uri, dataset_row["dataset_id"], work_id)
            source_snapshot_seq: int = int(state_row["last_applied_source_snapshot_seq"])
            connection.execute(
                postgresql.insert(dataset_work)
                .values(
                    work_id=work_id,
                    dataset_id=dataset_row["dataset_id"],
                    source_id=dataset_row["source_id"],
                    source_snapshot_seq=source_snapshot_seq,
                    spec_revision_id=dataset_row["desired_spec_revision_id"],
                    kind=WorkKind.REBUILD.value,
                    state=WorkState.PENDING.value,
                    phase=WorkPhase.COMPACT.value,
                    next_attempt_at=current,
                    expected_ingest_lance_uri=state_row["ingest_lance_uri"],
                    expected_ingest_lance_version=state_row["ingest_lance_version"],
                    expected_active_publication_id=state_row["active_publication_id"],
                    candidate_lance_uri=candidate_uri,
                    candidate_lance_version=0,
                )
                .on_conflict_do_nothing(index_elements=[dataset_work.c.work_id])
            )
            return work_id

    def select_retention_floor(self, connection: Connection, current: datetime) -> RowMapping | None:
        """Select the earliest snapshot needed by replay, blocked, or open work.

        Args:
            connection: Read connection.
            current: Status clock.

        Returns:
            Earliest retained snapshot mapping or ``None``.
        """
        open_work: Any = sa.exists(
            sa.select(sa.literal(1)).where(
                dataset_work.c.source_snapshot_seq == source_snapshots.c.source_snapshot_seq,
                dataset_work.c.state != WorkState.SUCCEEDED.value,
            )
        )
        replay_age_seconds: Any = sa.extract("epoch", sa.literal(current) - source_snapshots.c.created_at)
        return (
            connection.execute(
                sa.select(source_snapshots)
                .join(iceberg_sources, iceberg_sources.c.source_id == source_snapshots.c.source_id)
                .where(
                    sa.or_(
                        source_snapshots.c.state == SourceSnapshotState.BLOCKED.value,
                        open_work,
                        replay_age_seconds <= iceberg_sources.c.replay_horizon_seconds,
                    )
                )
                .order_by(source_snapshots.c.source_snapshot_seq)
                .limit(1)
            )
            .mappings()
            .one_or_none()
        )

    def retention_floor(self, now: datetime | None = None) -> RowMapping | None:
        """Read the exact oldest source snapshot that Iceberg expiration must preserve.

        Args:
            now: Optional deterministic status clock.

        Returns:
            Retained source snapshot or ``None``.
        """
        with self.engine.connect() as connection:
            return self.select_retention_floor(connection, now or utc_now())

    def control_plane_status(self, now: datetime | None = None) -> ControlPlaneStatus:
        """Read a constant-size queue, blockage, and retention snapshot.

        Args:
            now: Optional deterministic status clock.

        Returns:
            Bounded operational status.
        """
        current: datetime = now or utc_now()
        with self.engine.connect() as connection:
            state_counts: dict[str, int] = {
                str(row["state"]): int(row["count"])
                for row in connection.execute(
                    sa.select(dataset_work.c.state, sa.func.count().label("count")).group_by(dataset_work.c.state)
                ).mappings()
            }
            due_work: int = int(
                connection.scalar(
                    sa.select(sa.func.count()).select_from(dataset_work).where(self.due_predicate(current))
                )
                or 0
            )
            blocked_snapshots: int = int(
                connection.scalar(
                    sa.select(sa.func.count())
                    .select_from(source_snapshots)
                    .where(source_snapshots.c.state == SourceSnapshotState.BLOCKED.value)
                )
                or 0
            )
            oldest_open: datetime | None = connection.scalar(
                sa.select(sa.func.min(dataset_work.c.created_at)).where(
                    dataset_work.c.state != WorkState.SUCCEEDED.value
                )
            )
            floor: RowMapping | None = self.select_retention_floor(connection, current)
        return ControlPlaneStatus(
            pending_work=state_counts.get(WorkState.PENDING.value, 0),
            running_work=state_counts.get(WorkState.RUNNING.value, 0),
            retry_wait_work=state_counts.get(WorkState.RETRY_WAIT.value, 0),
            blocked_work=state_counts.get(WorkState.BLOCKED.value, 0),
            due_work=due_work,
            blocked_source_snapshots=blocked_snapshots,
            oldest_open_work_at=oldest_open,
            retention_source_snapshot_seq=int(floor["source_snapshot_seq"]) if floor is not None else None,
            retention_snapshot_id=int(floor["snapshot_id"]) if floor is not None else None,
            retention_parent_snapshot_id=floor["parent_snapshot_id"] if floor is not None else None,
            retention_state=SourceSnapshotState(str(floor["state"])) if floor is not None else None,
            retention_created_at=floor["created_at"] if floor is not None else None,
        )

    def claim_publication_cleanup(
        self,
        current: datetime,
        limit: int,
    ) -> list[PublicationCleanup]:
        """Select a bounded batch of retired publications beyond per-spec retention.

        Args:
            current: Fixed transaction clock used with each revision's retention policy.
            limit: Maximum returned publications.

        Returns:
            Idempotent external cleanup identities.
        """
        if limit < 1:
            raise ValueError("cleanup limit must be positive")
        ranked: Any = (
            sa.select(
                dataset_publications.c.publication_id,
                dataset_publications.c.work_id,
                dataset_publications.c.dataset_id,
                dataset_publications.c.lance_uri,
                dataset_publications.c.lance_version,
                dataset_publications.c.manifest_uri,
                dataset_publications.c.retired_at,
                dataset_spec_revisions.c.retained_publications,
                dataset_spec_revisions.c.artifact_retention_seconds,
                sa.func.row_number()
                .over(
                    partition_by=dataset_publications.c.dataset_id,
                    order_by=dataset_publications.c.published_at.desc(),
                )
                .label("publication_rank"),
            )
            .join(
                dataset_spec_revisions,
                dataset_spec_revisions.c.spec_revision_id == dataset_publications.c.spec_revision_id,
            )
            .subquery("ranked_publications")
        )
        with self.engine.connect() as connection:
            rows: list[RowMapping] = list(
                connection.execute(
                    sa.select(ranked)
                    .where(
                        ranked.c.retired_at.is_not(None),
                        ranked.c.retired_at
                        + ranked.c.artifact_retention_seconds * sa.cast(sa.literal("1 second"), sa.Interval())
                        <= current,
                        ranked.c.publication_rank > ranked.c.retained_publications,
                        ~sa.exists(
                            sa.select(sa.literal(1)).where(
                                dataset_state.c.active_publication_id == ranked.c.publication_id
                            )
                        ),
                    )
                    .order_by(ranked.c.retired_at, ranked.c.publication_id)
                    .limit(limit)
                ).mappings()
            )
        return [
            PublicationCleanup(
                publication_id=row["publication_id"],
                work_id=row["work_id"],
                dataset_id=row["dataset_id"],
                lance_uri=str(row["lance_uri"]),
                lance_version=int(row["lance_version"]),
                manifest_uri=str(row["manifest_uri"]),
                pin_name=candidate_pin_name(row["work_id"]),
            )
            for row in rows
        ]

    def finalize_publication_cleanup(self, cleanup: PublicationCleanup) -> bool:
        """Delete one retired publication after external cleanup succeeds.

        Args:
            cleanup: Exact retired publication identity.

        Returns:
            Whether the publication is absent afterward.
        """
        with self.engine.begin() as connection:
            active: Any = sa.exists(
                sa.select(sa.literal(1)).where(dataset_state.c.active_publication_id == cleanup.publication_id)
            )
            row: RowMapping | None = (
                connection.execute(
                    sa.select(dataset_publications)
                    .where(dataset_publications.c.publication_id == cleanup.publication_id)
                    .with_for_update()
                )
                .mappings()
                .one_or_none()
            )
            if row is None:
                return True
            expected: tuple[object, ...] = (
                cleanup.work_id,
                cleanup.dataset_id,
                cleanup.lance_uri,
                cleanup.lance_version,
                cleanup.manifest_uri,
            )
            actual: tuple[object, ...] = (
                row["work_id"],
                row["dataset_id"],
                row["lance_uri"],
                row["lance_version"],
                row["manifest_uri"],
            )
            if actual != expected or row["retired_at"] is None:
                raise StateTransitionError("publication cleanup identity differs from durable state")
            connection.execute(
                sa.delete(publication_indexes).where(
                    publication_indexes.c.publication_id == cleanup.publication_id,
                    ~active,
                )
            )
            result: sa.CursorResult[Any] = connection.execute(
                sa.delete(dataset_publications).where(
                    dataset_publications.c.publication_id == cleanup.publication_id,
                    ~active,
                )
            )
            return result.rowcount == 1

    def delete_completed_audit(self, completed_before: datetime, limit: int) -> tuple[int, int]:
        """Prune bounded completed work and unreferenced source snapshots.

        Args:
            completed_before: Fixed audit cutoff.
            limit: Per-table deletion bound.

        Returns:
            Deleted work and source-snapshot row counts.
        """
        if limit < 1:
            raise ValueError("audit deletion limit must be positive")
        with self.engine.begin() as connection:
            work_ids: Any = (
                sa.select(dataset_work.c.work_id)
                .where(
                    dataset_work.c.state == WorkState.SUCCEEDED.value,
                    dataset_work.c.updated_at < completed_before,
                    ~sa.exists(
                        sa.select(sa.literal(1)).where(dataset_publications.c.work_id == dataset_work.c.work_id)
                    ),
                )
                .order_by(dataset_work.c.updated_at, dataset_work.c.work_id)
                .limit(limit)
                .cte("completed_work_ids")
            )
            work_result: sa.CursorResult[Any] = connection.execute(
                sa.delete(dataset_work).where(dataset_work.c.work_id.in_(sa.select(work_ids.c.work_id)))
            )
            snapshot_ids: Any = (
                sa.select(source_snapshots.c.source_snapshot_seq)
                .where(
                    source_snapshots.c.state == SourceSnapshotState.COMPLETE.value,
                    source_snapshots.c.updated_at < completed_before,
                    ~sa.exists(
                        sa.select(sa.literal(1)).where(
                            dataset_work.c.source_snapshot_seq == source_snapshots.c.source_snapshot_seq
                        )
                    ),
                    ~sa.exists(
                        sa.select(sa.literal(1)).where(
                            dataset_publications.c.source_snapshot_seq == source_snapshots.c.source_snapshot_seq
                        )
                    ),
                    ~sa.exists(
                        sa.select(sa.literal(1)).where(
                            dataset_state.c.last_applied_source_snapshot_seq == source_snapshots.c.source_snapshot_seq
                        )
                    ),
                )
                .order_by(source_snapshots.c.source_snapshot_seq)
                .limit(limit)
                .cte("completed_snapshot_ids")
            )
            snapshot_result: sa.CursorResult[Any] = connection.execute(
                sa.delete(source_snapshots).where(
                    source_snapshots.c.source_snapshot_seq.in_(sa.select(snapshot_ids.c.source_snapshot_seq))
                )
            )
            return int(work_result.rowcount or 0), int(snapshot_result.rowcount or 0)
