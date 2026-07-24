"""Concrete Spark and Iceberg metadata adapters for scheduled source planning."""

from __future__ import annotations

import re
import uuid
from collections import deque
from collections.abc import Callable, Iterator, Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime
from functools import partial
from typing import Any, Protocol

import pyarrow as pa
from pyspark.errors import AnalysisException, IllegalArgumentException
from pyspark.sql import Column, DataFrame, Row, SparkSession
from pyspark.sql.functions import col, countDistinct, exists, isnan, lit, lower, map_values, trim, when
from pyspark.sql.functions import max as spark_max
from pyspark.sql.types import ArrayType, DataType, FloatType, MapType, StringType, TimestampType

from lance_etl.etl.digest import canonical_event_digest
from lance_etl.etl.mutation import DELETE_OPERATIONS, UPSERT_OPERATIONS, normalize_operation
from lance_etl.routing import validate_routing_segment
from lance_etl.source import (
    BaselineProof,
    MaintenanceTrust,
    ManifestContent,
    ManifestEntry,
    ManifestStatus,
    PartitionField,
    PartitionSpec,
    PartitionValues,
    SnapshotRecord,
    SourceCheckpoint,
    SourcePlan,
    SourceSnapshotRejection,
    TableMetadata,
    TargetKey,
    WindowKind,
    WindowPlan,
)
from lance_etl.source.contract import validate_table_contract
from lance_etl.source.errors import (
    SourceBaselineError,
    SourceConfigurationError,
    SourceContractError,
    SourceLineageError,
    SourceSnapshotBlockedError,
    blocked_snapshot_error,
)
from lance_etl.source.lineage import index_snapshots, validate_snapshot_identity
from lance_etl.source.manifests import classify_snapshot, discover_added_targets, discover_baseline_targets
from lance_etl.state import IcebergSource, SourceSnapshotKind, SourceSnapshotPlan, SourceSnapshotState

TABLE_IDENTIFIER_PATTERN: re.Pattern[str] = re.compile(
    r"^[A-Za-z_][A-Za-z0-9_]{0,127}(?:\.[A-Za-z_][A-Za-z0-9_]{0,127}){2,129}$"
)
"""Allowlisted catalog-qualified Iceberg identifier contract."""

MANIFEST_STATUS_BY_CODE: dict[int, ManifestStatus] = {
    0: ManifestStatus.EXISTING,
    1: ManifestStatus.ADDED,
    2: ManifestStatus.DELETED,
}
"""Iceberg manifest status integer mapping."""

MANIFEST_CONTENT_BY_CODE: dict[int, ManifestContent] = {
    0: ManifestContent.DATA,
    1: ManifestContent.POSITION_DELETES,
    2: ManifestContent.EQUALITY_DELETES,
}
"""Iceberg data-file content integer mapping."""

MAX_MANIFEST_FACTS_PER_SNAPSHOT: int = 10_000
"""Maximum distinct target-hour facts collected for one source snapshot."""


SnapshotDocumentLoader = Callable[[int], dict[str, Any] | None]
"""Load one immutable snapshot document by ID from a pinned metadata generation."""


@dataclass(frozen=True, slots=True)
class IcebergMetadataPin:
    """Bounded Python projection over one immutable Java Iceberg metadata generation."""

    source_metadata: TableMetadata
    metadata_location: str
    snapshot_loader: SnapshotDocumentLoader = field(compare=False, repr=False)


def table_metadata_from_java(metadata: Any) -> TableMetadata:
    """Normalize the bounded source contract from one Java Iceberg metadata object.

    Args:
        metadata: Immutable ``org.apache.iceberg.TableMetadata`` object.

    Returns:
        Stable table identity, current head, and active partition specification.

    Raises:
        SourceContractError: If required Iceberg metadata is absent or malformed.
    """
    try:
        table_uuid: str = str(metadata.uuid())
        uuid.UUID(table_uuid)
        java_spec: Any = metadata.spec()
        java_schema: Any = metadata.schema()
        spec_id: int = int(metadata.defaultSpecId())
        if int(java_spec.specId()) != spec_id:
            raise ValueError("Iceberg default partition specification is inconsistent")
        fields: list[PartitionField] = []
        java_field: Any
        for java_field in java_spec.fields():
            source_column: Any = java_schema.findColumnName(int(java_field.sourceId()))
            if source_column is None:
                raise ValueError("Iceberg partition source column is missing")
            fields.append(
                PartitionField(
                    field_name=str(java_field.name()),
                    source_column=str(source_column),
                    transform=str(java_field.transform()),
                )
            )
        current_snapshot: Any = metadata.currentSnapshot()
        return TableMetadata(
            table_uuid=table_uuid,
            current_snapshot_id=int(current_snapshot.snapshotId()) if current_snapshot is not None else None,
            active_partition_spec=PartitionSpec(spec_id, tuple(fields)),
        )
    except (AttributeError, TypeError, ValueError) as exc:
        raise SourceContractError("Iceberg table metadata violates the required schema contract") from exc


def snapshot_document_from_java_metadata(metadata: Any, snapshot_id: int) -> dict[str, Any] | None:
    """Load one bounded snapshot projection from pinned Java Iceberg metadata.

    Args:
        metadata: Immutable ``org.apache.iceberg.TableMetadata`` object.
        snapshot_id: Exact retained snapshot identity.

    Returns:
        Normalized snapshot document, or ``None`` when the snapshot is not retained.

    Raises:
        SourceLineageError: If the retained snapshot metadata is malformed.
    """
    java_snapshot: Any = metadata.snapshot(snapshot_id)
    if java_snapshot is None:
        return None
    try:
        raw_summary: dict[Any, Any] = dict(java_snapshot.summary())
        summary: dict[str, str] = {str(key): str(value) for key, value in raw_summary.items()}
        operation: Any = java_snapshot.operation()
        if operation is None:
            raise ValueError("Iceberg snapshot operation is missing")
        summary["operation"] = str(operation)
        parent: Any = java_snapshot.parentId()
        return {
            "snapshot-id": int(java_snapshot.snapshotId()),
            "parent-snapshot-id": int(parent) if parent is not None else None,
            "sequence-number": int(java_snapshot.sequenceNumber()),
            "timestamp-ms": int(java_snapshot.timestampMillis()),
            "summary": summary,
        }
    except (AttributeError, TypeError, ValueError) as exc:
        raise SourceLineageError("Iceberg snapshot metadata violates the required schema") from exc


def current_iceberg_metadata(spark: SparkSession, table: str) -> IcebergMetadataPin:
    """Pin the catalog-current Java Iceberg metadata with a bounded Python projection.

    This is the single documented bridge to PySpark's internal Java gateway. Iceberg metadata-log
    timestamps cannot identify the current file because writer clock skew is permitted. Loading the
    table through ``Spark3Util`` and reading ``operations().current()`` uses the catalog's actual current
    metadata pointer. The immutable Java object supplies random-access ancestry without decoding the
    full retained snapshot history into Python.

    Args:
        spark: Active Spark session.
        table: Validated catalog-qualified table.

    Returns:
        Exact metadata pin with only bounded Python state.

    Raises:
        RuntimeError: If the catalog returns no metadata location.
    """
    java_table: Any = spark._jvm.org.apache.iceberg.spark.Spark3Util.loadIcebergTable(
        spark._jsparkSession,
        table,
    )
    java_metadata: Any = java_table.operations().current()
    if java_metadata is None:
        raise RuntimeError("Iceberg catalog exposes no current metadata")
    metadata_location: str = str(java_metadata.metadataFileLocation())
    if not metadata_location:
        raise RuntimeError("Iceberg catalog exposes no current metadata file")
    return IcebergMetadataPin(
        source_metadata=table_metadata_from_java(java_metadata),
        metadata_location=metadata_location,
        snapshot_loader=partial(snapshot_document_from_java_metadata, java_metadata),
    )


class SourceCheckpointRepository(Protocol):
    """Structural interface for reading the durable audit tip."""

    def latest_source_snapshot(self, source_id: uuid.UUID) -> Mapping[str, object] | None:
        """Return the newest durable source snapshot.

        Args:
            source_id: PostgreSQL-owned source identity.

        Returns:
            Mapping-like row or ``None``.
        """
        ...

    def enqueue_blocked_source_snapshot(self, plan: SourceSnapshotPlan, error_code: str) -> int:
        """Persist a rejected exact next snapshot without child work.

        Args:
            plan: Rejected snapshot identity.
            error_code: Bounded deterministic classification.

        Returns:
            Durable rejected source-snapshot sequence.
        """
        ...

    def block_source_snapshot(
        self,
        source_snapshot_seq: int,
        error_code: str,
        error_message: str | None = None,
        expected_planning_epoch: int | None = None,
    ) -> bool:
        """Gate the latest exact safe audit tip when no next identity is trustworthy.

        Args:
            source_snapshot_seq: Latest safe durable audit sequence.
            error_code: Bounded deterministic classification.
            error_message: Optional bounded diagnostic.
            expected_planning_epoch: Optional planner-observed source epoch used as an ABA fence.

        Returns:
            Whether the durable fail-closed gate exists.
        """
        ...


@dataclass(frozen=True, slots=True)
class BaselineQualifier:
    """Prove one exact initial snapshot has canonical raw mutation identity.

    Baseline qualification deliberately validates only the shared Iceberg envelope. Dataset-specific
    field, vector-dimension, and index contracts belong to the immutable spec revision frozen on
    each dataset work item. This keeps a single source baseline valid for heterogeneous datasets.
    """

    spark: SparkSession

    def qualify(self, registered_source: IcebergSource, metadata: TableMetadata, snapshot_id: int) -> BaselineProof:
        """Scan and validate the exact baseline with executor-computed canonical digests.

        Args:
            registered_source: PostgreSQL-owned source registration.
            metadata: Pinned table UUID and partition specification.
            snapshot_id: Explicit retained baseline snapshot.

        Returns:
            Proof tied to the exact table, snapshot, and active partition specification.
        """
        source_frame: DataFrame = (
            self.spark.read.format("iceberg")
            .option("snapshot-id", str(snapshot_id))
            .load(registered_source.spark_table)
        )
        self.validate_contract(source_frame)

        def digest_batches(batches: Iterator[pa.RecordBatch]) -> Iterator[pa.RecordBatch]:
            """Compute canonical mutation digests for bounded Arrow source batches.

            Args:
                batches: Exact-snapshot source batches.

            Yields:
                Routing key, record ID, and canonical digest batches.
            """
            output_schema: pa.Schema = pa.schema(
                [
                    pa.field("tenant_id", pa.string()),
                    pa.field("namespace", pa.string()),
                    pa.field("org_id", pa.string()),
                    pa.field("record_id", pa.string()),
                    pa.field("mutation_digest", pa.binary()),
                ]
            )
            batch: pa.RecordBatch
            for batch in batches:
                output: list[dict[str, object]] = []
                row: dict[str, Any]
                for row in pa.Table.from_batches([batch]).to_pylist():
                    operation: str = normalize_operation(str(row["op"]))
                    payload: dict[str, object] = {
                        "vectors": dict(row.get("vectors") or {}),
                        "texts": dict(row.get("texts") or {}),
                        "metadata": dict(row.get("metadata") or {}),
                    }
                    target: tuple[str, str, str] = (
                        str(row["tenant_id"]),
                        str(row["namespace"]),
                        str(row["org_id"]),
                    )
                    digest: bytes = canonical_event_digest(
                        target,
                        str(row["record_id"]),
                        operation,
                        row["ts"],
                        payload,
                    )
                    output.append(
                        {
                            "tenant_id": target[0],
                            "namespace": target[1],
                            "org_id": target[2],
                            "record_id": row["record_id"],
                            "mutation_digest": digest,
                        }
                    )
                yield from pa.Table.from_pylist(output, schema=output_schema).to_batches()

        digests: DataFrame = source_frame.select(
            "tenant_id",
            "namespace",
            "org_id",
            "record_id",
            "op",
            "ts",
            "vectors",
            "texts",
            "metadata",
        ).mapInArrow(
            digest_batches,
            "tenant_id string, namespace string, org_id string, record_id string, mutation_digest binary",
        )
        conflicts: int = (
            digests.groupBy("tenant_id", "namespace", "org_id", "record_id")
            .agg(countDistinct("mutation_digest").alias("distinct_mutations"))
            .where(col("distinct_mutations") > 1)
            .count()
        )
        return BaselineProof(
            metadata.table_uuid,
            snapshot_id,
            metadata.active_partition_spec.spec_id,
            conflicts == 0,
            conflicts,
        )

    def validate_contract(self, source_frame: Any) -> None:
        """Reject deterministic baseline contract violations before digest aggregation.

        Args:
            source_frame: Exact baseline Spark DataFrame.

        Raises:
            ValueError: If source shape, operation, or routing fields are invalid.
        """
        required: set[str] = {
            "tenant_id",
            "namespace",
            "org_id",
            "record_id",
            "op",
            "ts",
            "vectors",
            "texts",
            "metadata",
        }
        missing: list[str] = sorted(required - set(source_frame.columns))
        if missing:
            raise ValueError(f"baseline source is missing required columns {missing}")
        string_columns: tuple[str, ...] = (
            "tenant_id",
            "namespace",
            "org_id",
            "record_id",
            "op",
        )
        if any(not isinstance(source_frame.schema[name].dataType, StringType) for name in string_columns):
            raise ValueError("baseline routing, record_id, and operation columns must be strings")
        if not isinstance(source_frame.schema["ts"].dataType, TimestampType):
            raise ValueError("baseline timestamp column must be a timestamp")
        vectors_type: DataType = source_frame.schema["vectors"].dataType
        texts_type: DataType = source_frame.schema["texts"].dataType
        metadata_type: DataType = source_frame.schema["metadata"].dataType
        vectors_valid: bool = (
            isinstance(vectors_type, MapType)
            and isinstance(vectors_type.keyType, StringType)
            and isinstance(vectors_type.valueType, ArrayType)
            and isinstance(vectors_type.valueType.elementType, FloatType)
        )
        strings_valid: bool = all(
            isinstance(map_type, MapType)
            and isinstance(map_type.keyType, StringType)
            and isinstance(map_type.valueType, StringType)
            for map_type in (texts_type, metadata_type)
        )
        if not vectors_valid or not strings_valid:
            raise ValueError("baseline maps must match vectors<string,array<float>> and text metadata string maps")
        nonnull: tuple[str, ...] = (
            "tenant_id",
            "namespace",
            "org_id",
            "record_id",
            "op",
            "ts",
        )
        operation: Column = lower(trim(col("op")))
        supported: tuple[str, ...] = tuple(sorted(UPSERT_OPERATIONS | DELETE_OPERATIONS))
        invalid_vector_element: Column = exists(
            map_values(col("vectors")),
            lambda vector: exists(
                vector,
                lambda value: (
                    value.isNull() | isnan(value) | (value == lit(float("inf"))) | (value == lit(float("-inf")))
                ),
            ),
        )
        checks: list[tuple[Column, str]] = [
            (
                any_null_expression(nonnull),
                "baseline source contains null routing, identity, operation, or timestamp",
            ),
            (~operation.isin(*supported), "baseline source contains an unsupported mutation operation"),
            (invalid_vector_element, "baseline source contains a null or non-finite vector element"),
        ]
        summary: Row | None = source_frame.agg(
            *(
                spark_max(when(check[0], lit(1)).otherwise(lit(0))).alias(f"invalid_{index}")
                for index, check in enumerate(checks)
            )
        ).first()
        if summary is None:
            raise RuntimeError("baseline source validation produced no aggregate result")
        for index, check in enumerate(checks):
            if int(summary[index] or 0):
                raise ValueError(check[1])


def any_null_expression(columns: tuple[str, ...]) -> Any:
    """Build a typed OR of null checks for required Spark columns.

    Args:
        columns: Required source column names.

    Returns:
        Spark boolean column expression.
    """
    expression: Column = col(columns[0]).isNull()
    name: Any
    for name in columns[1:]:
        expression = expression | col(name).isNull()
    return expression


def snapshot_summary(snapshot: Mapping[str, Any]) -> Mapping[str, Any]:
    """Return a validated Iceberg snapshot summary mapping.

    Args:
        snapshot: Parsed snapshot metadata object.

    Returns:
        Snapshot summary mapping, or an empty mapping when absent.

    Raises:
        SourceLineageError: If the summary has an invalid shape.
    """
    summary: Any = snapshot.get("summary")
    if summary is None:
        return {}
    if not isinstance(summary, Mapping):
        raise SourceLineageError("Iceberg snapshot summary is not an object")
    return summary


def snapshot_document_id(snapshot: Mapping[str, Any]) -> int:
    """Return one validated snapshot document identity.

    Args:
        snapshot: Bounded snapshot metadata projection.

    Returns:
        Exact snapshot ID.

    Raises:
        SourceLineageError: If the snapshot identity is malformed.
    """
    try:
        return int(snapshot["snapshot-id"])
    except (KeyError, TypeError, ValueError) as exc:
        raise SourceLineageError("Iceberg snapshot metadata has an invalid snapshot ID") from exc


def snapshot_document_parent_id(snapshot: Mapping[str, Any]) -> int | None:
    """Return one validated direct-parent identity.

    Args:
        snapshot: Bounded snapshot metadata projection.

    Returns:
        Direct parent ID, or ``None`` at a lineage root.

    Raises:
        SourceLineageError: If the parent identity is malformed.
    """
    parent: Any = snapshot.get("parent-snapshot-id")
    if parent is None:
        return None
    try:
        return int(parent)
    except (TypeError, ValueError) as exc:
        raise SourceLineageError("Iceberg snapshot metadata has an invalid parent ID") from exc


def next_snapshot_id(loader: SnapshotDocumentLoader, snapshot_id: int | None) -> int | None:
    """Resolve one direct-parent hop for constant-memory cycle detection.

    Args:
        loader: Snapshot lookup bound to one immutable metadata generation.
        snapshot_id: Current snapshot ID, or ``None`` at the lineage root.

    Returns:
        Direct parent ID, or ``None`` when the snapshot is absent or has no parent.
    """
    if snapshot_id is None:
        return None
    snapshot: dict[str, Any] | None = loader(snapshot_id)
    return snapshot_document_parent_id(snapshot) if snapshot is not None else None


def iter_snapshot_documents(
    loader: SnapshotDocumentLoader,
    pinned_head_snapshot_id: int,
    stop_snapshot_id: int,
) -> Iterator[dict[str, Any]]:
    """Yield one pinned direct-parent ancestry with constant Python memory.

    Floyd cycle detection avoids retaining every visited snapshot ID. Missing ancestors terminate
    the walk and remain visible to the caller as absent stopping evidence.

    Args:
        loader: Snapshot lookup bound to one immutable metadata generation.
        pinned_head_snapshot_id: Inclusive pinned ancestry head.
        stop_snapshot_id: Inclusive stopping ancestor.

    Yields:
        Snapshot documents ordered from head toward the stopping ancestor.

    Raises:
        SourceLineageError: If metadata identities conflict or the parent chain cycles.
    """
    cursor: int = pinned_head_snapshot_id
    slow: int | None = pinned_head_snapshot_id
    fast: int | None = pinned_head_snapshot_id
    while True:
        snapshot: dict[str, Any] | None = loader(cursor)
        if snapshot is None:
            return
        if snapshot_document_id(snapshot) != cursor:
            raise SourceLineageError("Iceberg snapshot lookup returned a conflicting identity")
        yield snapshot
        if cursor == stop_snapshot_id:
            return
        parent_snapshot_id: int | None = snapshot_document_parent_id(snapshot)
        if parent_snapshot_id is None:
            return
        cursor = parent_snapshot_id
        slow = next_snapshot_id(loader, slow)
        fast = next_snapshot_id(loader, next_snapshot_id(loader, fast))
        if slow is not None and slow == fast:
            raise SourceLineageError("Iceberg snapshot parent chain contains a cycle")


def bounded_snapshot_documents(
    loader: SnapshotDocumentLoader,
    pinned_head_snapshot_id: int,
    stop_snapshot_id: int,
    descendant_limit: int | None,
    retain_head: bool,
) -> tuple[dict[str, Any], ...]:
    """Retain only the requested ancestry tail while streaming a pinned parent walk.

    Args:
        loader: Snapshot lookup bound to one immutable metadata generation.
        pinned_head_snapshot_id: Inclusive pinned ancestry head.
        stop_snapshot_id: Inclusive stopping ancestor.
        descendant_limit: Optional maximum descendants nearest the stopping ancestor.
        retain_head: Whether to preserve the pinned head alongside a bounded tail.

    Returns:
        Requested ancestry projection in head-to-ancestor order.

    Raises:
        ValueError: If ``descendant_limit`` is negative.
    """
    if descendant_limit is not None and descendant_limit < 0:
        raise ValueError("descendant_limit must be non-negative")
    if descendant_limit is None:
        return tuple(iter_snapshot_documents(loader, pinned_head_snapshot_id, stop_snapshot_id))
    retained: deque[dict[str, Any]] = deque(maxlen=descendant_limit + 1)
    head_snapshot: dict[str, Any] | None = None
    snapshot: dict[str, Any]
    for snapshot in iter_snapshot_documents(loader, pinned_head_snapshot_id, stop_snapshot_id):
        if head_snapshot is None:
            head_snapshot = snapshot
        retained.append(snapshot)
    result: tuple[dict[str, Any], ...] = tuple(retained)
    if (
        retain_head
        and head_snapshot is not None
        and all(snapshot_document_id(item) != pinned_head_snapshot_id for item in result)
    ):
        return (head_snapshot, *result)
    return result


def snapshot_from_document(table_uuid: str, spec_id: int, snapshot: Mapping[str, Any]) -> SnapshotRecord:
    """Normalize one snapshot object from Iceberg metadata JSON.

    Args:
        table_uuid: Stable table identity.
        spec_id: Resolved snapshot partition specification.
        snapshot: Parsed Iceberg snapshot object.

    Returns:
        Typed snapshot record.

    Raises:
        SourceLineageError: If the snapshot lacks an operation.
    """
    summary: Mapping[str, Any] = snapshot_summary(snapshot)
    operation: Any = summary.get("operation")
    if operation is None:
        raise SourceLineageError("Iceberg snapshot metadata lacks an operation")
    parent: Any = snapshot.get("parent-snapshot-id")
    try:
        return SnapshotRecord(
            table_uuid=table_uuid,
            snapshot_id=int(snapshot["snapshot-id"]),
            parent_snapshot_id=int(parent) if parent is not None else None,
            sequence_number=int(snapshot["sequence-number"]),
            committed_at_ms=int(snapshot["timestamp-ms"]),
            operation=str(operation),
            partition_spec_id=spec_id,
            summary=tuple(sorted((str(key), str(value)) for key, value in summary.items())),
        )
    except (KeyError, TypeError, ValueError) as exc:
        raise SourceLineageError("Iceberg snapshot metadata violates the required schema") from exc


def collect_manifest_rows(selected: DataFrame, snapshot_id: int) -> list[Row]:
    """Collect distinct manifest facts under a strict driver-memory bound.

    Args:
        selected: Distinct target-hour manifest-fact frame.
        snapshot_id: Exact source snapshot used for durable rejection evidence.

    Returns:
        At most :data:`MAX_MANIFEST_FACTS_PER_SNAPSHOT` rows.

    Raises:
        SourceSnapshotBlockedError: If the snapshot exceeds the local planning budget.
    """
    rows: list[Row] = selected.limit(MAX_MANIFEST_FACTS_PER_SNAPSHOT + 1).collect()
    if len(rows) > MAX_MANIFEST_FACTS_PER_SNAPSHOT:
        raise blocked_snapshot_error(
            snapshot_id,
            "SOURCE_MANIFEST_FACT_LIMIT",
            "source snapshot exceeds the bounded manifest-fact planning budget",
        )
    return rows


def bounded_incremental_candidates(
    snapshots: tuple[SnapshotRecord, ...],
    checkpoint: SourceCheckpoint,
) -> tuple[SnapshotRecord, ...]:
    """Validate a bounded ancestry suffix and return direct descendants in commit order.

    Args:
        snapshots: Catalog records ordered from the bounded suffix head through the checkpoint.
        checkpoint: Durable audit tip anchoring the suffix.

    Returns:
        Direct descendants ordered oldest to newest.

    Raises:
        SourceLineageError: If the checkpoint evidence or direct-parent chain is inconsistent.
    """
    indexed: dict[int, SnapshotRecord] = index_snapshots(snapshots)
    retained: SnapshotRecord | None = indexed.get(checkpoint.snapshot_id)
    if retained is None or snapshots[-1] != retained:
        raise SourceLineageError("durable Iceberg checkpoint is missing from pinned ancestry")
    validate_snapshot_identity(retained, checkpoint.table_uuid, checkpoint.partition_spec_id)
    if retained.sequence_number != checkpoint.sequence_number:
        raise SourceLineageError("durable checkpoint sequence does not match Iceberg snapshot metadata")
    candidates: tuple[SnapshotRecord, ...] = tuple(reversed(snapshots[:-1]))
    parent_snapshot_id: int = checkpoint.snapshot_id
    candidate: SnapshotRecord
    for candidate in candidates:
        if candidate.parent_snapshot_id != parent_snapshot_id:
            raise SourceLineageError("bounded Iceberg ancestry is not a direct parent chain")
        parent_snapshot_id = candidate.snapshot_id
    return candidates


def incremental_snapshot_error(
    snapshot: SnapshotRecord,
    metadata: TableMetadata,
    previous_sequence: int,
) -> str | None:
    """Return a bounded identity or sequence rejection code for one descendant.

    Args:
        snapshot: Candidate direct descendant.
        metadata: Cycle-pinned table identity and active partition specification.
        previous_sequence: Sequence number of the preceding direct ancestor.

    Returns:
        Rejection code, or ``None`` when immutable identity and ordering are valid.
    """
    try:
        validate_snapshot_identity(snapshot, metadata.table_uuid, metadata.active_partition_spec.spec_id)
    except SourceLineageError:
        return "SOURCE_SNAPSHOT_IDENTITY"
    return "SOURCE_SEQUENCE_ORDER" if snapshot.sequence_number <= previous_sequence else None


@dataclass(frozen=True, slots=True)
class SparkIcebergCatalog:
    """Read exact Iceberg metadata and partition deltas through Spark metadata tables."""

    spark: SparkSession
    baseline_snapshot_id: int | None = None
    planning_metadata: dict[str, IcebergMetadataPin] = field(default_factory=dict, compare=False, repr=False)

    def validate_table(self, table: str) -> str:
        """Validate one deployment-owned catalog identifier before Spark access.

        Args:
            table: Catalog-qualified table.

        Returns:
            Validated identifier.

        Raises:
            ValueError: If the identifier is not a bounded dotted name.
        """
        if len(table) > 514 or TABLE_IDENTIFIER_PATTERN.fullmatch(table) is None:
            raise ValueError("source table must contain bounded ASCII catalog, namespace, and table identifiers")
        return table

    def begin_planning(self, table: str) -> None:
        """Pin one metadata document for all reads in a planning cycle.

        Args:
            table: Catalog-qualified table.

        Raises:
            RuntimeError: If a planning cycle is already active for the table.
        """
        validated: str = self.validate_table(table)
        if validated in self.planning_metadata:
            raise RuntimeError("an Iceberg planning cycle is already active for the source table")
        self.planning_metadata[validated] = self.read_metadata(validated)

    def end_planning(self, table: str) -> None:
        """Release a cycle-scoped metadata pin.

        Args:
            table: Catalog-qualified table.
        """
        validated: str = self.validate_table(table)
        self.planning_metadata.pop(validated, None)

    def metadata(self, table: str) -> IcebergMetadataPin:
        """Return the cycle-pinned metadata or pin the current Iceberg generation.

        Args:
            table: Catalog-qualified table.

        Returns:
            Exact immutable metadata pin with bounded Python state.

        Raises:
            RuntimeError: If the catalog exposes no current metadata file.
        """
        validated: str = self.validate_table(table)
        pinned: IcebergMetadataPin | None = self.planning_metadata.get(validated)
        return pinned if pinned is not None else self.read_metadata(validated)

    def read_metadata(self, table: str) -> IcebergMetadataPin:
        """Pin one catalog-current immutable Iceberg metadata generation.

        Args:
            table: Validated catalog-qualified table.

        Returns:
            Exact metadata pin with bounded Python state.

        Raises:
            RuntimeError: If the catalog exposes no current metadata file.
        """
        validated: str = self.validate_table(table)
        return current_iceberg_metadata(self.spark, validated)

    def table_metadata(self, table: str) -> TableMetadata:
        """Return stable UUID, current head, and exact active partition spec.

        Args:
            table: Catalog-qualified table.

        Returns:
            Typed source table metadata.
        """
        return self.metadata(table).source_metadata

    def snapshots_through(
        self,
        table: str,
        pinned_head_snapshot_id: int,
        stop_snapshot_id: int,
        descendant_limit: int | None = None,
        retain_head: bool = False,
    ) -> tuple[SnapshotRecord, ...]:
        """Walk direct parents from one metadata document and one manifest aggregation.

        Args:
            table: Catalog-qualified table.
            pinned_head_snapshot_id: Inclusive head pinned by the planner.
            stop_snapshot_id: Inclusive stopping ancestor.
            descendant_limit: Optional maximum descendants nearest the stopping ancestor. The
                stopping record is retained in addition to this limit.
            retain_head: Whether to retain the pinned head in addition to a bounded ancestry tail.

        Returns:
            Snapshot records needed for only this bounded ancestry.
        """
        validated: str = self.validate_table(table)
        pin: IcebergMetadataPin = self.metadata(validated)
        metadata: TableMetadata = pin.source_metadata
        snapshot_documents: tuple[dict[str, Any], ...] = bounded_snapshot_documents(
            pin.snapshot_loader,
            pinned_head_snapshot_id,
            stop_snapshot_id,
            descendant_limit,
            retain_head,
        )
        spec_ids: dict[int, int] = self.snapshot_partition_spec_ids(
            validated,
            snapshot_documents,
            metadata.active_partition_spec.spec_id,
        )
        return tuple(
            snapshot_from_document(
                metadata.table_uuid,
                spec_ids[int(snapshot["snapshot-id"])],
                snapshot,
            )
            for snapshot in snapshot_documents
        )

    def snapshot_partition_spec_ids(
        self,
        table: str,
        snapshots: tuple[dict[str, Any], ...],
        fallback_spec_id: int,
    ) -> dict[int, int]:
        """Resolve historical snapshot specs with at most one Spark action.

        Args:
            table: Validated Iceberg table identifier.
            snapshots: Bounded metadata-document ancestry.
            fallback_spec_id: Current active spec used only for snapshots with no added manifests.

        Returns:
            Mapping from snapshot ID to exact added-manifest spec or the safe active fallback.

        Raises:
            SourceContractError: If one snapshot adds manifests under multiple specs.
        """
        resolved: dict[int, int] = {}
        unresolved: list[int] = []
        snapshot: dict[str, Any]
        for snapshot in snapshots:
            snapshot_id: int = int(snapshot["snapshot-id"])
            summary: Mapping[str, Any] = snapshot_summary(snapshot)
            summary_spec: Any = summary.get("partition-spec-id", summary.get("partition_spec_id"))
            if summary_spec is None:
                unresolved.append(snapshot_id)
            else:
                try:
                    resolved[snapshot_id] = int(summary_spec)
                except (TypeError, ValueError) as exc:
                    raise SourceContractError("snapshot partition specification metadata is malformed") from exc
        if unresolved:
            try:
                manifests: DataFrame = self.spark.read.format("iceberg").load(f"{table}.all_manifests")
                rows: list[Row] = (
                    manifests.where(col("added_snapshot_id").isin(unresolved))
                    .groupBy(col("added_snapshot_id").cast("long").alias("added_snapshot_id"))
                    .agg(
                        countDistinct(col("partition_spec_id")).alias("spec_count"),
                        spark_max(col("partition_spec_id").cast("int")).alias("partition_spec_id"),
                    )
                    .collect()
                )
            except (AnalysisException, IllegalArgumentException) as exc:
                raise SourceContractError("historical partition specification metadata is unreadable") from exc
            row: Row
            for row in rows:
                try:
                    spec_count: int = int(row["spec_count"])
                    added_snapshot_id: int = int(row["added_snapshot_id"])
                    partition_spec_id: int | None = (
                        int(row["partition_spec_id"]) if row["partition_spec_id"] is not None else None
                    )
                except (KeyError, TypeError, ValueError) as exc:
                    raise SourceContractError("historical partition specification row is malformed") from exc
                if spec_count > 1:
                    raise SourceContractError(
                        "one source snapshot adds manifests under multiple partition specifications"
                    )
                if partition_spec_id is not None:
                    resolved[added_snapshot_id] = partition_spec_id
        return {
            int(snapshot["snapshot-id"]): resolved.get(int(snapshot["snapshot-id"]), fallback_spec_id)
            for snapshot in snapshots
        }

    def verify_planning_snapshots(
        self,
        table: str,
        expected_table_uuid: str,
        snapshot_ids: tuple[int, ...],
    ) -> None:
        """Verify live catalog binding and retained ancestry after all snapshot reads.

        The cycle metadata pin protects internal planning consistency, but Spark metadata tables
        and exact snapshot scans still resolve the live catalog name. A fresh metadata read after
        those actions detects table replacement, branch movement, or expiration before any plan is
        handed to PostgreSQL.

        Args:
            table: Catalog-qualified source table.
            expected_table_uuid: Stable UUID from the cycle-pinned metadata document.
            snapshot_ids: Oldest checkpoint followed by every snapshot read for the plan.

        Raises:
            SourceLineageError: If the live binding or retained ancestry no longer matches.
        """
        if not snapshot_ids:
            return
        try:
            pin: IcebergMetadataPin = self.read_metadata(table)
            metadata: TableMetadata = pin.source_metadata
        except (AnalysisException, IllegalArgumentException, RuntimeError, SourceContractError) as exc:
            raise SourceLineageError("live Iceberg table binding could not be revalidated") from exc
        if metadata.table_uuid != expected_table_uuid or metadata.current_snapshot_id is None:
            raise SourceLineageError("live Iceberg table identity changed during source planning")
        remaining: set[int] = set(snapshot_ids)
        snapshot: dict[str, Any]
        for snapshot in iter_snapshot_documents(
            pin.snapshot_loader,
            metadata.current_snapshot_id,
            snapshot_ids[0],
        ):
            remaining.discard(snapshot_document_id(snapshot))
        if remaining:
            raise SourceLineageError("planned Iceberg snapshots left the live retained ancestry")

    def manifest_entries(self, table: str, snapshot_id: int) -> tuple[ManifestEntry, ...]:
        """Aggregate changed files to distinct target-hour manifest facts.

        Args:
            table: Catalog-qualified table.
            snapshot_id: Exact source snapshot.

        Returns:
            Distinct normalized manifest facts, independent of source row count.
        """
        validated: str = self.validate_table(table)
        if snapshot_id == self.baseline_snapshot_id:
            return self.baseline_entries(validated, snapshot_id)
        try:
            entries: DataFrame = (
                self.spark.read.format("iceberg")
                .load(f"{validated}.all_entries")
                .where(col("snapshot_id") == snapshot_id)
            )
            selected: DataFrame = entries.select(
                col("snapshot_id").cast("long").alias("entry_snapshot_id"),
                col("status").cast("int").alias("entry_status"),
                col("data_file.content").cast("int").alias("entry_content"),
                col("data_file.spec_id").cast("int").alias("entry_spec_id"),
                col("data_file.partition.tenant_id").cast("string").alias("tenant_id"),
                col("data_file.partition.namespace").cast("string").alias("namespace"),
                col("data_file.partition.org_id").cast("string").alias("org_id"),
                col("data_file.partition.ts_hour").cast("int").alias("ts_hour"),
            ).distinct()
            return tuple(manifest_from_row(row) for row in collect_manifest_rows(selected, snapshot_id))
        except (AnalysisException, IllegalArgumentException) as exc:
            raise blocked_snapshot_error(
                snapshot_id,
                "SOURCE_SNAPSHOT_NOT_READABLE",
                "source snapshot metadata is no longer readable",
            ) from exc
        except (KeyError, TypeError, ValueError) as exc:
            raise blocked_snapshot_error(
                snapshot_id,
                "SOURCE_MANIFEST_CONTRACT",
                "source snapshot manifest evidence is malformed",
            ) from exc

    def baseline_entries(self, table: str, snapshot_id: int) -> tuple[ManifestEntry, ...]:
        """Return distinct live target-hour facts for one validated canonical baseline.

        Args:
            table: Validated catalog-qualified table.
            snapshot_id: Exact canonical baseline snapshot.

        Returns:
            Normalized live data-file facts.
        """
        try:
            files: DataFrame = (
                self.spark.read.format("iceberg").option("snapshot-id", str(snapshot_id)).load(f"{table}.files")
            )
            selected: DataFrame = files.select(
                col("content").cast("int").alias("entry_content"),
                col("spec_id").cast("int").alias("entry_spec_id"),
                col("partition.tenant_id").cast("string").alias("tenant_id"),
                col("partition.namespace").cast("string").alias("namespace"),
                col("partition.org_id").cast("string").alias("org_id"),
                col("partition.ts_hour").cast("int").alias("ts_hour"),
            ).distinct()
            return tuple(
                manifest_from_row(row, snapshot_id, ManifestStatus.EXISTING)
                for row in collect_manifest_rows(selected, snapshot_id)
            )
        except (AnalysisException, IllegalArgumentException) as exc:
            raise blocked_snapshot_error(
                snapshot_id,
                "SOURCE_SNAPSHOT_NOT_READABLE",
                "baseline snapshot metadata is no longer readable",
            ) from exc
        except (KeyError, TypeError, ValueError) as exc:
            raise blocked_snapshot_error(
                snapshot_id,
                "SOURCE_MANIFEST_CONTRACT",
                "baseline snapshot manifest evidence is malformed",
            ) from exc

    def maintenance_trust(self, table: str, snapshot_id: int) -> MaintenanceTrust | None:
        """Fail closed because generic Spark catalogs cannot authenticate writer identity.

        Args:
            table: Catalog-qualified table.
            snapshot_id: Rewrite snapshot identifier.

        Returns:
            Always ``None`` until an authenticated catalog adapter is selected.
        """
        self.validate_table(table)
        if snapshot_id < 0:
            raise ValueError("snapshot_id must be non-negative")
        return None


def plan_incremental_window(
    catalog: SparkIcebergCatalog,
    table: str,
    snapshot: SnapshotRecord,
) -> WindowPlan:
    """Classify one exact incremental snapshot and discover its target identities.

    Args:
        catalog: Concrete catalog adapter pinned for the current planning cycle.
        table: Validated registered source table.
        snapshot: Exact direct descendant being classified.

    Returns:
        Accepted source window without execution-only Spark scan state.
    """
    entries: tuple[ManifestEntry, ...] = catalog.manifest_entries(table, snapshot.snapshot_id)
    trust: MaintenanceTrust | None = catalog.maintenance_trust(table, snapshot.snapshot_id)
    kind: WindowKind = classify_snapshot(snapshot, entries, trust)
    targets: tuple[TargetKey, ...] = discover_added_targets(snapshot, entries) if kind is WindowKind.APPEND else ()
    return WindowPlan(snapshot, kind, targets)


@dataclass(frozen=True, slots=True)
class DurableSourcePlanProvider:
    """Build the next pinned source plan from Iceberg and the PostgreSQL audit tip."""

    source: IcebergSource
    catalog: SparkIcebergCatalog
    repository: SourceCheckpointRepository
    baseline_qualifier: BaselineQualifier

    def plan(self, max_snapshots: int = 32) -> SourcePlan:
        """Build a bounded accepted prefix or durably record the next rejection.

        One Iceberg metadata document is pinned for the whole call, so backlog planning cannot
        combine a table head from one metadata generation with ancestry from another.

        Args:
            max_snapshots: Maximum accepted windows the enqueuer may consume. One extra window is
                planned as a bounded truncation sentinel.

        Returns:
            Pinned side-effect-free source plan.

        Raises:
            SourceConfigurationError: If initial planning cannot produce trustworthy snapshot evidence.
        """
        row: Mapping[str, object] | None = self.repository.latest_source_snapshot(self.source.source_id)
        if row is not None and bool(row.get("source_blocked", row["state"] == SourceSnapshotState.BLOCKED.value)):
            return self.empty_plan_from_checkpoint(row)
        if max_snapshots <= 0:
            raise ValueError("max_snapshots must be positive")
        table: str = self.source.spark_table
        self.catalog.begin_planning(table)
        try:
            return self.plan_pinned(row, table, max_snapshots)
        except SourceContractError as exc:
            if row is None:
                raise SourceConfigurationError("initial Iceberg metadata violates the source contract") from exc
            source_planning_epoch: int | None = (
                int(row["source_planning_epoch"]) if row.get("source_planning_epoch") is not None else None
            )
            self.gate_checkpoint(
                int(row["source_snapshot_seq"]),
                "SOURCE_TABLE_CONTRACT",
                source_planning_epoch,
            )
            return self.empty_plan_from_checkpoint(row)
        finally:
            self.catalog.end_planning(table)

    def plan_pinned(
        self,
        row: Mapping[str, object] | None,
        table: str,
        max_snapshots: int,
    ) -> SourcePlan:
        """Plan against the catalog document pinned by :meth:`plan`.

        Args:
            row: Latest durable source checkpoint, or ``None`` before baseline.
            table: Validated registered source table.
            max_snapshots: Maximum windows the current cycle may enqueue.

        Returns:
            Pinned source plan containing at most one truncation-sentinel window.
        """
        metadata: TableMetadata = self.catalog.table_metadata(table)
        table_uuid: uuid.UUID = uuid.UUID(metadata.table_uuid)
        if row is None:
            if table_uuid != self.source.table_uuid:
                raise SourceConfigurationError(
                    "Iceberg source table UUID differs from its PostgreSQL registration before baseline"
                )
            try:
                validate_table_contract(metadata, None)
            except SourceContractError as exc:
                raise SourceConfigurationError("Iceberg source violates the initial table contract") from exc
            return self.plan_initial_baseline(metadata)
        source_planning_epoch: int | None = (
            int(row["source_planning_epoch"]) if row.get("source_planning_epoch") is not None else None
        )
        checkpoint: SourceCheckpoint = SourceCheckpoint(
            str(row["table_uuid"]),
            int(row["snapshot_id"]),
            int(row["iceberg_sequence_number"]),
            int(row["partition_spec_id"]),
        )
        try:
            validate_table_contract(metadata, checkpoint)
        except SourceContractError:
            self.gate_checkpoint(int(row["source_snapshot_seq"]), "SOURCE_TABLE_CONTRACT", source_planning_epoch)
            return self.empty_plan_from_checkpoint(row)
        return self.plan_next_increment(
            metadata,
            checkpoint,
            int(row["source_snapshot_seq"]),
            source_planning_epoch,
            max_snapshots,
        )

    def plan_initial_baseline(self, metadata: TableMetadata) -> SourcePlan:
        """Qualify and plan only the explicit first baseline snapshot.

        Args:
            metadata: Current pinned Iceberg table metadata.

        Returns:
            A single baseline window or an empty plan after durable rejection.

        Raises:
            SourceConfigurationError: If no exact source snapshot exists for durable rejection evidence.
        """
        baseline_snapshot_id: int | None = self.source.canonical_baseline_snapshot_id
        table: str = self.source.spark_table
        if metadata.current_snapshot_id is None:
            raise SourceConfigurationError("initial planning requires an existing canonical baseline snapshot")
        stop_snapshot_id: int = (
            baseline_snapshot_id if baseline_snapshot_id is not None else metadata.current_snapshot_id
        )
        snapshots: tuple[SnapshotRecord, ...] = self.catalog.snapshots_through(
            table,
            metadata.current_snapshot_id,
            stop_snapshot_id,
            descendant_limit=0,
            retain_head=True,
        )
        if baseline_snapshot_id is None:
            return self.reject_initial_head(metadata, snapshots, "BASELINE_NOT_CONFIGURED")
        baseline_snapshot: SnapshotRecord | None = index_snapshots(snapshots).get(baseline_snapshot_id)
        if baseline_snapshot is None:
            return self.reject_initial_head(metadata, snapshots, "BASELINE_NOT_RETAINED")
        try:
            validate_snapshot_identity(
                baseline_snapshot,
                metadata.table_uuid,
                metadata.active_partition_spec.spec_id,
            )
            try:
                proof: BaselineProof = self.baseline_qualifier.qualify(self.source, metadata, baseline_snapshot_id)
            except ValueError as exc:
                raise SourceBaselineError("baseline qualification rejected the source profile") from exc
            accepted: bool = (
                proof.table_uuid == metadata.table_uuid
                and proof.snapshot_id == baseline_snapshot_id
                and proof.partition_spec_id == baseline_snapshot.partition_spec_id
                and proof.canonical
                and proof.distinct_mutation_conflicts == 0
            )
            if not accepted:
                raise SourceBaselineError("baseline qualification found distinct mutations or mismatched identity")
            entries: tuple[ManifestEntry, ...] = self.catalog.manifest_entries(table, baseline_snapshot_id)
            targets: tuple[TargetKey, ...] = discover_baseline_targets(baseline_snapshot, entries)
            window: WindowPlan = WindowPlan(baseline_snapshot, WindowKind.BASELINE, targets)
            self.catalog.verify_planning_snapshots(table, metadata.table_uuid, (baseline_snapshot_id,))
        except (
            AnalysisException,
            IllegalArgumentException,
            SourceBaselineError,
            SourceLineageError,
            SourceSnapshotBlockedError,
        ) as exc:
            error_code: str
            if isinstance(exc, SourceSnapshotBlockedError):
                error_code = exc.error_code
            elif isinstance(exc, (AnalysisException, IllegalArgumentException)):
                error_code = "BASELINE_NOT_RETAINED"
            elif isinstance(exc, SourceLineageError):
                error_code = "BASELINE_IDENTITY_MISMATCH"
            else:
                error_code = "BASELINE_NOT_CANONICAL"
            self.persist_rejected(baseline_snapshot, error_code)
            return SourcePlan(metadata.table_uuid, metadata.current_snapshot_id, ())
        return SourcePlan(metadata.table_uuid, metadata.current_snapshot_id, (window,))

    def reject_initial_head(
        self,
        metadata: TableMetadata,
        snapshots: tuple[SnapshotRecord, ...],
        error_code: str,
    ) -> SourcePlan:
        """Persist exact current-head evidence for an unusable initial baseline.

        Args:
            metadata: Metadata pin carrying the exact current head.
            snapshots: Bounded ancestry observed from that head.
            error_code: Bounded deterministic bootstrap classification.

        Returns:
            Empty source plan after durable rejection.

        Raises:
            SourceConfigurationError: If the pinned head itself has no trustworthy snapshot record.
        """
        if metadata.current_snapshot_id is None:
            raise SourceConfigurationError("initial source metadata has no current snapshot")
        head_snapshot: SnapshotRecord | None = index_snapshots(snapshots).get(metadata.current_snapshot_id)
        if head_snapshot is None:
            raise SourceConfigurationError("current Iceberg head is missing from its pinned snapshot ancestry")
        self.persist_rejected(head_snapshot, error_code)
        return self.empty_plan(metadata)

    def plan_next_increment(
        self,
        metadata: TableMetadata,
        checkpoint: SourceCheckpoint,
        checkpoint_source_snapshot_seq: int,
        source_planning_epoch: int | None = None,
        max_snapshots: int = 32,
    ) -> SourcePlan:
        """Inspect a bounded direct-descendant prefix and stop at its first rejection.

        Args:
            metadata: Current pinned table metadata.
            checkpoint: Exact durable audit tip.
            checkpoint_source_snapshot_seq: Durable row identity used for an atomic safe-tip gate.
            source_planning_epoch: Optional durable planner fence observed before reading Iceberg.
            max_snapshots: Maximum windows the enqueuer may consume. One extra accepted window is
                retained to signal a truncated backlog.

        Returns:
            Bounded accepted prefix or an empty plan after durable rejection.
        """
        if metadata.current_snapshot_id is None:
            self.gate_checkpoint(
                checkpoint_source_snapshot_seq,
                "SOURCE_LINEAGE_UNTRUSTED",
                source_planning_epoch,
            )
            return self.empty_plan(metadata)
        try:
            snapshots: tuple[SnapshotRecord, ...] = self.catalog.snapshots_through(
                self.source.spark_table,
                metadata.current_snapshot_id,
                checkpoint.snapshot_id,
                descendant_limit=max_snapshots + 1,
            )
            snapshots = snapshots[-(max_snapshots + 2) :]
            candidates: tuple[SnapshotRecord, ...] = bounded_incremental_candidates(snapshots, checkpoint)
        except (SourceContractError, SourceLineageError):
            self.gate_checkpoint(
                checkpoint_source_snapshot_seq,
                "SOURCE_LINEAGE_UNTRUSTED",
                source_planning_epoch,
            )
            return self.empty_plan(metadata)
        if not candidates:
            return self.empty_plan(metadata)
        windows: list[WindowPlan] = []
        rejection: SourceSnapshotRejection | None = None
        previous_sequence: int = checkpoint.sequence_number
        snapshot: SnapshotRecord
        for snapshot in candidates:
            error_code: str | None = incremental_snapshot_error(snapshot, metadata, previous_sequence)
            if error_code is not None:
                rejection = SourceSnapshotRejection(snapshot, error_code)
                break
            try:
                window: WindowPlan = plan_incremental_window(self.catalog, self.source.spark_table, snapshot)
            except SourceSnapshotBlockedError as exc:
                rejection = SourceSnapshotRejection(snapshot, exc.error_code)
                break
            windows.append(window)
            previous_sequence = snapshot.sequence_number
        try:
            verification_ids: tuple[int, ...] = (
                checkpoint.snapshot_id,
                *(window.snapshot.snapshot_id for window in windows),
                *((rejection.snapshot.snapshot_id,) if rejection is not None else ()),
            )
            self.catalog.verify_planning_snapshots(
                self.source.spark_table,
                metadata.table_uuid,
                verification_ids,
            )
        except SourceLineageError:
            self.gate_checkpoint(
                checkpoint_source_snapshot_seq,
                "SOURCE_LINEAGE_UNTRUSTED",
                source_planning_epoch,
            )
            return self.empty_plan(metadata)
        return SourcePlan(
            metadata.table_uuid,
            metadata.current_snapshot_id,
            tuple(windows),
            source_planning_epoch,
            rejection,
        )

    def empty_plan(self, metadata: TableMetadata) -> SourcePlan:
        """Return an empty result pinned to the metadata read for this planner pass.

        Args:
            metadata: Pinned table metadata.

        Returns:
            Empty source plan retaining the exact head observation.
        """
        return SourcePlan(metadata.table_uuid, metadata.current_snapshot_id, ())

    def empty_plan_from_checkpoint(self, row: Mapping[str, object]) -> SourcePlan:
        """Return an empty plan from a durable source gate without touching Iceberg.

        Args:
            row: Latest checkpoint mapping carrying persisted source identity and gate state.

        Returns:
            Empty source plan pinned to PostgreSQL evidence.
        """
        return SourcePlan(
            str(row["table_uuid"]),
            int(row["snapshot_id"]),
            (),
            int(row["source_planning_epoch"]) if row.get("source_planning_epoch") is not None else None,
        )

    def gate_checkpoint(
        self,
        source_snapshot_seq: int,
        error_code: str,
        expected_planning_epoch: int | None = None,
    ) -> None:
        """Persist one fail-closed gate and reject a vanished durable checkpoint.

        Args:
            source_snapshot_seq: Exact durable checkpoint that failed validation.
            error_code: Bounded deterministic failure classification.
            expected_planning_epoch: Optional planner-observed source epoch used as an ABA fence.

        Raises:
            SourceLineageError: If concurrent state loss removed the checkpoint before it could be gated.
        """
        if expected_planning_epoch is None:
            blocked: bool = self.repository.block_source_snapshot(source_snapshot_seq, error_code)
        else:
            blocked = self.repository.block_source_snapshot(
                source_snapshot_seq,
                error_code,
                expected_planning_epoch=expected_planning_epoch,
            )
        if not blocked:
            raise SourceLineageError("durable source checkpoint disappeared before it could be gated")

    def persist_rejected(
        self,
        snapshot: SnapshotRecord,
        error_code: str,
        source_planning_epoch: int | None = None,
    ) -> int:
        """Persist one exact rejected source snapshot without target children.

        Args:
            snapshot: First unsupported direct descendant.
            error_code: Bounded planning classification.
            source_planning_epoch: Optional durable planner fence observed before reading Iceberg.

        Returns:
            Durable rejected audit sequence.
        """
        committed_at: datetime = datetime.fromtimestamp(snapshot.committed_at_ms / 1000, UTC)
        return self.repository.enqueue_blocked_source_snapshot(
            SourceSnapshotPlan(
                source_id=self.source.source_id,
                snapshot_id=snapshot.snapshot_id,
                parent_snapshot_id=snapshot.parent_snapshot_id,
                iceberg_sequence_number=snapshot.sequence_number,
                partition_spec_id=snapshot.partition_spec_id,
                committed_at=committed_at,
                iceberg_operation=snapshot.operation,
                kind=SourceSnapshotKind.REJECTED,
                source_planning_epoch=source_planning_epoch,
            ),
            error_code,
        )


def snapshot_from_row(table_uuid: str, spec_id: int, sequence_number: int, row: Row) -> SnapshotRecord:
    """Normalize one Spark snapshots metadata row.

    Args:
        table_uuid: Stable table identity.
        spec_id: Approved partition specification.
        sequence_number: Authoritative sequence number from Iceberg metadata JSON.
        row: Spark metadata row.

    Returns:
        Typed snapshot record.
    """
    values: dict[str, Any] = row.asDict(recursive=True)
    committed: datetime = values["committed_at"]
    committed_at_ms: int = int(committed.timestamp() * 1000)
    return SnapshotRecord(
        table_uuid=table_uuid,
        snapshot_id=int(values["snapshot_id"]),
        parent_snapshot_id=int(values["parent_id"]) if values.get("parent_id") is not None else None,
        sequence_number=sequence_number,
        committed_at_ms=committed_at_ms,
        operation=str(values["operation"]),
        partition_spec_id=spec_id,
        summary=tuple(sorted((str(key), str(value)) for key, value in (values.get("summary") or {}).items())),
    )


def manifest_from_row(
    row: Row,
    snapshot_id: int | None = None,
    status: ManifestStatus | None = None,
) -> ManifestEntry:
    """Normalize one distinct Spark manifest fact.

    Args:
        row: Spark row carrying flattened manifest fields.
        snapshot_id: Explicit snapshot for baseline file rows.
        status: Explicit status for baseline live files.

    Returns:
        Typed manifest entry.

    Raises:
        SourceSnapshotBlockedError: If a required partition value is null or invalid.
    """
    values: dict[str, Any] = row.asDict(recursive=True)
    resolved_snapshot: int = snapshot_id if snapshot_id is not None else int(values["entry_snapshot_id"])
    partition_columns: tuple[str, ...] = ("tenant_id", "namespace", "org_id", "ts_hour")
    if any(values.get(name) is None for name in partition_columns):
        raise blocked_snapshot_error(
            resolved_snapshot,
            "SOURCE_PARTITION_NULL",
            "source manifest contains a null required partition value",
        )
    try:
        tenant_id: str = validate_routing_segment(str(values["tenant_id"]), "tenant_id")
        namespace: str = validate_routing_segment(str(values["namespace"]), "namespace")
        org_id: str = validate_routing_segment(str(values["org_id"]), "org_id")
    except ValueError as exc:
        raise blocked_snapshot_error(
            resolved_snapshot,
            "SOURCE_PARTITION_ROUTING",
            "source manifest contains an invalid routing partition value",
        ) from exc
    resolved_status: ManifestStatus = status or MANIFEST_STATUS_BY_CODE[int(values["entry_status"])]
    return ManifestEntry(
        snapshot_id=resolved_snapshot,
        partition_spec_id=int(values["entry_spec_id"]),
        status=resolved_status,
        content=MANIFEST_CONTENT_BY_CODE[int(values["entry_content"])],
        partition=PartitionValues(
            tenant_id=tenant_id,
            namespace=namespace,
            org_id=org_id,
            ts_hour=int(values["ts_hour"]),
        ),
    )
