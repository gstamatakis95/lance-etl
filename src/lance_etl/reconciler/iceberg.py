"""Concrete Spark and Iceberg metadata adapters for scheduled source planning."""

from __future__ import annotations

import json
import re
import uuid
from collections.abc import Iterator, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Protocol

import pyarrow as pa
from pyspark.sql import Column, DataFrame, Row, SparkSession
from pyspark.sql.functions import col, countDistinct, lit, lower, trim

from lance_etl.cloud_storage import resolve_filesystem
from lance_etl.etl.digest import canonical_event_digest
from lance_etl.etl.mutation import DELETE_OPERATIONS, UPSERT_OPERATIONS, normalize_operation
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
    SourcePlanner,
    TableMetadata,
    TouchedTarget,
    WindowKind,
    WindowPlan,
)
from lance_etl.source.contract import required_partition_fields, validate_table_contract
from lance_etl.source.errors import (
    SourceBaselineError,
    SourceContractError,
    SourceLineageError,
    SourceSnapshotBlockedError,
)
from lance_etl.source.lineage import index_snapshots, validate_snapshot_identity
from lance_etl.source.manifests import discover_baseline_targets
from lance_etl.source.planner import build_window
from lance_etl.state import IcebergSource, SourceSnapshotKind, SourceSnapshotPlan, SourceSnapshotState

TABLE_IDENTIFIER_PATTERN: re.Pattern[str] = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*){1,2}$")
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

    def block_source_snapshot(self, source_snapshot_seq: int, error_code: str) -> bool:
        """Gate the latest exact safe audit tip when no next identity is trustworthy.

        Args:
            source_snapshot_seq: Latest safe durable audit sequence.
            error_code: Bounded deterministic classification.

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
            registered_source: PostgreSQL-owned source and column mapping.
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
        self.validate_contract(source_frame, registered_source)
        selected_columns: list[Column] = [
            col(registered_source.tenant_column).alias("tenant_id"),
            col(registered_source.namespace_column).alias("namespace"),
            col(registered_source.org_column).alias("org_id"),
            col(registered_source.record_id_column).alias("record_id"),
            col(registered_source.operation_column).alias("op"),
            col(registered_source.ts_column).alias("ts"),
            col(registered_source.vectors_column).alias("vectors"),
            col(registered_source.texts_column).alias("texts"),
            col(registered_source.metadata_column).alias("metadata"),
        ]

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

        digests: DataFrame = source_frame.select(*selected_columns).mapInArrow(
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

    def validate_contract(self, source_frame: Any, registered_source: IcebergSource) -> None:
        """Reject deterministic baseline contract violations before digest aggregation.

        Args:
            source_frame: Exact baseline Spark DataFrame.
            registered_source: PostgreSQL-owned source and column mapping.

        Raises:
            ValueError: If source shape, operation, or routing fields are invalid.
        """
        required: set[str] = {
            registered_source.tenant_column,
            registered_source.namespace_column,
            registered_source.org_column,
            registered_source.record_id_column,
            registered_source.operation_column,
            registered_source.ts_column,
            registered_source.vectors_column,
            registered_source.texts_column,
            registered_source.metadata_column,
        }
        missing: list[str] = sorted(required - set(source_frame.columns))
        if missing:
            raise ValueError(f"baseline source is missing required columns {missing}")
        nonnull: tuple[str, ...] = (
            registered_source.tenant_column,
            registered_source.namespace_column,
            registered_source.org_column,
            registered_source.record_id_column,
            registered_source.operation_column,
            registered_source.ts_column,
        )
        if source_frame.where(lit(False) | any_null_expression(nonnull)).limit(1).count():
            raise ValueError("baseline source contains null routing, identity, operation, or timestamp")
        operation: Column = lower(trim(col(registered_source.operation_column)))
        supported: tuple[str, ...] = tuple(sorted(UPSERT_OPERATIONS | DELETE_OPERATIONS))
        if source_frame.where(~operation.isin(*supported)).limit(1).count():
            raise ValueError("baseline source contains an unsupported mutation operation")


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


@dataclass(frozen=True, slots=True)
class SparkIcebergCatalog:
    """Read exact Iceberg metadata and partition deltas through Spark metadata tables."""

    spark: SparkSession
    baseline_snapshot_id: int | None = None
    tenant_column: str = "tenant_id"
    namespace_column: str = "namespace"
    org_column: str = "org_id"

    def validate_table(self, table: str) -> str:
        """Validate one deployment-owned catalog identifier before Spark access.

        Args:
            table: Catalog-qualified table.

        Returns:
            Validated identifier.

        Raises:
            ValueError: If the identifier is not a bounded dotted name.
        """
        if TABLE_IDENTIFIER_PATTERN.fullmatch(table) is None:
            raise ValueError("source table must be a two- or three-part ASCII identifier")
        return table

    def metadata_document(self, table: str) -> dict[str, Any]:
        """Load the current Iceberg metadata JSON through its metadata-log table.

        Args:
            table: Catalog-qualified table.

        Returns:
            Parsed Iceberg metadata document.

        Raises:
            RuntimeError: If the catalog exposes no current metadata file.
        """
        validated: str = self.validate_table(table)
        log: DataFrame = self.spark.read.format("iceberg").load(f"{validated}.metadata_log_entries")
        row: Row | None = log.orderBy(col("timestamp").desc()).select("file").first()
        if row is None or not row["file"]:
            raise RuntimeError("Iceberg metadata log exposes no current metadata file")
        metadata_uri: str = str(row["file"])
        filesystem: Any
        path: str
        filesystem, path = resolve_filesystem(metadata_uri, None)
        with filesystem.open_input_file(path) as stream:
            return json.loads(stream.read().decode("utf-8"))

    def table_metadata(self, table: str) -> TableMetadata:
        """Return stable UUID, current head, and exact active partition spec.

        Args:
            table: Catalog-qualified table.

        Returns:
            Typed source table metadata.
        """
        document: dict[str, Any] = self.metadata_document(table)
        table_uuid: str = str(document.get("table-uuid", ""))
        uuid.UUID(table_uuid)
        spec_id: int = int(document["default-spec-id"])
        schemas: dict[int, dict[str, Any]] = {int(schema["schema-id"]): schema for schema in document["schemas"]}
        current_schema: dict[str, Any] = schemas[int(document["current-schema-id"])]
        names_by_id: dict[int, str] = {int(field["id"]): str(field["name"]) for field in current_schema["fields"]}
        specs: dict[int, dict[str, Any]] = {int(spec["spec-id"]): spec for spec in document["partition-specs"]}
        active_spec: dict[str, Any] = specs[spec_id]
        fields: tuple[PartitionField, ...] = tuple(
            PartitionField(
                field_name=str(field["name"]),
                source_column=names_by_id[int(field["source-id"])],
                transform=str(field["transform"]),
            )
            for field in active_spec["fields"]
        )
        current_snapshot: Any = document.get("current-snapshot-id")
        return TableMetadata(
            table_uuid=table_uuid,
            current_snapshot_id=int(current_snapshot) if current_snapshot is not None else None,
            active_partition_spec=PartitionSpec(spec_id, fields),
        )

    def snapshots_through(
        self, table: str, pinned_head_snapshot_id: int, stop_snapshot_id: int
    ) -> tuple[SnapshotRecord, ...]:
        """Walk direct parents with one-row metadata queries and bounded driver memory.

        Args:
            table: Catalog-qualified table.
            pinned_head_snapshot_id: Inclusive head pinned by the planner.
            stop_snapshot_id: Inclusive stopping ancestor.

        Returns:
            Snapshot records needed for only this bounded ancestry.
        """
        validated: str = self.validate_table(table)
        metadata: TableMetadata = self.table_metadata(validated)
        document: dict[str, Any] = self.metadata_document(validated)
        sequence_numbers: dict[int, int] = {
            int(snapshot["snapshot-id"]): int(snapshot["sequence-number"]) for snapshot in document.get("snapshots", ())
        }
        snapshots: DataFrame = self.spark.read.format("iceberg").load(f"{validated}.snapshots")
        records: list[SnapshotRecord] = []
        cursor: int = pinned_head_snapshot_id
        visited: set[int] = set()
        while cursor not in visited:
            visited.add(cursor)
            row: Row | None = snapshots.where(col("snapshot_id") == cursor).limit(1).first()
            if row is None:
                break
            spec_id: int = self.snapshot_partition_spec_id(
                validated, cursor, row, metadata.active_partition_spec.spec_id
            )
            sequence_number: int | None = sequence_numbers.get(cursor)
            if sequence_number is None:
                raise SourceLineageError("Iceberg metadata lacks a sequence number for a required snapshot")
            record: SnapshotRecord = snapshot_from_row(metadata.table_uuid, spec_id, sequence_number, row)
            records.append(record)
            if cursor == stop_snapshot_id or record.parent_snapshot_id is None:
                break
            cursor = record.parent_snapshot_id
        return tuple(records)

    def snapshot_partition_spec_id(self, table: str, snapshot_id: int, row: Row, fallback_spec_id: int) -> int:
        """Resolve a snapshot's own partition spec from summary or added manifests.

        Args:
            table: Validated Iceberg table identifier.
            snapshot_id: Exact historical snapshot.
            row: Spark snapshots metadata row.
            fallback_spec_id: Current active spec used only for snapshots with no added manifests.

        Returns:
            Exact added-manifest spec ID or the safe active fallback.

        Raises:
            SourceContractError: If one snapshot adds manifests under multiple specs.
        """
        summary: dict[str, Any] = row.asDict(recursive=True).get("summary") or {}
        key: Any
        for key in ("partition-spec-id", "partition_spec_id"):
            if key in summary:
                return int(summary[key])
        manifests: DataFrame = self.spark.read.format("iceberg").load(f"{table}.all_manifests")
        specs: list[Row] = (
            manifests.where(col("added_snapshot_id") == snapshot_id)
            .select(col("partition_spec_id").cast("int").alias("partition_spec_id"))
            .distinct()
            .limit(2)
            .collect()
        )
        if len(specs) > 1:
            raise SourceContractError("one source snapshot adds manifests under multiple partition specifications")
        return int(specs[0]["partition_spec_id"]) if specs else fallback_spec_id

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
        entries: DataFrame = (
            self.spark.read.format("iceberg").load(f"{validated}.all_entries").where(col("snapshot_id") == snapshot_id)
        )
        selected: DataFrame = entries.select(
            col("snapshot_id").cast("long").alias("entry_snapshot_id"),
            col("status").cast("int").alias("entry_status"),
            col("data_file.content").cast("int").alias("entry_content"),
            col("data_file.spec_id").cast("int").alias("entry_spec_id"),
            col(f"data_file.partition.{self.tenant_column}").cast("string").alias("tenant_id"),
            col(f"data_file.partition.{self.namespace_column}").cast("string").alias("namespace"),
            col(f"data_file.partition.{self.org_column}").cast("string").alias("org_id"),
            col("data_file.partition.ts_hour").cast("int").alias("ts_hour"),
        ).distinct()
        return tuple(manifest_from_row(row) for row in selected.collect())

    def baseline_entries(self, table: str, snapshot_id: int) -> tuple[ManifestEntry, ...]:
        """Return distinct live target-hour facts for one validated canonical baseline.

        Args:
            table: Validated catalog-qualified table.
            snapshot_id: Exact canonical baseline snapshot.

        Returns:
            Normalized live data-file facts.
        """
        files: DataFrame = (
            self.spark.read.format("iceberg").option("snapshot-id", str(snapshot_id)).load(f"{table}.files")
        )
        selected: DataFrame = files.select(
            col("content").cast("int").alias("entry_content"),
            col("spec_id").cast("int").alias("entry_spec_id"),
            col(f"partition.{self.tenant_column}").cast("string").alias("tenant_id"),
            col(f"partition.{self.namespace_column}").cast("string").alias("namespace"),
            col(f"partition.{self.org_column}").cast("string").alias("org_id"),
            col("partition.ts_hour").cast("int").alias("ts_hour"),
        ).distinct()
        return tuple(manifest_from_row(row, snapshot_id, ManifestStatus.EXISTING) for row in selected.collect())

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


@dataclass(frozen=True, slots=True)
class DurableSourcePlanProvider:
    """Build the next pinned source plan from Iceberg and the PostgreSQL audit tip."""

    source: IcebergSource
    catalog: SparkIcebergCatalog
    repository: SourceCheckpointRepository
    baseline_qualifier: BaselineQualifier

    def plan(self) -> SourcePlan:
        """Build only the next accepted window or durably record the next rejection.

        Returns:
            Pinned side-effect-free source plan.

        Raises:
            RuntimeError: If first startup lacks an explicit canonical baseline.
        """
        table: str = self.source.spark_table
        metadata: TableMetadata = self.catalog.table_metadata(table)
        table_uuid: uuid.UUID = uuid.UUID(metadata.table_uuid)
        if table_uuid != self.source.table_uuid:
            raise SourceContractError("Iceberg source table UUID differs from its PostgreSQL registration")
        row: Mapping[str, object] | None = self.repository.latest_source_snapshot(self.source.source_id)
        if row is None:
            validate_table_contract(
                metadata,
                None,
                required_partition_fields(
                    self.source.tenant_column,
                    self.source.namespace_column,
                    self.source.org_column,
                ),
            )
            return self.plan_initial_baseline(metadata)
        if row["state"] == SourceSnapshotState.BLOCKED.value:
            return self.empty_plan(metadata)
        checkpoint: SourceCheckpoint = SourceCheckpoint(
            metadata.table_uuid,
            int(row["snapshot_id"]),
            int(row["iceberg_sequence_number"]),
            int(row["partition_spec_id"]),
        )
        try:
            validate_table_contract(
                metadata,
                checkpoint,
                required_partition_fields(
                    self.source.tenant_column,
                    self.source.namespace_column,
                    self.source.org_column,
                ),
            )
        except SourceContractError:
            self.repository.block_source_snapshot(int(row["source_snapshot_seq"]), "SOURCE_TABLE_CONTRACT")
            return self.empty_plan(metadata)
        return self.plan_next_increment(metadata, checkpoint, int(row["source_snapshot_seq"]))

    def plan_initial_baseline(self, metadata: TableMetadata) -> SourcePlan:
        """Qualify and plan only the explicit first baseline snapshot.

        Args:
            metadata: Current pinned Iceberg table metadata.

        Returns:
            A single baseline window or an empty plan after durable rejection.
        """
        baseline_snapshot_id: int | None = self.source.canonical_baseline_snapshot_id
        table: str = self.source.spark_table
        if baseline_snapshot_id is None:
            raise RuntimeError("initial planning requires a PostgreSQL canonical baseline snapshot ID")
        if metadata.current_snapshot_id is None:
            return SourcePlan(metadata.table_uuid, None, metadata.active_partition_spec.spec_id, ())
        snapshots: tuple[SnapshotRecord, ...] = self.catalog.snapshots_through(
            table,
            metadata.current_snapshot_id,
            baseline_snapshot_id,
        )
        baseline_snapshot: SnapshotRecord | None = index_snapshots(snapshots).get(baseline_snapshot_id)
        if baseline_snapshot is None:
            raise SourceLineageError("pinned baseline is not retained in the requested ancestry")
        try:
            validate_snapshot_identity(
                baseline_snapshot,
                metadata.table_uuid,
                metadata.active_partition_spec.spec_id,
            )
            proof: BaselineProof = self.baseline_qualifier.qualify(self.source, metadata, baseline_snapshot_id)
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
            touched: tuple[TouchedTarget, ...] = discover_baseline_targets(baseline_snapshot, entries)
            window: WindowPlan = build_window(table, baseline_snapshot, WindowKind.BASELINE, touched)
        except (SourceBaselineError, SourceLineageError, SourceSnapshotBlockedError) as exc:
            error_code: str
            if isinstance(exc, SourceSnapshotBlockedError):
                error_code = exc.error_code
            elif isinstance(exc, SourceLineageError):
                error_code = "BASELINE_IDENTITY_MISMATCH"
            else:
                error_code = "BASELINE_NOT_CANONICAL"
            self.persist_rejected(baseline_snapshot, error_code)
            return SourcePlan(
                metadata.table_uuid,
                metadata.current_snapshot_id,
                metadata.active_partition_spec.spec_id,
                (),
            )
        return SourcePlan(
            metadata.table_uuid,
            metadata.current_snapshot_id,
            metadata.active_partition_spec.spec_id,
            (window,),
        )

    def plan_next_increment(
        self,
        metadata: TableMetadata,
        checkpoint: SourceCheckpoint,
        checkpoint_source_snapshot_seq: int,
    ) -> SourcePlan:
        """Inspect only the direct next descendant and stop at its first rejection.

        Args:
            metadata: Current pinned table metadata.
            checkpoint: Exact durable audit tip.
            checkpoint_source_snapshot_seq: Durable row identity used for an atomic safe-tip gate.

        Returns:
            One accepted next window or an empty plan after durable rejection.
        """
        if metadata.current_snapshot_id is None:
            return SourcePlan(metadata.table_uuid, None, metadata.active_partition_spec.spec_id, ())
        try:
            snapshots: tuple[SnapshotRecord, ...] = self.catalog.snapshots_through(
                self.source.spark_table,
                metadata.current_snapshot_id,
                checkpoint.snapshot_id,
            )
            snapshot: SnapshotRecord | None = direct_next_snapshot(
                snapshots,
                metadata.current_snapshot_id,
                checkpoint.snapshot_id,
            )
        except (SourceContractError, SourceLineageError):
            self.repository.block_source_snapshot(checkpoint_source_snapshot_seq, "SOURCE_LINEAGE_UNTRUSTED")
            return self.empty_plan(metadata)
        if snapshot is None:
            return self.empty_plan(metadata)
        try:
            validate_snapshot_identity(snapshot, metadata.table_uuid, metadata.active_partition_spec.spec_id)
        except SourceLineageError:
            self.persist_rejected(snapshot, "SOURCE_SNAPSHOT_IDENTITY")
            return self.empty_plan(metadata)
        if snapshot.sequence_number <= checkpoint.sequence_number:
            self.persist_rejected(snapshot, "SOURCE_SEQUENCE_ORDER")
            return self.empty_plan(metadata)
        try:
            window: WindowPlan = SourcePlanner(self.catalog).plan_snapshot(self.source.spark_table, snapshot)
        except SourceSnapshotBlockedError as exc:
            self.persist_rejected(snapshot, exc.error_code)
            return self.empty_plan(metadata)
        return SourcePlan(
            metadata.table_uuid,
            metadata.current_snapshot_id,
            metadata.active_partition_spec.spec_id,
            (window,),
        )

    def empty_plan(self, metadata: TableMetadata) -> SourcePlan:
        """Return an empty result pinned to the metadata read for this planner pass.

        Args:
            metadata: Pinned table metadata.

        Returns:
            Empty source plan retaining exact head and active-spec evidence.
        """
        return SourcePlan(
            metadata.table_uuid,
            metadata.current_snapshot_id,
            metadata.active_partition_spec.spec_id,
            (),
        )

    def persist_rejected(self, snapshot: SnapshotRecord, error_code: str) -> int:
        """Persist one exact rejected source snapshot without target children.

        Args:
            snapshot: First unsupported direct descendant.
            error_code: Bounded planning classification.

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
            ),
            error_code,
        )


def direct_next_snapshot(
    snapshots: tuple[SnapshotRecord, ...],
    head_snapshot_id: int,
    checkpoint_snapshot_id: int,
) -> SnapshotRecord | None:
    """Return only the direct child of a retained checkpoint in a pinned ancestry.

    Args:
        snapshots: Bounded ancestry from head through checkpoint.
        head_snapshot_id: Pinned current head.
        checkpoint_snapshot_id: Durable audit tip.

    Returns:
        Direct child snapshot, or ``None`` when checkpoint is already head.

    Raises:
        SourceLineageError: If ancestry is missing, cyclic, or does not reach the checkpoint.
    """
    if head_snapshot_id == checkpoint_snapshot_id:
        return None
    indexed: dict[int, SnapshotRecord] = index_snapshots(snapshots)
    cursor: int = head_snapshot_id
    visited: set[int] = set()
    child: SnapshotRecord | None = None
    while cursor != checkpoint_snapshot_id:
        if cursor in visited:
            raise SourceLineageError("cycle detected in pinned Iceberg snapshot ancestry")
        visited.add(cursor)
        record: SnapshotRecord | None = indexed.get(cursor)
        if record is None:
            raise SourceLineageError("pinned Iceberg ancestry is missing a required snapshot")
        child = record
        if record.parent_snapshot_id is None:
            raise SourceLineageError("pinned Iceberg head does not descend from the durable audit tip")
        cursor = record.parent_snapshot_id
    return child


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
    """
    values: dict[str, Any] = row.asDict(recursive=True)
    resolved_snapshot: int = snapshot_id if snapshot_id is not None else int(values["entry_snapshot_id"])
    resolved_status: ManifestStatus = status or MANIFEST_STATUS_BY_CODE[int(values["entry_status"])]
    return ManifestEntry(
        snapshot_id=resolved_snapshot,
        partition_spec_id=int(values["entry_spec_id"]),
        status=resolved_status,
        content=MANIFEST_CONTENT_BY_CODE[int(values["entry_content"])],
        partition=PartitionValues(
            tenant_id=str(values["tenant_id"]),
            namespace=str(values["namespace"]),
            org_id=str(values["org_id"]),
            ts_hour=int(values["ts_hour"]),
        ),
    )
