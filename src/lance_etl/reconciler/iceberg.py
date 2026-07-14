"""Concrete Spark and Iceberg metadata adapters for scheduled source planning."""

from __future__ import annotations

import json
import re
import uuid
from collections.abc import Iterator
from dataclasses import dataclass
from typing import Any, Protocol

import pyarrow as pa
from pyspark.sql import Row, SparkSession
from pyspark.sql.functions import array, array_except, col, countDistinct, element_at, lit, lower, map_keys, size, trim

from lance_etl.cloud_storage import resolve_filesystem
from lance_etl.etl.digest import canonical_event_digest
from lance_etl.etl.mutation import DELETE_OPERATIONS, UPSERT_OPERATIONS, normalize_operation
from lance_etl.reconciler.config import DeploymentProfile
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
    WindowKind,
)
from lance_etl.source.contract import validate_table_contract
from lance_etl.source.errors import (
    SourceBaselineError,
    SourceContractError,
    SourceLineageError,
    SourceSnapshotBlockedError,
)
from lance_etl.source.lineage import index_snapshots, validate_snapshot_identity
from lance_etl.source.manifests import discover_baseline_targets
from lance_etl.source.planner import build_window
from lance_etl.state import SourceWindowKind, SourceWindowPlan, SourceWindowState

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

    def latest_source_window(self, table_uuid: uuid.UUID | None = None) -> Any:
        """Return the newest durable source row.

        Args:
            table_uuid: Stable Iceberg table identity.

        Returns:
            Mapping-like row or ``None``.
        """
        ...

    def enqueue_blocked_source_window(self, plan: SourceWindowPlan, error_code: str) -> int:
        """Persist a rejected exact next snapshot without child work.

        Args:
            plan: Rejected snapshot identity.
            error_code: Bounded deterministic classification.

        Returns:
            Durable rejected window sequence.
        """
        ...

    def block_source_window(self, window_seq: int, error_code: str) -> bool:
        """Gate the latest exact safe audit tip when no next identity is trustworthy.

        Args:
            window_seq: Latest safe durable audit sequence.
            error_code: Bounded deterministic classification.

        Returns:
            Whether the durable fail-closed gate exists.
        """
        ...


@dataclass(frozen=True, slots=True)
class BaselineQualifier:
    """Prove one exact initial snapshot has canonical per-target mutation identity."""

    spark: SparkSession
    profile: DeploymentProfile

    def qualify(self, table: str, metadata: TableMetadata, snapshot_id: int) -> BaselineProof:
        """Scan and validate the exact baseline with executor-computed canonical digests.

        Args:
            table: Deployment-owned Iceberg table.
            metadata: Pinned table UUID and partition specification.
            snapshot_id: Explicit retained baseline snapshot.

        Returns:
            Proof tied to the exact table, snapshot, and active partition specification.
        """
        source = self.spark.read.format("iceberg").option("snapshot-id", str(snapshot_id)).load(table)
        self.validate_contract(source)
        selected_columns = [
            "tenant_id",
            "namespace",
            "org_id",
            "vector_id",
            "op",
            "event_timestamp",
            "vectors",
            "texts",
            "metadata",
        ]
        if self.profile.include_ttl:
            selected_columns.append("ttl")
        profile = self.profile

        def digest_batches(batches: Iterator[pa.RecordBatch]) -> Iterator[pa.RecordBatch]:
            """Compute canonical mutation digests for bounded Arrow source batches.

            Args:
                batches: Exact-snapshot source batches.

            Yields:
                Routing key, vector ID, and canonical digest batches.
            """
            output_schema = pa.schema(
                [
                    pa.field("tenant_id", pa.string()),
                    pa.field("namespace", pa.string()),
                    pa.field("org_id", pa.string()),
                    pa.field("vector_id", pa.string()),
                    pa.field("mutation_digest", pa.binary(32)),
                ]
            )
            for batch in batches:
                output: list[dict[str, object]] = []
                for row in pa.Table.from_batches([batch]).to_pylist():
                    operation = normalize_operation(str(row["op"]))
                    vectors = dict(row.get("vectors") or {})
                    texts = dict(row.get("texts") or {})
                    metadata_values = dict(row.get("metadata") or {})
                    payload: dict[str, object] = {}
                    payload.update({name: vectors.get(name) for name, dimension in profile.vector_fields})
                    payload.update({name: texts.get(name) for name in profile.text_fields})
                    payload.update({name: metadata_values.get(name) for name in profile.metadata_fields})
                    if profile.include_ttl:
                        payload["ttl"] = row.get("ttl")
                    target = str(row["tenant_id"]), str(row["namespace"]), str(row["org_id"])
                    digest = canonical_event_digest(
                        target,
                        str(row["vector_id"]),
                        operation,
                        row["event_timestamp"],
                        payload,
                    )
                    output.append(
                        {
                            "tenant_id": target[0],
                            "namespace": target[1],
                            "org_id": target[2],
                            "vector_id": row["vector_id"],
                            "mutation_digest": digest,
                        }
                    )
                yield from pa.Table.from_pylist(output, schema=output_schema).to_batches()

        digests = source.select(*selected_columns).mapInArrow(
            digest_batches,
            "tenant_id string, namespace string, org_id string, vector_id string, mutation_digest binary",
        )
        conflicts = (
            digests.groupBy("tenant_id", "namespace", "org_id", "vector_id")
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

    def validate_contract(self, source: Any) -> None:
        """Reject deterministic baseline contract violations before digest aggregation.

        Args:
            source: Exact baseline Spark DataFrame.

        Raises:
            ValueError: If source shape, operation, routing, or profile fields are invalid.
        """
        required = {
            "tenant_id",
            "namespace",
            "org_id",
            "vector_id",
            "op",
            "event_timestamp",
            "vectors",
            "texts",
            "metadata",
        }
        if self.profile.include_ttl:
            required.add("ttl")
        missing = sorted(required - set(source.columns))
        if missing:
            raise ValueError(f"baseline source is missing required columns {missing}")
        nonnull = ("tenant_id", "namespace", "org_id", "vector_id", "op", "event_timestamp")
        if source.where(lit(False) | any_null_expression(nonnull)).limit(1).count():
            raise ValueError("baseline source contains null routing, identity, operation, or timestamp")
        operation = lower(trim(col("op")))
        supported = tuple(sorted(UPSERT_OPERATIONS | DELETE_OPERATIONS))
        if source.where(~operation.isin(*supported)).limit(1).count():
            raise ValueError("baseline source contains an unsupported mutation operation")
        deleted = operation.isin(*tuple(sorted(DELETE_OPERATIONS)))
        contracts = (
            ("vectors", tuple(name for name, dimension in self.profile.vector_fields)),
            ("texts", self.profile.text_fields),
            ("metadata", self.profile.metadata_fields),
        )
        for map_column, allowed in contracts:
            keys = map_keys(col(map_column))
            unknown = size(array_except(keys, array(*(lit(name) for name in allowed)))) if allowed else size(keys)
            if source.where(unknown > 0).limit(1).count():
                raise ValueError(f"baseline map {map_column!r} contains fields outside the release profile")
        for name, dimension in self.profile.vector_fields:
            vector = element_at(col("vectors"), lit(name))
            if source.where(~deleted & vector.isNull()).limit(1).count():
                raise ValueError(f"baseline upsert is missing required vector {name!r}")
            if source.where(vector.isNotNull() & (size(vector) != dimension)).limit(1).count():
                raise ValueError(f"baseline vector {name!r} does not match dimension {dimension}")


def any_null_expression(columns: tuple[str, ...]) -> Any:
    """Build a typed OR of null checks for required Spark columns.

    Args:
        columns: Required source column names.

    Returns:
        Spark boolean column expression.
    """
    expression = col(columns[0]).isNull()
    for name in columns[1:]:
        expression = expression | col(name).isNull()
    return expression


@dataclass(frozen=True, slots=True)
class SparkIcebergCatalog:
    """Read exact Iceberg metadata and partition deltas through Spark metadata tables."""

    spark: SparkSession
    baseline_snapshot_id: int | None = None

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
        validated = self.validate_table(table)
        log = self.spark.read.format("iceberg").load(f"{validated}.metadata_log_entries")
        row = log.orderBy(col("timestamp").desc()).select("file").first()
        if row is None or not row["file"]:
            raise RuntimeError("Iceberg metadata log exposes no current metadata file")
        metadata_uri = str(row["file"])
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
        document = self.metadata_document(table)
        table_uuid = str(document.get("table-uuid", ""))
        uuid.UUID(table_uuid)
        spec_id = int(document["default-spec-id"])
        schemas = {int(schema["schema-id"]): schema for schema in document["schemas"]}
        current_schema = schemas[int(document["current-schema-id"])]
        names_by_id = {int(field["id"]): str(field["name"]) for field in current_schema["fields"]}
        specs = {int(spec["spec-id"]): spec for spec in document["partition-specs"]}
        active_spec = specs[spec_id]
        fields = tuple(
            PartitionField(
                field_name=str(field["name"]),
                source_column=names_by_id[int(field["source-id"])],
                transform=str(field["transform"]),
            )
            for field in active_spec["fields"]
        )
        current_snapshot = document.get("current-snapshot-id")
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
        validated = self.validate_table(table)
        metadata = self.table_metadata(validated)
        snapshots = self.spark.read.format("iceberg").load(f"{validated}.snapshots")
        records: list[SnapshotRecord] = []
        cursor = pinned_head_snapshot_id
        visited: set[int] = set()
        while cursor not in visited:
            visited.add(cursor)
            row = snapshots.where(col("snapshot_id") == cursor).limit(1).first()
            if row is None:
                break
            spec_id = self.snapshot_partition_spec_id(validated, cursor, row, metadata.active_partition_spec.spec_id)
            record = snapshot_from_row(metadata.table_uuid, spec_id, row)
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
        summary = row.asDict(recursive=True).get("summary") or {}
        for key in ("partition-spec-id", "partition_spec_id"):
            if key in summary:
                return int(summary[key])
        manifests = self.spark.read.format("iceberg").load(f"{table}.all_manifests")
        specs = (
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
        validated = self.validate_table(table)
        if snapshot_id == self.baseline_snapshot_id:
            return self.baseline_entries(validated, snapshot_id)
        reader = self.spark.read.format("iceberg").option("snapshot-id", str(snapshot_id))
        entries = reader.load(f"{validated}.all_entries").where(col("snapshot_id") == snapshot_id)
        selected = entries.select(
            col("snapshot_id").cast("long").alias("entry_snapshot_id"),
            col("status").cast("int").alias("entry_status"),
            col("data_file.content").cast("int").alias("entry_content"),
            col("data_file.spec_id").cast("int").alias("entry_spec_id"),
            col("data_file.partition.tenant_id").cast("string").alias("tenant_id"),
            col("data_file.partition.namespace").cast("string").alias("namespace"),
            col("data_file.partition.org_id").cast("string").alias("org_id"),
            col("data_file.partition.processing_timestamp_hour").cast("int").alias("processing_timestamp_hour"),
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
        files = self.spark.read.format("iceberg").option("snapshot-id", str(snapshot_id)).load(f"{table}.files")
        selected = files.select(
            col("content").cast("int").alias("entry_content"),
            col("spec_id").cast("int").alias("entry_spec_id"),
            col("partition.tenant_id").cast("string").alias("tenant_id"),
            col("partition.namespace").cast("string").alias("namespace"),
            col("partition.org_id").cast("string").alias("org_id"),
            col("partition.processing_timestamp_hour").cast("int").alias("processing_timestamp_hour"),
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

    table: str
    catalog: SparkIcebergCatalog
    repository: SourceCheckpointRepository
    baseline_snapshot_id: int | None
    baseline_qualifier: BaselineQualifier

    def plan(self) -> SourcePlan:
        """Build only the next accepted window or durably record the next rejection.

        Returns:
            Pinned side-effect-free source plan.

        Raises:
            RuntimeError: If first startup lacks an explicit canonical baseline.
        """
        metadata = self.catalog.table_metadata(self.table)
        uuid.UUID(metadata.table_uuid)
        row = self.repository.latest_source_window()
        if row is None:
            validate_table_contract(metadata, None)
            return self.plan_initial_baseline(metadata)
        if row["state"] == SourceWindowState.BLOCKED.value:
            return self.empty_plan(metadata)
        checkpoint = SourceCheckpoint(
            str(row["table_uuid"]),
            int(row["snapshot_id"]),
            int(row["iceberg_sequence_number"]),
            int(row["partition_spec_id"]),
        )
        try:
            validate_table_contract(metadata, checkpoint)
        except SourceContractError:
            self.repository.block_source_window(int(row["window_seq"]), "SOURCE_TABLE_CONTRACT")
            return self.empty_plan(metadata)
        return self.plan_next_increment(metadata, checkpoint, int(row["window_seq"]))

    def plan_initial_baseline(self, metadata: TableMetadata) -> SourcePlan:
        """Qualify and plan only the explicit first baseline snapshot.

        Args:
            metadata: Current pinned Iceberg table metadata.

        Returns:
            A single baseline window or an empty plan after durable rejection.
        """
        if self.baseline_snapshot_id is None:
            raise RuntimeError("initial planning requires LANCE_ETL_CANONICAL_BASELINE_SNAPSHOT_ID")
        if metadata.current_snapshot_id is None:
            return SourcePlan(metadata.table_uuid, None, metadata.active_partition_spec.spec_id, ())
        snapshots = self.catalog.snapshots_through(
            self.table,
            metadata.current_snapshot_id,
            self.baseline_snapshot_id,
        )
        baseline_snapshot = index_snapshots(snapshots).get(self.baseline_snapshot_id)
        if baseline_snapshot is None:
            raise SourceLineageError("pinned baseline is not retained in the requested ancestry")
        try:
            validate_snapshot_identity(
                baseline_snapshot,
                metadata.table_uuid,
                metadata.active_partition_spec.spec_id,
            )
            proof = self.baseline_qualifier.qualify(self.table, metadata, self.baseline_snapshot_id)
            accepted = (
                proof.table_uuid == metadata.table_uuid
                and proof.snapshot_id == self.baseline_snapshot_id
                and proof.partition_spec_id == baseline_snapshot.partition_spec_id
                and proof.canonical
                and proof.distinct_mutation_conflicts == 0
            )
            if not accepted:
                raise SourceBaselineError("baseline qualification found distinct mutations or mismatched identity")
            entries = self.catalog.manifest_entries(self.table, self.baseline_snapshot_id)
            touched = discover_baseline_targets(baseline_snapshot, entries)
            window = build_window(self.table, baseline_snapshot, WindowKind.BASELINE, touched)
        except (SourceBaselineError, SourceLineageError, SourceSnapshotBlockedError) as exc:
            if isinstance(exc, SourceSnapshotBlockedError):
                error_code = exc.error_code
            elif isinstance(exc, SourceLineageError):
                error_code = "BASELINE_IDENTITY_MISMATCH"
            else:
                error_code = "BASELINE_NOT_CANONICAL"
            self.persist_rejected(baseline_snapshot, error_code, baseline=True)
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
        checkpoint_window_seq: int,
    ) -> SourcePlan:
        """Inspect only the direct next descendant and stop at its first rejection.

        Args:
            metadata: Current pinned table metadata.
            checkpoint: Exact durable audit tip.
            checkpoint_window_seq: Durable row identity used for an atomic safe-tip gate.

        Returns:
            One accepted next window or an empty plan after durable rejection.
        """
        if metadata.current_snapshot_id is None:
            return SourcePlan(metadata.table_uuid, None, metadata.active_partition_spec.spec_id, ())
        try:
            snapshots = self.catalog.snapshots_through(
                self.table,
                metadata.current_snapshot_id,
                checkpoint.snapshot_id,
            )
            snapshot = direct_next_snapshot(
                snapshots,
                metadata.current_snapshot_id,
                checkpoint.snapshot_id,
            )
        except (SourceContractError, SourceLineageError):
            self.repository.block_source_window(checkpoint_window_seq, "SOURCE_LINEAGE_UNTRUSTED")
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
            window = SourcePlanner(self.catalog).plan_snapshot(self.table, snapshot)
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

    def persist_rejected(self, snapshot: SnapshotRecord, error_code: str, baseline: bool = False) -> int:
        """Persist one exact rejected source snapshot without target children.

        Args:
            snapshot: First unsupported direct descendant.
            error_code: Bounded planning classification.
            baseline: Whether this is the explicitly qualified initial baseline.

        Returns:
            Durable rejected audit sequence.
        """
        return self.repository.enqueue_blocked_source_window(
            SourceWindowPlan(
                table_uuid=uuid.UUID(snapshot.table_uuid),
                snapshot_id=snapshot.snapshot_id,
                parent_snapshot_id=snapshot.parent_snapshot_id,
                iceberg_sequence_number=snapshot.sequence_number,
                partition_spec_id=snapshot.partition_spec_id,
                kind=SourceWindowKind.BASELINE if baseline else SourceWindowKind.REJECTED,
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
    indexed = index_snapshots(snapshots)
    cursor = head_snapshot_id
    visited: set[int] = set()
    child: SnapshotRecord | None = None
    while cursor != checkpoint_snapshot_id:
        if cursor in visited:
            raise SourceLineageError("cycle detected in pinned Iceberg snapshot ancestry")
        visited.add(cursor)
        record = indexed.get(cursor)
        if record is None:
            raise SourceLineageError("pinned Iceberg ancestry is missing a required snapshot")
        child = record
        if record.parent_snapshot_id is None:
            raise SourceLineageError("pinned Iceberg head does not descend from the durable audit tip")
        cursor = record.parent_snapshot_id
    return child


def snapshot_from_row(table_uuid: str, spec_id: int, row: Row) -> SnapshotRecord:
    """Normalize one Spark snapshots metadata row.

    Args:
        table_uuid: Stable table identity.
        spec_id: Approved partition specification.
        row: Spark metadata row.

    Returns:
        Typed snapshot record.
    """
    values = row.asDict(recursive=True)
    committed = values["committed_at"]
    committed_at_ms = int(committed.timestamp() * 1000)
    return SnapshotRecord(
        table_uuid=table_uuid,
        snapshot_id=int(values["snapshot_id"]),
        parent_snapshot_id=int(values["parent_id"]) if values.get("parent_id") is not None else None,
        sequence_number=int(values["sequence_number"]),
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
    values = row.asDict(recursive=True)
    resolved_snapshot = snapshot_id if snapshot_id is not None else int(values["entry_snapshot_id"])
    resolved_status = status or MANIFEST_STATUS_BY_CODE[int(values["entry_status"])]
    return ManifestEntry(
        snapshot_id=resolved_snapshot,
        partition_spec_id=int(values["entry_spec_id"]),
        status=resolved_status,
        content=MANIFEST_CONTENT_BY_CODE[int(values["entry_content"])],
        partition=PartitionValues(
            tenant_id=str(values["tenant_id"]),
            namespace=str(values["namespace"]),
            org_id=str(values["org_id"]),
            processing_timestamp_hour=int(values["processing_timestamp_hour"]),
        ),
    )
