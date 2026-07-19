"""Concrete fenced Spark execution for exact source ingestion and target serving phases."""

from __future__ import annotations

import hashlib
import json
import struct
import threading
import uuid
from collections.abc import Iterator
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any, Protocol

import lance
import pyarrow as pa
import pyarrow.compute as pc
from pyspark import StorageLevel
from pyspark.errors import AnalysisException
from pyspark.sql import Column, DataFrame, Row, SparkSession
from pyspark.sql.functions import (
    array,
    array_except,
    col,
    countDistinct,
    element_at,
    lit,
    lower,
    map_keys,
    size,
    trim,
)
from pyspark.sql.functions import (
    max as spark_max,
)
from pyspark.sql.types import (
    ArrayType,
    BinaryType,
    BooleanType,
    DataType,
    FloatType,
    LongType,
    MapType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from lance_etl.cloud_storage import resolve_filesystem
from lance_etl.etl.completion import CompletionMarker, finalize_completion_marker
from lance_etl.etl.digest import SOURCE_DIGEST_HEADER, canonical_event_digest, encode_bytes
from lance_etl.etl.mutation import DELETE_OPERATIONS, UPSERT_OPERATIONS, normalize_operation
from lance_etl.etl.pivot import apply_fsl_cast, apply_ttl_cast
from lance_etl.etl.replay_sink import (
    DELETED_COLUMN,
    EVENT_DIGEST_COLUMN,
    SOURCE_SEQUENCE_COLUMN,
    WINDOW_SEQUENCE_COLUMN,
    ReplayConflict,
    ReplayMergeResult,
    open_or_create_replay_dataset,
    replay_safe_merge,
    replay_table_chunks,
)
from lance_etl.etl.sink import dataset_absent
from lance_etl.indexing import (
    IndexJobConfig,
    LanceIndexer,
    load_vector_config,
)
from lance_etl.maintenance import MaintenanceConfig, MaintenanceJob
from lance_etl.publication.manifest import candidate_pin_name, schema_fingerprint, tag_version
from lance_etl.reconciler.config import ReconcilerSettings
from lance_etl.reconciler.prewarm import ExactPrewarmer
from lance_etl.reconciler.results import ResultKind, WorkResult
from lance_etl.source import SnapshotRecord, SparkScanPlan, TargetKey, WindowKind, build_spark_scan, execute_spark_scan
from lance_etl.state import (
    DatasetSpecRevision,
    IcebergSource,
    IndexDefinition,
    IndexType,
    PublicationEvidence,
    PublicationIndexEvidence,
    WorkClaim,
    WorkExecutionContext,
    WorkKind,
)
from lance_etl.telemetry import Telemetry, TelemetryConfig


class ExecutionContextRepository(Protocol):
    """Read the exact immutable context for a live fenced claim."""

    def work_execution_context(self, claim: WorkClaim) -> WorkExecutionContext | None:
        """Resolve one claim while its lease and target fence remain current.

        Args:
            claim: Fenced target work.

        Returns:
            Exact execution context or ``None`` for a stale claim.
        """
        ...

    def renew_lease(self, claim: WorkClaim, lease_duration: timedelta) -> bool:
        """Renew one still-current worker lease.

        Args:
            claim: Current fenced claim.
            lease_duration: New lease duration from renewal.

        Returns:
            Whether the same fence still owns the work.
        """
        ...


class ServeWorkRunner(Protocol):
    """Run one configured SERVE or REBUILD phase."""

    def run(self, context: WorkExecutionContext) -> WorkResult:
        """Execute the claim's exact durable serving phase.

        Args:
            context: Live fenced execution context.

        Returns:
            Typed phase or publication result.
        """
        ...


@dataclass(frozen=True, slots=True)
class DistributedIngestRunner:
    """Execute one exact dataset snapshot under its frozen PostgreSQL specification."""

    spark: SparkSession
    telemetry_config: TelemetryConfig

    def run(self, context: WorkExecutionContext) -> WorkResult:
        """Normalize, conflict-check, digest, write, verify, and mark one source target.

        Args:
            context: Live INGEST context with exact source snapshot metadata.

        Returns:
            Exact completion evidence or a contract-block result.
        """
        if context.claim.kind is not WorkKind.INGEST or context.claim.source_snapshot_seq is None:
            raise ValueError("distributed ingest runner requires an INGEST claim")
        if (
            context.snapshot_id is None
            or context.iceberg_sequence_number is None
            or context.source_snapshot_kind is None
        ):
            raise ValueError("INGEST context lacks exact source snapshot metadata")
        snapshot: SnapshotRecord = SnapshotRecord(
            table_uuid=str(context.source.table_uuid),
            snapshot_id=context.snapshot_id,
            parent_snapshot_id=context.parent_snapshot_id,
            sequence_number=context.iceberg_sequence_number,
            committed_at_ms=0,
            operation="append",
            partition_spec_id=0,
        )
        spec: DatasetSpecRevision = context.spec_revision
        scan: SparkScanPlan = build_spark_scan(
            context.source_table,
            snapshot,
            WindowKind(context.source_snapshot_kind.value),
            TargetKey(context.identity.tenant_id, context.identity.namespace, context.identity.org_id),
            (
                context.source.tenant_column,
                context.source.namespace_column,
                context.source.org_column,
            ),
        )
        exc: AnalysisException | ValueError
        try:
            source: DataFrame = self.canonical_source(execute_spark_scan(self.spark, scan), context)
            self.validate_source_profile(source, spec)
            selected: DataFrame = self.select_profile_fields(source, spec)
            terminal: DataFrame = self.normalize_terminal(selected, context).persist(StorageLevel.MEMORY_AND_DISK)
            try:
                if terminal.where(col("vector_id").isNull()).limit(1).count():
                    return blocked_result(context.claim, "NULL_VECTOR_ID", "source contains a null vector_id")
                conflict: int = (
                    terminal.groupBy("vector_id")
                    .agg(countDistinct(EVENT_DIGEST_COLUMN).alias("distinct_mutations"))
                    .where(col("distinct_mutations") > 1)
                    .limit(1)
                    .count()
                )
                if conflict:
                    return blocked_result(
                        context.claim,
                        "SAME_SNAPSHOT_CONFLICT",
                        "source snapshot contains distinct unordered mutations for one vector_id",
                    )
                collapsed: DataFrame = terminal.dropDuplicates(["vector_id", EVENT_DIGEST_COLUMN]).dropDuplicates(
                    ["vector_id"]
                )
                source_digest: bytes
                source_rows: int
                source_digest, source_rows = self.compute_source_digest(collapsed)
                versions: list[int] = self.write_terminal(collapsed, context)
                if source_rows > 0 and not versions:
                    raise RuntimeError("terminal write produced no verified executor result")
                marker: CompletionMarker = self.finalize_marker(context, source_digest)
                return WorkResult(
                    claim=context.claim,
                    kind=ResultKind.INGEST_SUCCEEDED,
                    data_lance_version=marker.lance_version,
                    source_row_count=source_rows,
                    source_digest=source_digest,
                )
            finally:
                terminal.unpersist()
        except (AnalysisException, ValueError) as exc:
            return blocked_result(context.claim, "SOURCE_PROFILE_VIOLATION", str(exc))

    def canonical_source(self, source: DataFrame, context: WorkExecutionContext) -> DataFrame:
        """Project PostgreSQL-configured source columns into the canonical worker contract.

        Args:
            source: Exact Iceberg target scan using physical source names.
            context: Source registration carrying the persisted column mapping.

        Returns:
            DataFrame with canonical ingestion names.
        """
        registration: IcebergSource = context.source
        projections: list[Column] = [
            col(registration.tenant_column).alias("tenant_id"),
            col(registration.namespace_column).alias("namespace"),
            col(registration.org_column).alias("org_id"),
            col(registration.record_id_column).alias("vector_id"),
            col(registration.operation_column).alias("op"),
            col(registration.event_time_column).alias("event_timestamp"),
            col(registration.vectors_column).alias("vectors"),
            col(registration.texts_column).alias("texts"),
            col(registration.metadata_column).alias("metadata"),
        ]
        if context.spec_revision.include_ttl:
            if registration.ttl_column is None:
                raise ValueError("source registration omits ttl_column required by the dataset specification")
            projections.append(col(registration.ttl_column).alias("ttl"))
        return source.select(*projections)

    def validate_source_profile(self, source: DataFrame, spec: DatasetSpecRevision) -> None:
        """Reject unknown map keys and wrong vector dimensions before the first write.

        Args:
            source: Exact target snapshot scan.
            spec: Frozen dataset specification.

        Raises:
            ValueError: If source fields violate the frozen dataset specification.
        """
        self.validate_source_schema(source, spec)
        if source.where(col("event_timestamp").isNull()).limit(1).count():
            raise ValueError("source contains a null event_timestamp")
        normalized_operation: Column = lower(trim(col("op")))
        supported: tuple[str, ...] = tuple(sorted(UPSERT_OPERATIONS | DELETE_OPERATIONS))
        if source.where(col("op").isNull() | ~normalized_operation.isin(*supported)).limit(1).count():
            raise ValueError("source contains an unsupported mutation operation")
        self.validate_map_keys(source, spec)
        self.validate_vector_dimensions(source, normalized_operation, spec)

    def validate_source_schema(self, source: DataFrame, spec: DatasetSpecRevision) -> None:
        """Validate the fixed physical source types before distributed checks.

        Args:
            source: Exact target snapshot scan.
            spec: Frozen dataset specification.

        Raises:
            ValueError: If a required column is absent or carries the wrong Spark type.
        """
        required_columns: set[str] = {
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
        if spec.include_ttl:
            required_columns.add("ttl")
        missing_columns: list[str] = sorted(required_columns - set(source.columns))
        if missing_columns:
            raise ValueError(f"source is missing required columns {missing_columns}")
        string_columns: tuple[str, ...] = ("tenant_id", "namespace", "org_id", "vector_id", "op")
        if any(not isinstance(source.schema[name].dataType, StringType) for name in string_columns):
            raise ValueError("source routing, vector_id, and op columns must be strings")
        if not isinstance(source.schema["event_timestamp"].dataType, TimestampType):
            raise ValueError("source event_timestamp must be a timestamp")
        vectors_type: DataType = source.schema["vectors"].dataType
        texts_type: DataType = source.schema["texts"].dataType
        metadata_type: DataType = source.schema["metadata"].dataType
        vectors_valid: bool = (
            isinstance(vectors_type, MapType)
            and isinstance(vectors_type.keyType, StringType)
            and isinstance(vectors_type.valueType, ArrayType)
            and isinstance(vectors_type.valueType.elementType, FloatType)
        )
        strings_valid: bool = True
        map_type: DataType
        for map_type in (texts_type, metadata_type):
            strings_valid = strings_valid and (
                isinstance(map_type, MapType)
                and isinstance(map_type.keyType, StringType)
                and isinstance(map_type.valueType, StringType)
            )
        if not vectors_valid or not strings_valid:
            raise ValueError("source maps must match vectors<string,array<float>> and text metadata string maps")
        if spec.include_ttl and not isinstance(source.schema["ttl"].dataType, LongType):
            raise ValueError("source ttl must be bigint seconds")

    def validate_map_keys(self, source: DataFrame, spec: DatasetSpecRevision) -> None:
        """Reject dynamic map keys outside the frozen dataset specification.

        Args:
            source: Exact target snapshot scan.
            spec: Frozen dataset specification.

        Raises:
            ValueError: If a map contains a field outside the dataset specification.
        """
        map_contracts: tuple[tuple[str, tuple[str, ...]], ...] = (
            ("vectors", tuple(vector_field[0] for vector_field in spec.vector_fields)),
            ("texts", spec.text_fields),
            ("metadata", spec.metadata_fields),
        )
        map_column: str
        allowed: tuple[str, ...]
        for map_column, allowed in map_contracts:
            keys: Column = map_keys(col(map_column))
            unknown_count: Column = (
                size(keys) if not allowed else size(array_except(keys, array(*(lit(name) for name in allowed))))
            )
            if source.where(unknown_count > 0).limit(1).count():
                raise ValueError(
                    f"source map {map_column!r} contains fields outside spec revision {spec.spec_revision_id}"
                )

    def validate_vector_dimensions(
        self,
        source: DataFrame,
        normalized_operation: Column,
        spec: DatasetSpecRevision,
    ) -> None:
        """Validate required vector presence and fixed dimensions.

        Args:
            source: Exact target snapshot scan.
            normalized_operation: Normalized Spark operation expression.
            spec: Frozen dataset specification.

        Raises:
            ValueError: If an upsert omits a vector or any vector has the wrong dimension.
        """
        delete_operation: Column = normalized_operation.isin(*tuple(sorted(DELETE_OPERATIONS)))
        name: str
        dimension: int
        for name, dimension in spec.vector_fields:
            vector: Column = element_at(col("vectors"), lit(name))
            if source.where(~delete_operation & vector.isNull()).limit(1).count():
                raise ValueError(f"upsert is missing required vector field {name!r}")
            if source.where(vector.isNotNull() & (size(vector) != dimension)).limit(1).count():
                raise ValueError(f"vector field {name!r} does not match fixed dimension {dimension}")

    def select_profile_fields(self, source: DataFrame, spec: DatasetSpecRevision) -> DataFrame:
        """Project maps into the frozen complete post-image schema.

        Args:
            source: Validated exact target scan.
            spec: Frozen dataset specification.

        Returns:
            Projected rows containing every allowed field.
        """
        fields: list[Column] = [
            col("vector_id"),
            col("op"),
            col("event_timestamp"),
            *(
                element_at(col("vectors"), lit(vector_field[0])).alias(vector_field[0])
                for vector_field in spec.vector_fields
            ),
            *(element_at(col("texts"), lit(name)).alias(name) for name in spec.text_fields),
            *(element_at(col("metadata"), lit(name)).alias(name) for name in spec.metadata_fields),
        ]
        if spec.include_ttl:
            fields.append(col("ttl"))
        return source.select(*fields)

    def normalize_terminal(self, source: DataFrame, context: WorkExecutionContext) -> DataFrame:
        """Attach canonical event identity and tombstone fields in executor Arrow batches.

        Args:
            source: Frozen-specification source projection.
            context: Exact source ordering and work identity.

        Returns:
            Terminal mutation lineage before duplicate collapse.
        """
        schema: StructType = terminal_spark_schema(context.spec_revision)
        target: tuple[str, str, str] = (
            context.identity.tenant_id,
            context.identity.namespace,
            context.identity.org_id,
        )
        window_seq: int = int(context.claim.source_snapshot_seq or 0)
        source_sequence: int = int(context.iceberg_sequence_number or 0)
        profile: DatasetSpecRevision = context.spec_revision

        def normalize_batches(batches: Iterator[pa.RecordBatch]) -> Iterator[pa.RecordBatch]:
            """Normalize bounded source batches without collecting a target on the driver.

            Args:
                batches: Spark Arrow batches.

            Yields:
                Canonical terminal mutation batches.
            """
            arrow_schema: pa.Schema = terminal_arrow_schema(profile)
            payload_names: tuple[str, ...] = spec_payload_names(profile)
            batch: pa.RecordBatch
            for batch in batches:
                records: list[dict[str, object]] = []
                row: dict[str, object]
                for row in pa.Table.from_batches([batch]).to_pylist():
                    operation: str = normalize_operation(str(row["op"]))
                    event_timestamp: datetime = row["event_timestamp"]
                    if event_timestamp.tzinfo is None:
                        event_timestamp = event_timestamp.replace(tzinfo=UTC)
                    payload: dict[str, object] = {name: row.get(name) for name in payload_names}
                    digest: bytes = canonical_event_digest(
                        target,
                        str(row["vector_id"]),
                        operation,
                        event_timestamp,
                        payload,
                    )
                    deleted: bool = operation == "delete"
                    terminal_row: dict[str, object] = {
                        "vector_id": row["vector_id"],
                        "event_timestamp": None if deleted else event_timestamp,
                        **{name: None if deleted else payload[name] for name in payload_names},
                        WINDOW_SEQUENCE_COLUMN: window_seq,
                        SOURCE_SEQUENCE_COLUMN: source_sequence,
                        EVENT_DIGEST_COLUMN: digest,
                        DELETED_COLUMN: deleted,
                    }
                    records.append(terminal_row)
                table: pa.Table = pa.Table.from_pylist(records, schema=arrow_schema)
                yield from table.to_batches()

        return source.mapInArrow(normalize_batches, schema=schema)

    def compute_source_digest(self, terminal: DataFrame) -> tuple[bytes, int]:
        """Compute the frozen digest in one streaming sorted executor reduction.

        Args:
            terminal: Duplicate-collapsed target mutations.

        Returns:
            Raw digest and terminal row count.
        """
        ordered: DataFrame = (
            terminal.select("vector_id", SOURCE_SEQUENCE_COLUMN, EVENT_DIGEST_COLUMN)
            .repartition(1)
            .sortWithinPartitions("vector_id")
        )

        def digest_batches(batches: Iterator[pa.RecordBatch]) -> Iterator[pa.RecordBatch]:
            """Stream one globally sorted partition into a SHA-256 state.

            Args:
                batches: Globally ordered terminal tuple batches.

            Yields:
                One digest and row-count batch.
            """
            digest: Any = hashlib.sha256()
            digest.update(SOURCE_DIGEST_HEADER)
            count: int = 0
            batch: pa.RecordBatch
            for batch in batches:
                row: dict[str, object]
                for row in pa.Table.from_batches([batch]).to_pylist():
                    digest.update(encode_bytes(str(row["vector_id"]).encode("utf-8")))
                    digest.update(struct.pack(">q", int(row[SOURCE_SEQUENCE_COLUMN])))
                    digest.update(bytes(row[EVENT_DIGEST_COLUMN]))
                    count += 1
            yield pa.RecordBatch.from_pydict({"source_digest": [digest.digest()], "source_rows": [count]})

        result: list[Row] = ordered.mapInArrow(digest_batches, "source_digest binary, source_rows long").collect()
        if len(result) != 1:
            raise RuntimeError("source digest reduction did not produce exactly one result")
        return bytes(result[0]["source_digest"]), int(result[0]["source_rows"])

    def write_terminal(self, terminal: DataFrame, context: WorkExecutionContext) -> list[int]:
        """Write key-disjoint terminal partitions through replay-safe executor merges.

        Args:
            terminal: Duplicate-collapsed target mutations.
            context: Live fenced target context.

        Returns:
            Exact verified Lance versions observed by writer partitions.
        """
        profile: DatasetSpecRevision = context.spec_revision
        telemetry_config: TelemetryConfig = self.telemetry_config
        uri: str = context.claim.ingest_lance_uri
        partitioned: DataFrame = terminal.repartition(
            profile.ingest_shuffle_partitions,
            col("vector_id"),
        ).sortWithinPartitions("vector_id")

        def write_batches(batches: Iterator[pa.RecordBatch]) -> Iterator[pa.RecordBatch]:
            """Write bounded Arrow batches with unique keys per Spark partition.

            Args:
                batches: Key-disjoint terminal batches.

            Yields:
                One exact verified maximum version per non-empty partition.
            """
            telemetry: Telemetry = Telemetry.create(telemetry_config)
            maximum_version: int | None = None
            batch: pa.RecordBatch
            for batch in batches:
                if batch.num_rows == 0:
                    continue
                table: pa.Table = pa.Table.from_batches([batch])
                digest_index: int = table.schema.get_field_index(EVENT_DIGEST_COLUMN)
                table = table.set_column(
                    digest_index,
                    pa.field(EVENT_DIGEST_COLUMN, pa.binary(32)),
                    pc.cast(table[EVENT_DIGEST_COLUMN], pa.binary(32)),
                )
                if profile.include_ttl:
                    table = apply_ttl_cast(table, "ttl")
                name: str
                dimension: int
                for name, dimension in profile.vector_fields:
                    table = apply_fsl_cast(table, name, {}, dimension)
                    invalid: Any = pc.and_(pc.invert(table[DELETED_COLUMN]), pc.is_null(table[name]))
                    if table[name].null_count and pc.any(invalid).as_py():
                        raise ValueError(f"vector field {name!r} became null during fixed-dimension cast")
                stored_schema: pa.Schema = persisted_arrow_schema(profile)
                table = table.select(stored_schema.names).cast(stored_schema)
                maximum_rows: int = min(profile.merge_rows_per_chunk, profile.write_rows_per_fragment)
                merge_table: pa.Table
                for merge_table in replay_table_chunks(table, maximum_rows, profile.merge_batch_bytes):
                    result: ReplayMergeResult = replay_safe_merge(
                        uri,
                        merge_table,
                        telemetry,
                        max_rows_per_file=profile.write_rows_per_fragment,
                    )
                    maximum_version = max(maximum_version or 0, result.lance_version)
            if maximum_version is not None:
                yield pa.RecordBatch.from_pydict({"lance_version": [maximum_version]})

        return [
            int(row["lance_version"]) for row in partitioned.mapInArrow(write_batches, "lance_version long").collect()
        ]

    def finalize_marker(self, context: WorkExecutionContext, source_digest: bytes) -> CompletionMarker:
        """Commit the completion marker on one executor after all data writes verify.

        Args:
            context: Live target work context.
            source_digest: Frozen target and window digest.

        Returns:
            Reopened durable marker.
        """
        telemetry_config: TelemetryConfig = self.telemetry_config
        uri: str = context.claim.ingest_lance_uri
        window_seq: int = int(context.claim.source_snapshot_seq or 0)

        def finalize(item: tuple[str, int, bytes]) -> CompletionMarker:
            """Finalize one marker in its dedicated executor task.

            Args:
                item: URI, window sequence, and digest.

            Returns:
                Durable completion marker.
            """
            target_uri: str
            target_window: int
            target_digest: bytes
            target_uri, target_window, target_digest = item
            return finalize_completion_marker(
                target_uri,
                target_window,
                target_digest,
                Telemetry.create(telemetry_config),
            )

        return self.spark.sparkContext.parallelize([(uri, window_seq, source_digest)], 1).map(finalize).collect()[0]


@dataclass(frozen=True, slots=True)
class ConfiguredPublicationRunner:
    """Maintain, index, qualify, and prewarm under a frozen dataset specification."""

    spark: SparkSession
    telemetry_config: TelemetryConfig
    prewarmer: ExactPrewarmer

    def run(self, context: WorkExecutionContext) -> WorkResult:
        """Produce immutable qualification evidence for one serving generation.

        Args:
            context: Live fenced PUBLISH or REBUILD context.

        Returns:
            Publication evidence only after every required index has full coverage.
        """
        if context.claim.kind not in (WorkKind.PUBLISH, WorkKind.REBUILD):
            raise ValueError("publication runner requires PUBLISH or REBUILD work")
        spec: DatasetSpecRevision = context.spec_revision
        candidate_uri: str = (
            context.candidate_lance_uri
            if context.claim.kind is WorkKind.REBUILD and context.candidate_lance_uri is not None
            else context.claim.ingest_lance_uri
        )
        pin: str = candidate_pin_name(context.claim.work_id)
        candidate_version: int | None = self.candidate_pin_version(candidate_uri, pin)
        if candidate_version is None:
            if context.claim.kind is WorkKind.REBUILD:
                rebuild_result: str | WorkResult = self.canonical_rebuild(context)
                if isinstance(rebuild_result, WorkResult):
                    return rebuild_result
                candidate_uri = rebuild_result
            if spec.compaction_enabled:
                maintenance: dict[str, object] = MaintenanceJob(
                    MaintenanceConfig(
                        telemetry=self.telemetry_config,
                        ttl_column=spec.ttl_field.target_name if spec.ttl_field is not None else None,
                        target_rows_per_fragment=spec.target_rows_per_fragment,
                        materialize_deletions=spec.materialize_deletions,
                        materialize_deletions_threshold=spec.materialize_deletions_threshold,
                        compaction_mode=spec.compaction_mode.value,
                        defer_index_remap=spec.defer_index_remap,
                        max_source_fragments=spec.max_source_fragments,
                        num_threads=spec.compaction_threads,
                        cleanup_older_than_seconds=spec.cleanup_older_than_seconds,
                        retain_versions=spec.retain_versions,
                    )
                ).run(self.spark, [candidate_uri])[0]
                if maintenance.get("error"):
                    return retry_result(context.claim, "MAINTENANCE_FAILED", str(maintenance["error"]))
            indexing: list[dict[str, object]] = self.run_indexing(candidate_uri, spec)
            index_errors: list[dict[str, object]] = [
                item for result in indexing for item in result.get("indexes", []) if item.get("error")
            ]
            top_level_error: object | None = next(
                (result.get("error") for result in indexing if result.get("error")),
                None,
            )
            if top_level_error or index_errors:
                message: str = str(top_level_error or index_errors[0].get("error"))
                return retry_result(context.claim, "INDEX_BUILD_FAILED", message)
            candidate_version = self.candidate_version(candidate_uri)
            pin_error: ReplayConflict
            try:
                self.pin_candidate(candidate_uri, candidate_version, pin)
            except ReplayConflict as pin_error:
                return blocked_result(context.claim, "IMMUTABLE_CANDIDATE_PIN_CONFLICT", str(pin_error))
        counts: tuple[int, int, int, int] = self.candidate_counts(candidate_uri, candidate_version, spec)
        qualification: dict[str, object] = self.qualify_candidate(candidate_uri, spec, candidate_version, counts)
        if qualification.get("error_code"):
            return blocked_result(
                context.claim,
                str(qualification["error_code"]),
                str(qualification["error_message"]),
            )
        manifest_uri: str
        manifest_digest: bytes
        manifest_uri, manifest_digest = self.persist_manifest(context, candidate_uri, qualification)
        evidence: PublicationEvidence = publication_evidence(spec, qualification)
        if spec.prewarm_required:
            prewarm_error: RuntimeError
            try:
                self.prewarmer.prewarm(context.identity, candidate_uri, candidate_version)
            except RuntimeError as prewarm_error:
                return retry_result(context.claim, "PREWARM_FAILED", str(prewarm_error))
        return WorkResult(
            claim=context.claim,
            kind=ResultKind.PUBLISH_SUCCEEDED,
            candidate_lance_uri=candidate_uri,
            indexed_lance_version=int(qualification["lance_version"]),
            manifest_uri=manifest_uri,
            manifest_digest=manifest_digest,
            publication_evidence=evidence,
        )

    def canonical_rebuild(self, context: WorkExecutionContext) -> str | WorkResult:
        """Rewrite one exact applied source version into a canonical isolated candidate.

        Args:
            context: Live deterministic REBUILD work context.

        Returns:
            Candidate URI, or a blocking result for an irreconcilable duplicate conflict.
        """
        source_uri: str = context.claim.ingest_lance_uri
        source_version: int | None = context.claim.ingest_lance_version
        candidate_uri: str | None = context.candidate_lance_uri
        if source_version is None or candidate_uri is None:
            return blocked_result(context.claim, "REBUILD_CONTEXT_MISSING", "rebuild lacks exact source or candidate")
        self.ensure_rebuild_candidate(source_uri, source_version, candidate_uri)
        fragment_ids: list[int] = self.fragment_inventory(source_uri, source_version)
        profile: DatasetSpecRevision = context.spec_revision
        tasks: list[tuple[str, int, list[int], list[str]]] = [
            (source_uri, source_version, list(shard), list(terminal_column_names(profile)))
            for shard in shard_fragments(fragment_ids, profile.ingest_shuffle_partitions)
        ]

        def read_batches(task_batches: Iterator[pa.RecordBatch]) -> Iterator[pa.RecordBatch]:
            """Stream exact source fragment shards as Arrow batches.

            Args:
                task_batches: Small Arrow batches of source URI, version, fragments, and columns.

            Yields:
                Stored terminal Arrow batches without Python objects per source row.
            """
            task_batch: pa.RecordBatch
            for task_batch in task_batches:
                task: dict[str, object]
                for task in task_batch.to_pylist():
                    dataset: lance.LanceDataset = lance.dataset(
                        str(task["source_uri"]),
                        version=int(task["source_version"]),
                    )
                    selected: set[int] = set(task["fragment_ids"])
                    fragments: list[Any] = [
                        fragment for fragment in dataset.get_fragments() if fragment.fragment_id in selected
                    ]
                    reader: pa.RecordBatchReader = dataset.scanner(
                        columns=task["columns"],
                        fragments=fragments,
                    ).to_reader()
                    batch: pa.RecordBatch
                    for batch in reader:
                        table: pa.Table = pa.Table.from_batches([batch])
                        if profile.include_ttl:
                            ttl_index: int = table.schema.get_field_index("ttl")
                            table = table.set_column(
                                ttl_index,
                                pa.field("ttl", pa.int64()),
                                pc.cast(table["ttl"], pa.int64()),
                            )
                        name: str
                        dimension: int
                        for name, dimension in profile.vector_fields:
                            del dimension
                            vector_index: int = table.schema.get_field_index(name)
                            table = table.set_column(
                                vector_index,
                                pa.field(name, pa.list_(pa.float32())),
                                pc.cast(table[name], pa.list_(pa.float32())),
                            )
                        digest_index: int = table.schema.get_field_index(EVENT_DIGEST_COLUMN)
                        table = table.set_column(
                            digest_index,
                            pa.field(EVENT_DIGEST_COLUMN, pa.binary()),
                            pc.cast(table[EVENT_DIGEST_COLUMN], pa.binary()),
                        )
                        yield from table.to_batches()

        task_schema: StructType = StructType(
            [
                StructField("source_uri", StringType(), False),
                StructField("source_version", LongType(), False),
                StructField("fragment_ids", ArrayType(LongType(), False), False),
                StructField("columns", ArrayType(StringType(), False), False),
            ]
        )
        task_frame: DataFrame = self.spark.createDataFrame(tasks, task_schema)
        rows: DataFrame = task_frame.mapInArrow(read_batches, terminal_spark_schema(profile)).persist(
            StorageLevel.MEMORY_AND_DISK
        )
        winners: DataFrame | None = None
        try:
            maxima: DataFrame = rows.groupBy("vector_id").agg(
                spark_max(SOURCE_SEQUENCE_COLUMN).alias("maximum_source_sequence")
            )
            winners = (
                rows.alias("terminal_rows")
                .join(
                    maxima.alias("sequence_maxima"),
                    (col("terminal_rows.vector_id") == col("sequence_maxima.vector_id"))
                    & (
                        col(f"terminal_rows.{SOURCE_SEQUENCE_COLUMN}") == col("sequence_maxima.maximum_source_sequence")
                    ),
                )
                .select("terminal_rows.*", "sequence_maxima.maximum_source_sequence")
            )
            winners.persist(StorageLevel.MEMORY_AND_DISK)
            conflicts: int = (
                winners.groupBy("vector_id")
                .agg(countDistinct(EVENT_DIGEST_COLUMN).alias("distinct_mutations"))
                .where(col("distinct_mutations") > 1)
                .limit(1)
                .count()
            )
            if conflicts:
                return blocked_result(
                    context.claim,
                    "REBUILD_EQUAL_SEQUENCE_CONFLICT",
                    "duplicate rows at the maximum source sequence carry different event digests",
                )
            canonical: DataFrame = winners.drop("maximum_source_sequence").dropDuplicates(
                ["vector_id", SOURCE_SEQUENCE_COLUMN, EVENT_DIGEST_COLUMN]
            )
            rebuild_error: ReplayConflict | ValueError
            try:
                self.write_rebuild_candidate(canonical, candidate_uri, profile)
            except (ReplayConflict, ValueError) as rebuild_error:
                return blocked_result(context.claim, "REBUILD_CANDIDATE_CONFLICT", str(rebuild_error))
        finally:
            if winners is not None:
                winners.unpersist()
            rows.unpersist()
        return candidate_uri

    def ensure_rebuild_candidate(self, source_uri: str, source_version: int, candidate_uri: str) -> int:
        """Create an empty isolated candidate with the exact stored source schema.

        Args:
            source_uri: Existing canonical source dataset URI.
            source_version: Exact pinned source version.
            candidate_uri: Work-derived isolated candidate URI.

        Returns:
            Candidate bootstrap version.
        """
        telemetry_config: TelemetryConfig = self.telemetry_config

        def ensure(item: tuple[str, int, str]) -> int:
            """Bootstrap the candidate from one executor.

            Args:
                item: Source URI, exact source version, and candidate URI.

            Returns:
                Exact candidate version after idempotent bootstrap.
            """
            existing_uri: str
            version: int
            destination: str
            existing_uri, version, destination = item
            source: lance.LanceDataset = lance.dataset(existing_uri, version=version)
            candidate: lance.LanceDataset = open_or_create_replay_dataset(
                destination,
                source.schema.empty_table(),
                None,
            )
            Telemetry.create(telemetry_config).incr("rebuild.candidate_bootstrap")
            return int(candidate.version)

        versions: list[int] = (
            self.spark.sparkContext.parallelize([(source_uri, source_version, candidate_uri)], 1).map(ensure).collect()
        )
        if len(versions) != 1:
            raise RuntimeError("rebuild candidate bootstrap produced an invalid executor result")
        return int(versions[0])

    def fragment_inventory(self, candidate_uri: str, lance_version: int) -> list[int]:
        """Resolve one exact candidate fragment inventory on an executor.

        Args:
            candidate_uri: Candidate Lance dataset.
            lance_version: Exact pinned version.

        Returns:
            Fragment identifiers for distributed executor scans.
        """

        def inventory(item: tuple[str, int]) -> list[int]:
            """Return exact fragment identifiers from one executor open.

            Args:
                item: Candidate URI and exact version.

            Returns:
                Fragment identifiers.
            """
            uri: str
            version: int
            uri, version = item
            return [int(fragment.fragment_id) for fragment in lance.dataset(uri, version=version).get_fragments()]

        results: list[list[int]] = (
            self.spark.sparkContext.parallelize([(candidate_uri, lance_version)], 1).map(inventory).collect()
        )
        if len(results) != 1:
            raise RuntimeError("fragment inventory produced an invalid executor result")
        return results[0]

    def write_rebuild_candidate(
        self,
        canonical: DataFrame,
        candidate_uri: str,
        spec: DatasetSpecRevision,
    ) -> list[int]:
        """Converge key-disjoint canonical rows into an isolated rebuild candidate.

        Args:
            canonical: One maximum-sequence terminal row per logical vector ID.
            candidate_uri: Deterministic work-derived destination.
            spec: Frozen dataset specification.

        Returns:
            Exact candidate versions observed by non-empty writer partitions.
        """
        profile: DatasetSpecRevision = spec
        telemetry_config: TelemetryConfig = self.telemetry_config
        partitioned: DataFrame = canonical.repartition(
            profile.ingest_shuffle_partitions,
            col("vector_id"),
        ).sortWithinPartitions("vector_id")

        def write_batches(batches: Iterator[pa.RecordBatch]) -> Iterator[pa.RecordBatch]:
            """Write one key-disjoint canonical partition idempotently.

            Args:
                batches: Canonical Arrow batches.

            Yields:
                Maximum exact candidate version for this non-empty partition.
            """
            telemetry: Telemetry = Telemetry.create(telemetry_config)
            candidate_schema: pa.Schema = lance.dataset(candidate_uri).schema
            maximum_version: int | None = None
            batch: pa.RecordBatch
            for batch in batches:
                if batch.num_rows == 0:
                    continue
                table: pa.Table = pa.Table.from_batches([batch])
                digest_index: int = table.schema.get_field_index(EVENT_DIGEST_COLUMN)
                table = table.set_column(
                    digest_index,
                    pa.field(EVENT_DIGEST_COLUMN, pa.binary(32)),
                    pc.cast(table[EVENT_DIGEST_COLUMN], pa.binary(32)),
                )
                if profile.include_ttl and pa.types.is_duration(candidate_schema.field("ttl").type):
                    table = apply_ttl_cast(table, "ttl")
                name: str
                dimension: int
                for name, dimension in profile.vector_fields:
                    table = apply_fsl_cast(table, name, {}, dimension)
                table = table.select(candidate_schema.names).cast(candidate_schema)
                maximum_rows: int = min(profile.merge_rows_per_chunk, profile.write_rows_per_fragment)
                merge_table: pa.Table
                for merge_table in replay_table_chunks(table, maximum_rows, profile.merge_batch_bytes):
                    result: ReplayMergeResult = replay_safe_merge(
                        candidate_uri,
                        merge_table,
                        telemetry,
                        max_rows_per_file=profile.write_rows_per_fragment,
                    )
                    maximum_version = max(maximum_version or 0, result.lance_version)
            if maximum_version is not None:
                yield pa.RecordBatch.from_pydict({"lance_version": [maximum_version]})

        return [
            int(row["lance_version"]) for row in partitioned.mapInArrow(write_batches, "lance_version long").collect()
        ]

    def candidate_pin_version(self, candidate_uri: str, pin: str) -> int | None:
        """Resolve a work-derived immutable candidate pin on one executor.

        Args:
            candidate_uri: Candidate dataset URI.
            pin: Work-derived pin name.

        Returns:
            Exact pinned version or ``None`` before the candidate or pin exists.
        """

        def resolve(item: tuple[str, str]) -> int | None:
            """Resolve one candidate pin without row-level driver I/O.

            Args:
                item: Candidate URI and pin name.

            Returns:
                Pinned version or ``None``.
            """
            uri: str
            name: str
            uri, name = item
            error: FileNotFoundError | ValueError
            try:
                return tag_version(lance.dataset(uri), name)
            except (FileNotFoundError, ValueError) as error:
                if dataset_absent(error):
                    return None
                raise

        versions: list[int | None] = (
            self.spark.sparkContext.parallelize([(candidate_uri, pin)], 1).map(resolve).collect()
        )
        if len(versions) != 1:
            raise RuntimeError("candidate pin resolution produced an invalid executor result")
        return int(versions[0]) if versions[0] is not None else None

    def pin_candidate(self, candidate_uri: str, candidate_version: int, pin: str) -> None:
        """Create or verify one immutable work-derived candidate pin on an executor.

        Args:
            candidate_uri: Candidate dataset URI.
            candidate_version: Exact qualified version.
            pin: Work-derived immutable tag name.

        Raises:
            ReplayConflict: If the pin already names another version or cannot converge after a create race.
        """

        def create(item: tuple[str, int, str]) -> int:
            """Create or verify one immutable tag.

            Args:
                item: Candidate URI, exact version, and pin name.

            Returns:
                Verified pinned version.
            """
            uri: str
            version: int
            name: str
            uri, version, name = item
            dataset: lance.LanceDataset = lance.dataset(uri, version=version)
            current: int | None = tag_version(dataset, name)
            if current is None:
                error: ValueError
                try:
                    dataset.tags.create(name, version)
                except ValueError as error:
                    current = tag_version(lance.dataset(uri), name)
                    if current is None:
                        raise error
                else:
                    current = version
            if current != version:
                raise ReplayConflict("immutable candidate pin names a different exact version")
            return int(current)

        versions: list[int] = (
            self.spark.sparkContext.parallelize([(candidate_uri, candidate_version, pin)], 1).map(create).collect()
        )
        if versions != [candidate_version]:
            raise RuntimeError("candidate pin persistence produced invalid exact-version evidence")

    def run_indexing(self, candidate_uri: str, spec: DatasetSpecRevision) -> list[dict[str, object]]:
        """Build every required index with its own typed definition.

        Args:
            candidate_uri: Candidate Lance dataset.
            spec: Frozen dataset specification.

        Returns:
            Per-definition indexing results.
        """
        return [
            LanceIndexer(self.index_config(spec, definition)).run(self.spark, [candidate_uri])[0]
            for definition in spec.index_definitions
        ]

    def index_config(self, spec: DatasetSpecRevision, definition: IndexDefinition) -> IndexJobConfig:
        """Build one explicit typed index configuration.

        Args:
            spec: Frozen dataset specification.
            definition: One required index definition.

        Returns:
            Indexer configuration selecting only the requested definition.
        """
        field_name: str = next(field.target_name for field in spec.fields if field.field_id == definition.field_id)
        config: IndexJobConfig = IndexJobConfig(
            telemetry=self.telemetry_config,
            index_name_overrides={field_name: definition.index_name},
            fragments_per_index_task=spec.fragments_per_index_task,
            max_index_deltas=spec.max_index_deltas,
            max_stale_replans=spec.max_stale_replans,
        )
        if definition.index_type is IndexType.IVF_RQ and definition.vector_options is not None:
            config.vector_columns = [field_name]
            config.num_partitions = definition.vector_options.num_partitions
            config.minimum_partitions = definition.vector_options.minimum_partitions
            config.maximum_partitions = definition.vector_options.maximum_partitions
            config.target_rows_per_partition = definition.vector_options.target_rows_per_partition
            config.vector_min_rows = definition.vector_options.minimum_rows
            config.metric = definition.vector_options.metric.value
            config.num_bits = definition.vector_options.num_bits
            config.streaming_sample_rate = definition.vector_options.streaming_sample_rate
            config.streaming_refine_passes = definition.vector_options.streaming_refine_passes
            config.retrain_growth_factor = definition.vector_options.retrain_growth_factor
        elif definition.index_type is IndexType.BTREE:
            config.scalar_columns = [field_name]
        elif definition.index_type is IndexType.BITMAP:
            config.bitmap_columns = [field_name]
        elif definition.index_type is IndexType.ZONEMAP:
            config.zonemap_columns = [field_name]
        elif definition.index_type is IndexType.INVERTED and definition.fts_options is not None:
            config.text_columns = [field_name]
            config.fts_with_position = definition.fts_options.with_position
            config.fts_base_tokenizer = definition.fts_options.base_tokenizer
            config.fts_language = definition.fts_options.language
            config.fts_max_unindexed_fragments = definition.fts_options.max_unindexed_fragments
        return config

    def candidate_version(self, candidate_uri: str) -> int:
        """Resolve one candidate head version on an executor after indexing finishes.

        Args:
            candidate_uri: Candidate Lance dataset.

        Returns:
            Exact candidate version.
        """

        def resolve(uri: str) -> int:
            """Open one candidate on an executor and return its exact version.

            Args:
                uri: Candidate URI.

            Returns:
                Current exact version.
            """
            return int(lance.dataset(uri).version)

        versions: list[int] = self.spark.sparkContext.parallelize([candidate_uri], 1).map(resolve).collect()
        if len(versions) != 1:
            raise RuntimeError("candidate version resolution produced an invalid executor result")
        return int(versions[0])

    def candidate_counts(
        self,
        candidate_uri: str,
        lance_version: int,
        spec: DatasetSpecRevision,
    ) -> tuple[int, int, int, int]:
        """Compute exact total and distinct counts across Spark executors.

        Args:
            candidate_uri: Candidate Lance dataset.
            lance_version: Exact pinned candidate version.
            spec: Frozen dataset specification.

        Returns:
            Total rows, distinct vector IDs, live rows, and distinct live vector IDs.
        """

        def inventory(item: tuple[str, int]) -> list[int]:
            """Read the exact fragment inventory on one executor.

            Args:
                item: Candidate URI and exact version.

            Returns:
                Fragment IDs.
            """
            uri: str
            version: int
            uri, version = item
            dataset: lance.LanceDataset = lance.dataset(uri, version=version)
            return [int(fragment.fragment_id) for fragment in dataset.get_fragments()]

        inventory_rows: list[list[int]] = (
            self.spark.sparkContext.parallelize([(candidate_uri, lance_version)], 1).map(inventory).collect()
        )
        if len(inventory_rows) != 1:
            raise RuntimeError("candidate count inventory produced an invalid executor result")
        fragment_ids: list[int] = inventory_rows[0]
        shards: list[tuple[int, ...]] = shard_fragments(fragment_ids, spec.ingest_shuffle_partitions)

        def terminal_ids(item: tuple[str, int, tuple[int, ...]]) -> Iterator[tuple[str, bool]]:
            """Stream vector IDs and deletion state from one exact fragment shard.

            Args:
                item: Candidate URI, version, and fragment IDs.

            Yields:
                Vector IDs and deletion state for distributed distinct counting.
            """
            uri: str
            version: int
            wanted: tuple[int, ...]
            uri, version, wanted = item
            dataset: lance.LanceDataset = lance.dataset(uri, version=version)
            fragments: list[Any] = [
                fragment for fragment in dataset.get_fragments() if fragment.fragment_id in set(wanted)
            ]
            reader: pa.RecordBatchReader = dataset.scanner(
                columns=["vector_id", DELETED_COLUMN],
                fragments=fragments,
            ).to_reader()
            batch: pa.RecordBatch
            for batch in reader:
                vector_id: object
                is_deleted: object
                for vector_id, is_deleted in zip(
                    batch.column(0).to_pylist(),
                    batch.column(1).to_pylist(),
                    strict=True,
                ):
                    yield str(vector_id), bool(is_deleted)

        tasks: list[tuple[str, int, tuple[int, ...]]] = [(candidate_uri, lance_version, shard) for shard in shards]
        terminal_rows: Any = (
            self.spark.sparkContext.parallelize(tasks, max(1, len(tasks))).flatMap(terminal_ids).cache()
        )
        try:
            total_rows: int = terminal_rows.count()
            distinct_all: int = (
                terminal_rows.map(lambda item: item[0]).distinct(numPartitions=spec.ingest_shuffle_partitions).count()
            )
            live_rows: int = terminal_rows.filter(lambda item: not item[1]).count()
            distinct_live: int = (
                terminal_rows.filter(lambda item: not item[1])
                .map(lambda item: item[0])
                .distinct(numPartitions=spec.ingest_shuffle_partitions)
                .count()
            )
        finally:
            terminal_rows.unpersist()
        return int(total_rows), int(distinct_all), int(live_rows), int(distinct_live)

    def qualify_candidate(
        self,
        candidate_uri: str,
        spec: DatasetSpecRevision,
        lance_version: int | None = None,
        counts: tuple[int, int, int, int] | None = None,
    ) -> dict[str, object]:
        """Qualify exact schema and full per-index fragment coverage on one executor.

        Args:
            candidate_uri: Candidate Lance dataset.
            spec: Frozen dataset specification.
            lance_version: Optional exact retained version for validation.
            counts: Optional distributed total, distinct-all, live, and distinct-live evidence.

        Returns:
            Bounded qualification evidence or a stable blocking classification.
        """
        requirements: tuple[tuple[str, str, str], ...] = required_indexes(spec)
        expected_schema: pa.Schema = persisted_arrow_schema(spec)
        spec_revision_id: str = str(spec.spec_revision_id)

        def qualify(
            item: tuple[
                str,
                int | None,
                tuple[int, int, int, int] | None,
                tuple[tuple[str, str, str], ...],
                pa.Schema,
            ],
        ) -> dict[str, object]:
            """Open and fully qualify one exact candidate on an executor.

            Args:
                item: Candidate URI, exact version, row evidence, required indexes, and frozen schema.

            Returns:
                Exact candidate qualification evidence.
            """
            uri: str
            version: int | None
            distributed_counts: tuple[int, int, int, int] | None
            required: tuple[tuple[str, str, str], ...]
            frozen_schema: pa.Schema
            uri, version, distributed_counts, required, frozen_schema = item
            dataset: lance.LanceDataset = lance.dataset(uri) if version is None else lance.dataset(uri, version=version)
            if not dataset.schema.equals(frozen_schema):
                return {
                    "error_code": "CANDIDATE_SCHEMA_MISMATCH",
                    "error_message": "candidate schema does not exactly match the frozen dataset specification",
                }
            descriptions: dict[str, Any] = {description.name: description for description in dataset.describe_indices()}
            outcomes: list[dict[str, object]] = []
            missing: list[str] = []
            kind: str
            column: str
            name: str
            for kind, column, name in required:
                description: Any | None = descriptions.get(name)
                if description is None:
                    missing.append(name)
                    outcomes.append(
                        {"kind": kind, "column": column, "name": name, "present": False, "fully_covered": False}
                    )
                    continue
                stats: dict[str, Any] = dataset.stats.index_stats(name)
                uncovered: int = int(stats.get("num_unindexed_fragments") or 0)
                actual_kind: str = str(description.index_type).upper()
                kind_matches: bool = index_kind_matches(kind, actual_kind)
                columns_match: bool = tuple(description.field_names) == (column,)
                generation_digest: str | None = None
                if kind == "IVF_RQ":
                    vector_config: dict[str, Any] | None = load_vector_config(dataset, column)
                    if vector_config is not None:
                        encoded: bytes = json.dumps(
                            vector_config,
                            sort_keys=True,
                            separators=(",", ":"),
                        ).encode("utf-8")
                        generation_digest = hashlib.sha256(encoded).hexdigest()
                outcomes.append(
                    {
                        "kind": kind,
                        "column": column,
                        "name": name,
                        "present": True,
                        "actual_kind": actual_kind,
                        "kind_matches": kind_matches,
                        "columns_match": columns_match,
                        "fully_covered": uncovered == 0 and kind_matches and columns_match,
                        "unindexed_fragments": uncovered,
                        "index_fragments": int(stats.get("num_indexed_fragments") or 0),
                        "artifact_generation_digest": generation_digest,
                    }
                )
                if uncovered or not kind_matches or not columns_match or (kind == "IVF_RQ" and not generation_digest):
                    missing.append(name)
            if missing:
                return {
                    "error_code": "INCOMPLETE_INDEX_COVERAGE",
                    "error_message": f"required indexes are absent or incomplete: {sorted(missing)}",
                    "indexes": outcomes,
                }
            total_rows: int = int(dataset.count_rows())
            total: int
            distinct_all: int
            live: int
            distinct_live: int
            total, distinct_all, live, distinct_live = distributed_counts or (
                total_rows,
                total_rows,
                total_rows,
                total_rows,
            )
            if total_rows != total:
                return {
                    "error_code": "CANDIDATE_COUNT_MISMATCH",
                    "error_message": "candidate exact row count differs from distributed qualification evidence",
                }
            if total != distinct_all:
                return {
                    "error_code": "DUPLICATE_VECTOR_ID",
                    "error_message": "candidate contains multiple terminal rows for one vector_id",
                }
            return {
                "lance_version": int(dataset.version),
                "fragment_count": len(dataset.get_fragments()),
                "total_rows": total,
                "distinct_vector_ids": distinct_all,
                "live_rows": live,
                "distinct_live_vector_ids": distinct_live,
                "schema_fingerprint": schema_fingerprint(dataset.schema),
                "indexes": outcomes,
                "spec_revision_id": spec_revision_id,
            }

        payload: tuple[
            str,
            int | None,
            tuple[int, int, int, int] | None,
            tuple[tuple[str, str, str], ...],
            pa.Schema,
        ] = (candidate_uri, lance_version, counts, requirements, expected_schema)
        results: list[dict[str, object]] = self.spark.sparkContext.parallelize([payload], 1).map(qualify).collect()
        if len(results) != 1:
            raise RuntimeError("candidate qualification produced an invalid executor result count")
        return results[0]

    def persist_manifest(
        self,
        context: WorkExecutionContext,
        candidate_uri: str,
        evidence: dict[str, object],
    ) -> tuple[str, bytes]:
        """Persist content-addressed exact qualification evidence on one executor.

        Args:
            context: Fenced serving work.
            candidate_uri: Exact pinned candidate dataset.
            evidence: Successful per-index qualification evidence.

        Returns:
            Immutable manifest URI and SHA-256 digest.
        """
        manifest: dict[str, object] = {
            "schema_version": 1,
            "work_id": str(context.claim.work_id),
            "dataset_id": str(context.claim.dataset_id),
            "candidate_lance_uri": candidate_uri,
            **evidence,
        }
        content: bytes = json.dumps(manifest, sort_keys=True, separators=(",", ":")).encode("utf-8")
        digest: bytes = hashlib.sha256(content).digest()
        manifest_uri: str = f"{candidate_uri}.artifacts/manifests/{digest.hex()}.json"

        def write(item: tuple[str, bytes]) -> str:
            """Write one immutable manifest if it is absent.

            Args:
                item: Manifest URI and canonical content.

            Returns:
                Manifest URI.
            """
            uri: str
            body: bytes
            uri, body = item
            filesystem: Any
            path: str
            filesystem, path = resolve_filesystem(uri, None)
            if filesystem.get_file_info(path).type.name == "NotFound":
                parent: str = path.rpartition("/")[0]
                if parent:
                    filesystem.create_dir(parent, recursive=True)
                stream: Any
                with filesystem.open_output_stream(path) as stream:
                    stream.write(body)
            else:
                stream: Any
                with filesystem.open_input_file(path) as stream:
                    existing: bytes = stream.read()
                if existing != body:
                    raise RuntimeError("content-addressed artifact manifest contains different bytes")
            return uri

        persisted: list[str] = self.spark.sparkContext.parallelize([(manifest_uri, content)], 1).map(write).collect()
        if persisted != [manifest_uri]:
            raise RuntimeError("artifact manifest persistence produced an invalid executor result")
        return manifest_uri, digest


@dataclass(frozen=True, slots=True)
class FencedWorkExecutor:
    """Concrete work dispatcher that resolves live context before any external access."""

    repository: ExecutionContextRepository
    ingest: DistributedIngestRunner
    serve: ServeWorkRunner
    settings: ReconcilerSettings

    def execute(self, claim: WorkClaim) -> WorkResult:
        """Execute one claim through its fixed INGEST or serving runner.

        Args:
            claim: Fenced durable target work.

        Returns:
            Typed worker outcome.
        """
        context: WorkExecutionContext | None = self.repository.work_execution_context(claim)
        if context is None:
            return WorkResult(claim, ResultKind.RETRY, error_code="STALE_CLAIM", error_message="claim fence expired")
        heartbeat: LeaseHeartbeat = LeaseHeartbeat(
            self.repository,
            claim,
            self.settings.lease_duration,
            self.settings.lease_heartbeat_interval,
        )
        heartbeat.start()
        try:
            result: WorkResult = self.ingest.run(context) if claim.kind is WorkKind.INGEST else self.serve.run(context)
        finally:
            heartbeat.stop()
        if heartbeat.lost:
            return retry_result(claim, "LEASE_LOST", "claim lease was lost during external execution")
        return result


@dataclass(slots=True)
class LeaseHeartbeat:
    """Renew one claim in a bounded background loop during long Spark work."""

    repository: ExecutionContextRepository
    claim: WorkClaim
    lease_duration: timedelta
    interval: timedelta
    lost: bool = False
    stop_event: threading.Event = field(default_factory=threading.Event, init=False)
    thread: threading.Thread | None = field(default=None, init=False)

    def start(self) -> None:
        """Start periodic renewal for this claim."""
        self.thread = threading.Thread(target=self.run, name=f"lease-{self.claim.work_id.hex}", daemon=True)
        self.thread.start()

    def run(self) -> None:
        """Renew until stopped, fenced, or the repository becomes unavailable."""
        while not self.stop_event.wait(self.interval.total_seconds()):
            try:
                if not self.repository.renew_lease(self.claim, self.lease_duration):
                    self.lost = True
                    return
            except Exception:
                self.lost = True
                return

    def stop(self) -> None:
        """Stop renewal and wait for the bounded thread to terminate."""
        self.stop_event.set()
        if self.thread is not None:
            self.thread.join(timeout=max(1.0, self.interval.total_seconds()))


def spec_payload_names(spec: DatasetSpecRevision) -> tuple[str, ...]:
    """Return fixed payload fields in schema order.

    Args:
        spec: Frozen dataset specification.

    Returns:
        Payload field names.
    """
    names: list[str] = [
        *(vector_field[0] for vector_field in spec.vector_fields),
        *spec.text_fields,
        *spec.metadata_fields,
    ]
    if spec.include_ttl:
        names.append("ttl")
    return tuple(names)


def terminal_column_names(spec: DatasetSpecRevision) -> tuple[str, ...]:
    """Return every persisted terminal column in exact schema order.

    Args:
        spec: Frozen dataset specification.

    Returns:
        Ordered stored column names used by canonical rebuild scans.
    """
    return tuple(field.name for field in terminal_spark_schema(spec).fields)


def required_indexes(spec: DatasetSpecRevision) -> tuple[tuple[str, str, str], ...]:
    """Return every serving index required by the immutable specification.

    Args:
        spec: Frozen dataset specification.

    Returns:
        Explicit kind, column, and stable index-name declarations.
    """
    fields_by_id: dict[uuid.UUID, str] = {field.field_id: field.target_name for field in spec.fields}
    return tuple(
        (definition.index_type.value, fields_by_id[definition.field_id], definition.index_name)
        for definition in spec.index_definitions
    )


def publication_evidence(
    spec: DatasetSpecRevision,
    qualification: dict[str, object],
) -> PublicationEvidence:
    """Convert bounded qualification output into normalized PostgreSQL evidence.

    Args:
        spec: Frozen specification whose index identities own the evidence.
        qualification: Successful exact-version qualification dictionary.

    Returns:
        Typed schema, count, fragment, and per-index evidence.
    """
    definitions: dict[str, IndexDefinition] = {
        definition.index_name: definition for definition in spec.index_definitions
    }
    index_evidence: list[PublicationIndexEvidence] = []
    raw_indexes: object | None = qualification.get("indexes")
    if not isinstance(raw_indexes, list):
        raise ValueError("qualification lacks per-index evidence")
    raw: object
    for raw in raw_indexes:
        if not isinstance(raw, dict):
            raise ValueError("qualification index evidence must be a mapping")
        definition: IndexDefinition = definitions[str(raw["name"])]
        digest_value: object | None = raw.get("artifact_generation_digest")
        index_evidence.append(
            PublicationIndexEvidence(
                index_definition_id=definition.index_definition_id,
                actual_index_type=definition.index_type,
                indexed_fragment_count=int(raw.get("index_fragments") or 0),
                unindexed_fragment_count=int(raw.get("unindexed_fragments") or 0),
                artifact_generation_digest=bytes.fromhex(str(digest_value)) if digest_value is not None else None,
            )
        )
    return PublicationEvidence(
        schema_digest=bytes.fromhex(str(qualification["schema_fingerprint"])),
        total_row_count=int(qualification["total_rows"]),
        distinct_row_count=int(qualification["distinct_vector_ids"]),
        live_row_count=int(qualification["live_rows"]),
        distinct_live_row_count=int(qualification["distinct_live_vector_ids"]),
        fragment_count=int(qualification["fragment_count"]),
        indexes=tuple(index_evidence),
    ).validate()


def index_kind_matches(required: str, actual: str) -> bool:
    """Compare a release index kind with pylance's normalized description kind.

    Args:
        required: Release declaration such as ``IVF_RQ``.
        actual: Pylance index description kind.

    Returns:
        Whether the exact required index family is present.
    """
    accepted: dict[str, frozenset[str]] = {
        "IVF_RQ": frozenset({"IVF", "IVF_RQ"}),
        "INVERTED": frozenset({"INVERTED"}),
        "BTREE": frozenset({"BTREE"}),
        "BITMAP": frozenset({"BITMAP"}),
        "ZONEMAP": frozenset({"ZONEMAP"}),
    }
    return actual in accepted.get(required, frozenset())


def shard_fragments(fragment_ids: list[int], maximum_shards: int) -> list[tuple[int, ...]]:
    """Split exact fragment IDs into bounded non-empty executor shards.

    Args:
        fragment_ids: Candidate fragment IDs.
        maximum_shards: Release-owned maximum task count.

    Returns:
        Balanced fragment tuples, or one empty tuple for an empty dataset.
    """
    if not fragment_ids:
        return [()]
    shard_count: int = min(maximum_shards, len(fragment_ids))
    return [tuple(fragment_ids[index::shard_count]) for index in range(shard_count)]


def terminal_arrow_schema(spec: DatasetSpecRevision) -> pa.Schema:
    """Build the executor Arrow schema for normalized terminal mutations.

    Args:
        spec: Frozen dataset specification.

    Returns:
        Arrow schema before fixed-size vector cast.
    """
    fields: list[pa.Field] = [
        pa.field("vector_id", pa.string(), nullable=False),
        pa.field("event_timestamp", pa.timestamp("us", "UTC")),
    ]
    fields.extend(pa.field(vector_field[0], pa.list_(pa.float32())) for vector_field in spec.vector_fields)
    fields.extend(pa.field(name, pa.string()) for name in spec.text_fields)
    fields.extend(pa.field(name, pa.string()) for name in spec.metadata_fields)
    if spec.include_ttl:
        fields.append(pa.field("ttl", pa.int64()))
    fields.extend(
        (
            pa.field(WINDOW_SEQUENCE_COLUMN, pa.int64(), nullable=False),
            pa.field(SOURCE_SEQUENCE_COLUMN, pa.int64(), nullable=False),
            pa.field(EVENT_DIGEST_COLUMN, pa.binary(), nullable=False),
            pa.field(DELETED_COLUMN, pa.bool_(), nullable=False),
        )
    )
    return pa.schema(fields)


def persisted_arrow_schema(spec: DatasetSpecRevision) -> pa.Schema:
    """Build the exact frozen schema stored in Lance and qualified for publication.

    Args:
        spec: Frozen dataset specification.

    Returns:
        Exact field names, order, physical types, and nullability for storage.
    """
    fields: list[pa.Field] = [
        pa.field("vector_id", pa.string(), nullable=False),
        pa.field("event_timestamp", pa.timestamp("us", "UTC")),
    ]
    fields.extend(pa.field(name, pa.list_(pa.float32(), dimension)) for name, dimension in spec.vector_fields)
    fields.extend(pa.field(name, pa.string()) for name in spec.text_fields)
    fields.extend(pa.field(name, pa.string()) for name in spec.metadata_fields)
    if spec.include_ttl:
        fields.append(pa.field("ttl", pa.duration("s")))
    fields.extend(
        (
            pa.field(WINDOW_SEQUENCE_COLUMN, pa.int64(), nullable=False),
            pa.field(SOURCE_SEQUENCE_COLUMN, pa.int64(), nullable=False),
            pa.field(EVENT_DIGEST_COLUMN, pa.binary(32), nullable=False),
            pa.field(DELETED_COLUMN, pa.bool_(), nullable=False),
        )
    )
    return pa.schema(fields)


def terminal_spark_schema(spec: DatasetSpecRevision) -> StructType:
    """Build the matching Spark schema for terminal Arrow execution.

    Args:
        spec: Frozen dataset specification.

    Returns:
        Spark schema matching ``terminal_arrow_schema``.
    """
    fields: list[StructField] = [
        StructField("vector_id", StringType(), nullable=False),
        StructField("event_timestamp", TimestampType()),
    ]
    fields.extend(StructField(vector_field[0], ArrayType(FloatType())) for vector_field in spec.vector_fields)
    fields.extend(StructField(name, StringType()) for name in spec.text_fields)
    fields.extend(StructField(name, StringType()) for name in spec.metadata_fields)
    if spec.include_ttl:
        fields.append(StructField("ttl", LongType()))
    fields.extend(
        (
            StructField(WINDOW_SEQUENCE_COLUMN, LongType(), nullable=False),
            StructField(SOURCE_SEQUENCE_COLUMN, LongType(), nullable=False),
            StructField(EVENT_DIGEST_COLUMN, BinaryType(), nullable=False),
            StructField(DELETED_COLUMN, BooleanType(), nullable=False),
        )
    )
    return StructType(fields)


def blocked_result(claim: WorkClaim, error_code: str, message: str) -> WorkResult:
    """Build one contract-block result.

    Args:
        claim: Fenced work identity.
        error_code: Stable bounded classification.
        message: Operator diagnostic.

    Returns:
        Typed blocked result.
    """
    return WorkResult(claim, ResultKind.BLOCKED, error_code=error_code, error_message=message)


def retry_result(claim: WorkClaim, error_code: str, message: str) -> WorkResult:
    """Build one transient retry result.

    Args:
        claim: Fenced work identity.
        error_code: Stable bounded classification.
        message: Operator diagnostic.

    Returns:
        Typed retry result.
    """
    return WorkResult(claim, ResultKind.RETRY, error_code=error_code, error_message=message)
