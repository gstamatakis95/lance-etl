"""Concrete fenced Spark execution for exact source ingestion and target serving phases."""

from __future__ import annotations

import hashlib
import json
import struct
import threading
from collections.abc import Iterator
from dataclasses import dataclass, field
from datetime import UTC, timedelta
from typing import Protocol

import lance
import pyarrow as pa
import pyarrow.compute as pc
from pyspark import StorageLevel
from pyspark.errors import AnalysisException
from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql.functions import array, array_except, col, countDistinct, element_at, lit, lower, map_keys, size, trim
from pyspark.sql.types import (
    ArrayType,
    BinaryType,
    BooleanType,
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
    replay_safe_merge,
)
from lance_etl.indexing import (
    IndexJobConfig,
    LanceIndexer,
    bitmap_index_name,
    fts_index_name,
    load_vector_config,
    scalar_index_name,
    vector_index_name,
    zonemap_index_name,
)
from lance_etl.maintenance import MaintenanceConfig, MaintenanceJob
from lance_etl.publication.manifest import schema_fingerprint
from lance_etl.reconciler.config import DeploymentProfile
from lance_etl.reconciler.results import ResultKind, WorkResult
from lance_etl.source import SnapshotRecord, TargetKey, WindowKind, build_spark_scan, execute_spark_scan
from lance_etl.state import WorkClaim, WorkExecutionContext, WorkKind, WorkPhase
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
    """Run one fixed-profile SERVE or REBUILD phase."""

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
    """Execute one exact target snapshot with bounded Spark and replay-safe Lance writes."""

    spark: SparkSession
    source_table: str
    profile: DeploymentProfile
    telemetry_config: TelemetryConfig

    def run(self, context: WorkExecutionContext) -> WorkResult:
        """Normalize, conflict-check, digest, write, verify, and mark one source target.

        Args:
            context: Live INGEST context with exact source snapshot metadata.

        Returns:
            Exact completion evidence or a contract-block result.
        """
        if context.claim.kind is not WorkKind.INGEST or context.claim.source_window_seq is None:
            raise ValueError("distributed ingest runner requires an INGEST claim")
        if context.snapshot_id is None or context.iceberg_sequence_number is None or context.source_window_kind is None:
            raise ValueError("INGEST context lacks exact source snapshot metadata")
        snapshot = SnapshotRecord(
            table_uuid="runtime",
            snapshot_id=context.snapshot_id,
            parent_snapshot_id=context.parent_snapshot_id,
            sequence_number=context.iceberg_sequence_number,
            committed_at_ms=0,
            operation="append",
            partition_spec_id=0,
        )
        scan = build_spark_scan(
            self.source_table,
            snapshot,
            WindowKind(context.source_window_kind.value),
            TargetKey(context.identity.tenant_id, context.identity.namespace, context.identity.org_id),
        )
        try:
            source = execute_spark_scan(self.spark, scan)
            self.validate_source_profile(source)
            selected = self.select_profile_fields(source)
            terminal = self.normalize_terminal(selected, context).persist(StorageLevel.MEMORY_AND_DISK)
            try:
                if terminal.where(col("vector_id").isNull()).limit(1).count():
                    return blocked_result(context.claim, "NULL_VECTOR_ID", "source contains a null vector_id")
                conflict = (
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
                collapsed = terminal.dropDuplicates(["vector_id", EVENT_DIGEST_COLUMN]).dropDuplicates(["vector_id"])
                source_digest, source_rows = self.compute_source_digest(collapsed)
                versions = self.write_terminal(collapsed, context)
                if source_rows > 0 and not versions:
                    raise RuntimeError("terminal write produced no verified executor result")
                marker = self.finalize_marker(context, source_digest)
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

    def validate_source_profile(self, source: DataFrame) -> None:
        """Reject unknown map keys and wrong vector dimensions before the first write.

        Args:
            source: Exact target snapshot scan.

        Raises:
            ValueError: If source fields violate the release-owned target profile.
        """
        self.validate_source_schema(source)
        if source.where(col("event_timestamp").isNull()).limit(1).count():
            raise ValueError("source contains a null event_timestamp")
        normalized_operation = lower(trim(col("op")))
        supported = tuple(sorted(UPSERT_OPERATIONS | DELETE_OPERATIONS))
        if source.where(col("op").isNull() | ~normalized_operation.isin(*supported)).limit(1).count():
            raise ValueError("source contains an unsupported mutation operation")
        self.validate_map_keys(source)
        self.validate_vector_dimensions(source, normalized_operation)

    def validate_source_schema(self, source: DataFrame) -> None:
        """Validate the fixed physical source types before distributed checks.

        Args:
            source: Exact target snapshot scan.

        Raises:
            ValueError: If a required column is absent or carries the wrong Spark type.
        """
        required_columns = {
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
            required_columns.add("ttl")
        missing_columns = sorted(required_columns - set(source.columns))
        if missing_columns:
            raise ValueError(f"source is missing required columns {missing_columns}")
        string_columns = ("tenant_id", "namespace", "org_id", "vector_id", "op")
        if any(not isinstance(source.schema[name].dataType, StringType) for name in string_columns):
            raise ValueError("source routing, vector_id, and op columns must be strings")
        if not isinstance(source.schema["event_timestamp"].dataType, TimestampType):
            raise ValueError("source event_timestamp must be a timestamp")
        vectors_type = source.schema["vectors"].dataType
        texts_type = source.schema["texts"].dataType
        metadata_type = source.schema["metadata"].dataType
        vectors_valid = (
            isinstance(vectors_type, MapType)
            and isinstance(vectors_type.keyType, StringType)
            and isinstance(vectors_type.valueType, ArrayType)
            and isinstance(vectors_type.valueType.elementType, FloatType)
        )
        strings_valid = all(
            isinstance(map_type, MapType)
            and isinstance(map_type.keyType, StringType)
            and isinstance(map_type.valueType, StringType)
            for map_type in (texts_type, metadata_type)
        )
        if not vectors_valid or not strings_valid:
            raise ValueError("source maps must match vectors<string,array<float>> and text metadata string maps")
        if self.profile.include_ttl and not isinstance(source.schema["ttl"].dataType, LongType):
            raise ValueError("source ttl must be bigint seconds")

    def validate_map_keys(self, source: DataFrame) -> None:
        """Reject dynamic map keys outside the release-owned profile.

        Args:
            source: Exact target snapshot scan.

        Raises:
            ValueError: If a map contains a field outside the target schema profile.
        """
        map_contracts = (
            ("vectors", tuple(name for name, dimension in self.profile.vector_fields)),
            ("texts", self.profile.text_fields),
            ("metadata", self.profile.metadata_fields),
        )
        for map_column, allowed in map_contracts:
            keys = map_keys(col(map_column))
            unknown_count = (
                size(keys) if not allowed else size(array_except(keys, array(*(lit(name) for name in allowed))))
            )
            if source.where(unknown_count > 0).limit(1).count():
                raise ValueError(f"source map {map_column!r} contains fields outside profile {self.profile.profile_id}")

    def validate_vector_dimensions(self, source: DataFrame, normalized_operation: Column) -> None:
        """Validate required vector presence and fixed dimensions.

        Args:
            source: Exact target snapshot scan.
            normalized_operation: Normalized Spark operation expression.

        Raises:
            ValueError: If an upsert omits a vector or any vector has the wrong dimension.
        """
        delete_operation = normalized_operation.isin(*tuple(sorted(DELETE_OPERATIONS)))
        for name, dimension in self.profile.vector_fields:
            vector = element_at(col("vectors"), lit(name))
            if source.where(~delete_operation & vector.isNull()).limit(1).count():
                raise ValueError(f"upsert is missing required vector field {name!r}")
            if source.where(vector.isNotNull() & (size(vector) != dimension)).limit(1).count():
                raise ValueError(f"vector field {name!r} does not match fixed dimension {dimension}")

    def select_profile_fields(self, source: DataFrame) -> DataFrame:
        """Project maps into a complete fixed post-image schema.

        Args:
            source: Validated exact target scan.

        Returns:
            Projected rows containing every allowed field.
        """
        fields = [
            col("vector_id"),
            col("op"),
            col("event_timestamp"),
            *(element_at(col("vectors"), lit(name)).alias(name) for name, dimension in self.profile.vector_fields),
            *(element_at(col("texts"), lit(name)).alias(name) for name in self.profile.text_fields),
            *(element_at(col("metadata"), lit(name)).alias(name) for name in self.profile.metadata_fields),
        ]
        if self.profile.include_ttl:
            fields.append(col("ttl"))
        return source.select(*fields)

    def normalize_terminal(self, source: DataFrame, context: WorkExecutionContext) -> DataFrame:
        """Attach canonical event identity and tombstone fields in executor Arrow batches.

        Args:
            source: Fixed-profile source projection.
            context: Exact source ordering and work identity.

        Returns:
            Terminal mutation lineage before duplicate collapse.
        """
        schema = terminal_spark_schema(self.profile)
        target = (context.identity.tenant_id, context.identity.namespace, context.identity.org_id)
        window_seq = int(context.claim.source_window_seq or 0)
        source_sequence = int(context.iceberg_sequence_number or 0)
        profile = self.profile

        def normalize_batches(batches: Iterator[pa.RecordBatch]) -> Iterator[pa.RecordBatch]:
            """Normalize bounded source batches without collecting a target on the driver.

            Args:
                batches: Spark Arrow batches.

            Yields:
                Canonical terminal mutation batches.
            """
            arrow_schema = terminal_arrow_schema(profile)
            payload_names = profile_payload_names(profile)
            for batch in batches:
                records: list[dict[str, object]] = []
                for row in pa.Table.from_batches([batch]).to_pylist():
                    operation = normalize_operation(str(row["op"]))
                    event_timestamp = row["event_timestamp"]
                    if event_timestamp.tzinfo is None:
                        event_timestamp = event_timestamp.replace(tzinfo=UTC)
                    payload = {name: row.get(name) for name in payload_names}
                    digest = canonical_event_digest(target, str(row["vector_id"]), operation, event_timestamp, payload)
                    deleted = operation == "delete"
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
                table = pa.Table.from_pylist(records, schema=arrow_schema)
                yield from table.to_batches()

        return source.mapInArrow(normalize_batches, schema=schema)

    def compute_source_digest(self, terminal: DataFrame) -> tuple[bytes, int]:
        """Compute the frozen digest in one streaming sorted executor reduction.

        Args:
            terminal: Duplicate-collapsed target mutations.

        Returns:
            Raw digest and terminal row count.
        """
        ordered = (
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
            digest = hashlib.sha256()
            digest.update(SOURCE_DIGEST_HEADER)
            count = 0
            for batch in batches:
                for row in pa.Table.from_batches([batch]).to_pylist():
                    digest.update(encode_bytes(str(row["vector_id"]).encode("utf-8")))
                    digest.update(struct.pack(">q", int(row[SOURCE_SEQUENCE_COLUMN])))
                    digest.update(bytes(row[EVENT_DIGEST_COLUMN]))
                    count += 1
            yield pa.RecordBatch.from_pydict({"source_digest": [digest.digest()], "source_rows": [count]})

        result = ordered.mapInArrow(digest_batches, "source_digest binary, source_rows long").collect()
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
        profile = self.profile
        telemetry_config = self.telemetry_config
        uri = context.claim.expected_ingest_lance_uri
        partitioned = terminal.repartition(profile.ingest_shuffle_partitions, col("vector_id")).sortWithinPartitions(
            "vector_id"
        )

        def write_batches(batches: Iterator[pa.RecordBatch]) -> Iterator[pa.RecordBatch]:
            """Write bounded Arrow batches with unique keys per Spark partition.

            Args:
                batches: Key-disjoint terminal batches.

            Yields:
                One exact verified maximum version per non-empty partition.
            """
            telemetry = Telemetry.create(telemetry_config)
            maximum_version: int | None = None
            for batch in batches:
                if batch.num_rows == 0:
                    continue
                table = pa.Table.from_batches([batch])
                digest_index = table.schema.get_field_index(EVENT_DIGEST_COLUMN)
                table = table.set_column(
                    digest_index,
                    pa.field(EVENT_DIGEST_COLUMN, pa.binary(32)),
                    pc.cast(table[EVENT_DIGEST_COLUMN], pa.binary(32)),
                )
                if profile.include_ttl:
                    table = apply_ttl_cast(table, "ttl")
                for name, dimension in profile.vector_fields:
                    table = apply_fsl_cast(table, name, {}, dimension)
                    invalid = pc.and_(pc.invert(table[DELETED_COLUMN]), pc.is_null(table[name]))
                    if table[name].null_count and pc.any(invalid).as_py():
                        raise ValueError(f"vector field {name!r} became null during fixed-dimension cast")
                result = replay_safe_merge(uri, table, telemetry)
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
        telemetry_config = self.telemetry_config
        uri = context.claim.expected_ingest_lance_uri
        window_seq = int(context.claim.source_window_seq or 0)

        def finalize(item: tuple[str, int, bytes]) -> CompletionMarker:
            """Finalize one marker in its dedicated executor task.

            Args:
                item: URI, window sequence, and digest.

            Returns:
                Durable completion marker.
            """
            target_uri, target_window, target_digest = item
            return finalize_completion_marker(
                target_uri,
                target_window,
                target_digest,
                Telemetry.create(telemetry_config),
            )

        return self.spark.sparkContext.parallelize([(uri, window_seq, source_digest)], 1).map(finalize).collect()[0]


@dataclass(frozen=True, slots=True)
class ProfiledServeRunner:
    """Maintain, index, qualify, and prewarm one candidate with a fixed release profile."""

    spark: SparkSession
    profile: DeploymentProfile
    telemetry_config: TelemetryConfig

    def run(self, context: WorkExecutionContext) -> WorkResult:
        """Produce immutable qualification evidence for one serving generation.

        Args:
            context: Live fenced SERVE or REBUILD context.

        Returns:
            Publication evidence only after every required index has full coverage.
        """
        if context.claim.kind not in (WorkKind.SERVE, WorkKind.REBUILD):
            raise ValueError("profiled serve runner requires SERVE or REBUILD work")
        if context.profile_id != self.profile.profile_id:
            return blocked_result(
                context.claim,
                "UNKNOWN_DEPLOYMENT_PROFILE",
                f"target profile {context.profile_id!r} does not match release profile {self.profile.profile_id!r}",
            )
        if context.claim.phase is WorkPhase.PREWARM and context.candidate_lance_uri is not None:
            return self.run_retained_prewarm(context)
        candidate_uri = context.claim.expected_ingest_lance_uri
        maintenance = MaintenanceJob(
            MaintenanceConfig(
                telemetry=self.telemetry_config,
                ttl_column="ttl" if self.profile.include_ttl else None,
            )
        ).run(self.spark, [candidate_uri])[0]
        if maintenance.get("error"):
            return retry_result(context.claim, "MAINTENANCE_FAILED", str(maintenance["error"]))
        indexing = LanceIndexer(self.index_config()).run(self.spark, [candidate_uri])[0]
        index_errors = [item for item in indexing.get("indexes", []) if item.get("error")]
        if indexing.get("error") or index_errors:
            message = str(indexing.get("error") or index_errors[0].get("error"))
            return retry_result(context.claim, "INDEX_BUILD_FAILED", message)
        candidate_version = self.candidate_version(candidate_uri)
        counts = self.candidate_counts(candidate_uri, candidate_version)
        evidence = self.qualify_candidate(candidate_uri, candidate_version, counts)
        if evidence.get("error_code"):
            return blocked_result(context.claim, str(evidence["error_code"]), str(evidence["error_message"]))
        manifest_uri, manifest_digest = self.persist_manifest(context, evidence)
        return WorkResult(
            claim=context.claim,
            kind=ResultKind.SERVE_SUCCEEDED,
            candidate_lance_uri=candidate_uri,
            indexed_lance_version=int(evidence["lance_version"]),
            artifact_manifest_uri=manifest_uri,
            artifact_digest=manifest_digest,
        )

    def run_retained_prewarm(self, context: WorkExecutionContext) -> WorkResult:
        """Revalidate and prewarm a retained immutable rollback publication.

        Args:
            context: PREWARM context carrying retained exact evidence.

        Returns:
            Publication result reusing the retained immutable manifest.
        """
        if (
            context.candidate_lance_uri is None
            or context.indexed_lance_version is None
            or context.artifact_manifest_uri is None
            or context.artifact_digest is None
        ):
            return blocked_result(context.claim, "ROLLBACK_EVIDENCE_MISSING", "rollback context lacks exact evidence")
        counts = self.candidate_counts(context.candidate_lance_uri, context.indexed_lance_version)
        evidence = self.qualify_candidate(context.candidate_lance_uri, context.indexed_lance_version, counts)
        if evidence.get("error_code"):
            return blocked_result(context.claim, str(evidence["error_code"]), str(evidence["error_message"]))
        return WorkResult(
            claim=context.claim,
            kind=ResultKind.SERVE_SUCCEEDED,
            candidate_lance_uri=context.candidate_lance_uri,
            indexed_lance_version=context.indexed_lance_version,
            artifact_manifest_uri=context.artifact_manifest_uri,
            artifact_digest=context.artifact_digest,
        )

    def index_config(self) -> IndexJobConfig:
        """Build explicit index configuration without dataset role discovery.

        Returns:
            Fixed required index configuration.
        """
        return IndexJobConfig(
            telemetry=self.telemetry_config,
            vector_columns=[name for name, dimension in self.profile.vector_fields],
            vector_min_rows=1,
            metric=self.profile.vector_metric,
            scalar_columns=list(self.profile.scalar_index_fields),
            bitmap_columns=list(self.profile.bitmap_index_fields),
            zonemap_columns=list(self.profile.zonemap_index_fields),
            text_columns=list(self.profile.text_fields),
        )

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

        versions = self.spark.sparkContext.parallelize([candidate_uri], 1).map(resolve).collect()
        if len(versions) != 1:
            raise RuntimeError("candidate version resolution produced an invalid executor result")
        return int(versions[0])

    def candidate_counts(self, candidate_uri: str, lance_version: int) -> tuple[int, int, int]:
        """Compute exact total, live, and distinct-live counts across Spark executors.

        Args:
            candidate_uri: Candidate Lance dataset.
            lance_version: Exact pinned candidate version.

        Returns:
            Total rows, live rows, and distinct live vector IDs.
        """

        def inventory(item: tuple[str, int]) -> tuple[int, int, list[int]]:
            """Read exact counts and fragment inventory on one executor.

            Args:
                item: Candidate URI and exact version.

            Returns:
                Total rows, live rows, and fragment IDs.
            """
            uri, version = item
            dataset = lance.dataset(uri, version=version)
            live_filter = pc.equal(pc.field(DELETED_COLUMN), pa.scalar(False))
            return (
                int(dataset.count_rows()),
                int(dataset.count_rows(filter=live_filter)),
                [int(fragment.fragment_id) for fragment in dataset.get_fragments()],
            )

        inventory_rows = (
            self.spark.sparkContext.parallelize([(candidate_uri, lance_version)], 1).map(inventory).collect()
        )
        if len(inventory_rows) != 1:
            raise RuntimeError("candidate count inventory produced an invalid executor result")
        total_rows, live_rows, fragment_ids = inventory_rows[0]
        shards = shard_fragments(fragment_ids, self.profile.ingest_shuffle_partitions)

        def live_ids(item: tuple[str, int, tuple[int, ...]]) -> Iterator[str]:
            """Stream live vector IDs from one exact fragment shard.

            Args:
                item: Candidate URI, version, and fragment IDs.

            Yields:
                Live vector IDs for distributed distinct counting.
            """
            uri, version, wanted = item
            dataset = lance.dataset(uri, version=version)
            fragments = [fragment for fragment in dataset.get_fragments() if fragment.fragment_id in set(wanted)]
            live_filter = pc.equal(pc.field(DELETED_COLUMN), pa.scalar(False))
            reader = dataset.scanner(columns=["vector_id"], filter=live_filter, fragments=fragments).to_reader()
            for batch in reader:
                yield from (str(value) for value in batch.column(0).to_pylist())

        tasks = [(candidate_uri, lance_version, shard) for shard in shards]
        distinct_live = (
            self.spark.sparkContext.parallelize(tasks, max(1, len(tasks)))
            .flatMap(live_ids)
            .distinct(numPartitions=self.profile.ingest_shuffle_partitions)
            .count()
        )
        return int(total_rows), int(live_rows), int(distinct_live)

    def qualify_candidate(
        self,
        candidate_uri: str,
        lance_version: int | None = None,
        counts: tuple[int, int, int] | None = None,
    ) -> dict[str, object]:
        """Qualify exact schema and full per-index fragment coverage on one executor.

        Args:
            candidate_uri: Candidate Lance dataset.
            lance_version: Optional exact retained version for rollback.
            counts: Optional distributed total, live, and distinct-live evidence.

        Returns:
            Bounded qualification evidence or a stable blocking classification.
        """
        requirements = required_indexes(self.profile)
        expected_columns = set(profile_payload_names(self.profile)) | {
            "vector_id",
            "event_timestamp",
            WINDOW_SEQUENCE_COLUMN,
            SOURCE_SEQUENCE_COLUMN,
            EVENT_DIGEST_COLUMN,
            DELETED_COLUMN,
        }
        profile_id = self.profile.profile_id

        def qualify(
            item: tuple[
                str,
                int | None,
                tuple[int, int, int] | None,
                tuple[tuple[str, str, str], ...],
                tuple[str, ...],
            ],
        ) -> dict[str, object]:
            """Open and fully qualify one exact candidate on an executor.

            Args:
                item: Candidate URI, required index declarations, and expected columns.

            Returns:
                Exact candidate qualification evidence.
            """
            uri, version, distributed_counts, required, columns = item
            dataset = lance.dataset(uri) if version is None else lance.dataset(uri, version=version)
            missing_columns = sorted(set(columns) - set(dataset.schema.names))
            if missing_columns:
                return {
                    "error_code": "CANDIDATE_SCHEMA_MISMATCH",
                    "error_message": f"candidate lacks required columns {missing_columns}",
                }
            descriptions = {description.name: description for description in dataset.describe_indices()}
            outcomes: list[dict[str, object]] = []
            missing: list[str] = []
            for kind, column, name in required:
                description = descriptions.get(name)
                if description is None:
                    missing.append(name)
                    outcomes.append(
                        {"kind": kind, "column": column, "name": name, "present": False, "fully_covered": False}
                    )
                    continue
                stats = dataset.stats.index_stats(name)
                uncovered = int(stats.get("num_unindexed_fragments") or 0)
                actual_kind = str(description.index_type).upper()
                kind_matches = index_kind_matches(kind, actual_kind)
                columns_match = tuple(description.field_names) == (column,)
                generation_digest: str | None = None
                if kind == "IVF_RQ":
                    vector_config = load_vector_config(dataset, column)
                    if vector_config is not None:
                        encoded = json.dumps(vector_config, sort_keys=True, separators=(",", ":")).encode("utf-8")
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
            total_rows = int(dataset.count_rows())
            total, live, distinct_live = distributed_counts or (total_rows, total_rows, total_rows)
            if total_rows != total:
                return {
                    "error_code": "CANDIDATE_COUNT_MISMATCH",
                    "error_message": "candidate exact row count differs from distributed qualification evidence",
                }
            if live != distinct_live:
                return {
                    "error_code": "DUPLICATE_LIVE_VECTOR_ID",
                    "error_message": "candidate contains duplicate live vector_id values",
                }
            return {
                "lance_version": int(dataset.version),
                "fragment_count": len(dataset.get_fragments()),
                "total_rows": total,
                "live_rows": live,
                "distinct_live_vector_ids": distinct_live,
                "schema_fingerprint": schema_fingerprint(dataset.schema),
                "indexes": outcomes,
                "profile_id": profile_id,
            }

        payload = (candidate_uri, lance_version, counts, requirements, tuple(sorted(expected_columns)))
        results = self.spark.sparkContext.parallelize([payload], 1).map(qualify).collect()
        if len(results) != 1:
            raise RuntimeError("candidate qualification produced an invalid executor result count")
        return results[0]

    def persist_manifest(
        self,
        context: WorkExecutionContext,
        evidence: dict[str, object],
    ) -> tuple[str, bytes]:
        """Persist content-addressed exact qualification evidence on one executor.

        Args:
            context: Fenced serving work.
            evidence: Successful per-index qualification evidence.

        Returns:
            Immutable manifest URI and SHA-256 digest.
        """
        manifest = {
            "schema_version": 1,
            "work_id": str(context.claim.work_id),
            "target_id": str(context.claim.target_id),
            "candidate_lance_uri": context.claim.expected_ingest_lance_uri,
            **evidence,
        }
        content = json.dumps(manifest, sort_keys=True, separators=(",", ":")).encode("utf-8")
        digest = hashlib.sha256(content).digest()
        manifest_uri = f"{context.claim.expected_ingest_lance_uri}.artifacts/manifests/{digest.hex()}.json"

        def write(item: tuple[str, bytes]) -> str:
            """Write one immutable manifest if it is absent.

            Args:
                item: Manifest URI and canonical content.

            Returns:
                Manifest URI.
            """
            uri, body = item
            filesystem, path = resolve_filesystem(uri, None)
            if filesystem.get_file_info(path).type.name == "NotFound":
                with filesystem.open_output_stream(path) as stream:
                    stream.write(body)
            else:
                with filesystem.open_input_file(path) as stream:
                    existing = stream.read()
                if existing != body:
                    raise RuntimeError("content-addressed artifact manifest contains different bytes")
            return uri

        persisted = self.spark.sparkContext.parallelize([(manifest_uri, content)], 1).map(write).collect()
        if persisted != [manifest_uri]:
            raise RuntimeError("artifact manifest persistence produced an invalid executor result")
        return manifest_uri, digest


@dataclass(frozen=True, slots=True)
class FencedWorkExecutor:
    """Concrete work dispatcher that resolves live context before any external access."""

    repository: ExecutionContextRepository
    ingest: DistributedIngestRunner
    serve: ServeWorkRunner
    profile: DeploymentProfile

    def execute(self, claim: WorkClaim) -> WorkResult:
        """Execute one claim through its fixed INGEST or serving runner.

        Args:
            claim: Fenced durable target work.

        Returns:
            Typed worker outcome.
        """
        context = self.repository.work_execution_context(claim)
        if context is None:
            return WorkResult(claim, ResultKind.RETRY, error_code="STALE_CLAIM", error_message="claim fence expired")
        heartbeat = LeaseHeartbeat(
            self.repository,
            claim,
            self.profile.lease_duration,
            self.profile.lease_heartbeat_interval,
        )
        heartbeat.start()
        try:
            result = self.ingest.run(context) if claim.kind is WorkKind.INGEST else self.serve.run(context)
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
    stop_event: threading.Event = field(init=False)
    thread: threading.Thread = field(init=False)

    def __post_init__(self) -> None:
        """Create process-local synchronization objects after dataclass initialization."""
        self.stop_event = threading.Event()
        self.thread = threading.Thread(target=self.run, name=f"lease-{self.claim.work_id.hex}", daemon=True)

    def start(self) -> None:
        """Start periodic renewal for this claim."""
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
        self.thread.join(timeout=max(1.0, self.interval.total_seconds()))


def profile_payload_names(profile: DeploymentProfile) -> tuple[str, ...]:
    """Return fixed payload fields in schema order.

    Args:
        profile: Release-owned target profile.

    Returns:
        Payload field names.
    """
    names = [*(name for name, dimension in profile.vector_fields), *profile.text_fields, *profile.metadata_fields]
    if profile.include_ttl:
        names.append("ttl")
    return tuple(names)


def required_indexes(profile: DeploymentProfile) -> tuple[tuple[str, str, str], ...]:
    """Return every serving index required by the immutable release profile.

    Args:
        profile: Release-owned target profile.

    Returns:
        Explicit kind, column, and stable index-name declarations.
    """
    declarations: list[tuple[str, str, str]] = []
    declarations.extend(("IVF_RQ", name, vector_index_name(name)) for name, dimension in profile.vector_fields)
    declarations.extend(("BTREE", name, scalar_index_name(name)) for name in profile.scalar_index_fields)
    declarations.extend(("BITMAP", name, bitmap_index_name(name)) for name in profile.bitmap_index_fields)
    declarations.extend(("ZONEMAP", name, zonemap_index_name(name)) for name in profile.zonemap_index_fields)
    declarations.extend(("INVERTED", name, fts_index_name(name)) for name in profile.text_fields)
    return tuple(declarations)


def index_kind_matches(required: str, actual: str) -> bool:
    """Compare a release index kind with pylance's normalized description kind.

    Args:
        required: Release declaration such as ``IVF_RQ``.
        actual: Pylance index description kind.

    Returns:
        Whether the exact required index family is present.
    """
    accepted = {
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
    shard_count = min(maximum_shards, len(fragment_ids))
    return [tuple(fragment_ids[index::shard_count]) for index in range(shard_count)]


def terminal_arrow_schema(profile: DeploymentProfile) -> pa.Schema:
    """Build the executor Arrow schema for normalized terminal mutations.

    Args:
        profile: Release-owned target profile.

    Returns:
        Arrow schema before fixed-size vector cast.
    """
    fields: list[pa.Field] = [
        pa.field("vector_id", pa.string()),
        pa.field("event_timestamp", pa.timestamp("us", "UTC")),
    ]
    fields.extend(pa.field(name, pa.list_(pa.float32())) for name, dimension in profile.vector_fields)
    fields.extend(pa.field(name, pa.string()) for name in profile.text_fields)
    fields.extend(pa.field(name, pa.string()) for name in profile.metadata_fields)
    if profile.include_ttl:
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


def terminal_spark_schema(profile: DeploymentProfile) -> StructType:
    """Build the matching Spark schema for terminal Arrow execution.

    Args:
        profile: Release-owned target profile.

    Returns:
        Spark schema matching ``terminal_arrow_schema``.
    """
    fields = [StructField("vector_id", StringType()), StructField("event_timestamp", TimestampType())]
    fields.extend(StructField(name, ArrayType(FloatType())) for name, dimension in profile.vector_fields)
    fields.extend(StructField(name, StringType()) for name in profile.text_fields)
    fields.extend(StructField(name, StringType()) for name in profile.metadata_fields)
    if profile.include_ttl:
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
