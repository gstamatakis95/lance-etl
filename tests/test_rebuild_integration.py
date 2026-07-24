"""Real local Spark and Lance tests for canonical dataset rebuilds."""

from __future__ import annotations

import hashlib
import os
import sys
import uuid
from collections.abc import Iterator
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from unittest.mock import MagicMock

import lance
import pyarrow as pa
import pytest
from pyspark.sql import DataFrame, SparkSession

import lance_etl.reconciler.workers as reconciler_workers
from lance_etl.etl.digest import canonical_source_digest
from lance_etl.etl.replay_sink import DELETED_COLUMN, EVENT_DIGEST_COLUMN, SOURCE_SEQUENCE_COLUMN
from lance_etl.reconciler.iceberg import BaselineQualifier
from lance_etl.reconciler.results import ResultKind, WorkResult
from lance_etl.reconciler.workers import (
    ConfiguredPublicationRunner,
    DistributedIngestRunner,
    source_digest_chunks,
)
from lance_etl.state import (
    DatasetField,
    DatasetSpecRevision,
    FieldRole,
    IcebergSource,
    RoutingIdentity,
    SourceLifecycleState,
    WorkClaim,
    WorkExecutionContext,
    WorkKind,
    WorkPhase,
    production_default_spec_revision,
)
from lance_etl.telemetry import TelemetryConfig

DIMENSION: int = 8
TIMESTAMP: datetime = datetime(2026, 7, 14, 9, tzinfo=UTC)


@pytest.fixture(scope="module")
def spark() -> Iterator[SparkSession]:
    """Provide a local Spark session using the locked Python interpreter.

    Yields:
        Two-core UTC Spark session.
    """
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
    session: SparkSession = (
        SparkSession.builder.master("local[2]")
        .appName("lance-etl-canonical-rebuild-tests")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()


def local_spec() -> DatasetSpecRevision:
    """Return a valid small-vector immutable specification.

    Returns:
        Specification with an eight-dimensional vector and small Spark fan-out.
    """
    base: DatasetSpecRevision = production_default_spec_revision()
    fields: tuple[DatasetField, ...] = tuple(
        replace(field, data_type=f"fixed_size_list<float32,{DIMENSION}>", vector_dimension=DIMENSION)
        if field.role is FieldRole.VECTOR
        else field
        for field in base.fields
    )
    candidate: DatasetSpecRevision = replace(
        base,
        fields=fields,
        ingest_shuffle_partitions=4,
        configuration_digest=b"",
    )
    return replace(candidate, configuration_digest=candidate.expected_configuration_digest()).validate()


def source_registration(spec: DatasetSpecRevision) -> IcebergSource:
    """Return a valid source registration for a rebuild context.

    Args:
        spec: Owning database specification.

    Returns:
        Source fixture.
    """
    return IcebergSource(
        source_id=uuid.uuid4(),
        source_name="local",
        spark_catalog="local",
        table_namespace="db",
        table_name="events",
        table_uuid=uuid.uuid4(),
        lance_base_uri="/tmp/lance",
        lifecycle_state=SourceLifecycleState.ACTIVE,
        default_spec_id=spec.spec_id,
        canonical_baseline_snapshot_id=1,
        replay_horizon=timedelta(days=7),
    ).validate()


def vector(value: float) -> list[float]:
    """Return one fixed-dimension vector fixture.

    Args:
        value: Repeated vector value.

    Returns:
        Fixed-size float vector.
    """
    return [value] * DIMENSION


def terminal_table(rows: list[dict[str, object]]) -> pa.Table:
    """Build an exact terminal Lance table including replay columns.

    Args:
        rows: Terminal row dictionaries.

    Returns:
        Table matching the configured terminal schema.
    """
    schema: pa.Schema = pa.schema(
        [
            pa.field("record_id", pa.string(), nullable=False),
            pa.field("ts", pa.timestamp("us", tz="UTC")),
            pa.field("vector", pa.list_(pa.float32(), DIMENSION)),
            pa.field("text", pa.string()),
            pa.field("cluster", pa.string()),
            pa.field("lance_etl_window_seq", pa.int64(), nullable=False),
            pa.field("lance_etl_source_sequence", pa.int64(), nullable=False),
            pa.field("lance_etl_event_digest", pa.binary(32), nullable=False),
            pa.field("is_deleted", pa.bool_(), nullable=False),
        ]
    )
    return pa.Table.from_pylist(rows, schema=schema)


def terminal_row(
    record_id: str,
    sequence: int,
    digest_byte: bytes,
    *,
    deleted: bool = False,
    value: float = 1.0,
) -> dict[str, object]:
    """Build one persisted terminal mutation fixture.

    Args:
        record_id: Logical vector key.
        sequence: Internal Iceberg arrival sequence.
        digest_byte: Single byte repeated into the canonical digest.
        deleted: Tombstone state.
        value: Vector and payload discriminator.

    Returns:
        Complete terminal row.
    """
    return {
        "record_id": record_id,
        "ts": None if deleted else TIMESTAMP,
        "vector": None if deleted else vector(value),
        "text": None if deleted else f"payload-{value}",
        "cluster": None if deleted else "cluster-a",
        "lance_etl_window_seq": sequence,
        "lance_etl_source_sequence": sequence,
        "lance_etl_event_digest": digest_byte * 32,
        "is_deleted": deleted,
    }


def rebuild_context(source_uri: str, source_version: int, candidate_uri: str) -> WorkExecutionContext:
    """Build one live REBUILD execution context.

    Args:
        source_uri: Exact historical source dataset.
        source_version: Exact source version.
        candidate_uri: Isolated deterministic candidate.

    Returns:
        Rebuild context fixture.
    """
    spec: DatasetSpecRevision = local_spec()
    claim: WorkClaim = WorkClaim(
        work_id=uuid.uuid4(),
        dataset_id=uuid.uuid4(),
        kind=WorkKind.REBUILD,
        phase=WorkPhase.COMPACT,
        lease_token=uuid.uuid4(),
        fence_epoch=1,
        attempt_count=1,
        source_snapshot_seq=1,
        spec_revision_id=spec.spec_revision_id,
        ingest_lance_uri=source_uri,
        ingest_lance_version=source_version,
    )
    return WorkExecutionContext(
        claim=claim,
        identity=RoutingIdentity("tenant1", "namespace1", "org1"),
        source_table="local.db.events",
        source=source_registration(spec),
        spec_revision=spec,
        snapshot_id=None,
        parent_snapshot_id=None,
        iceberg_sequence_number=None,
        partition_spec_id=None,
        source_snapshot_kind=None,
        candidate_lance_uri=candidate_uri,
        candidate_lance_version=None,
        artifact_manifest_uri=None,
        artifact_digest=None,
    )


@pytest.mark.integration
@pytest.mark.parametrize("invalid", [None, float("nan"), float("inf"), float("-inf")])
def test_source_profile_rejects_invalid_vector_elements(spark: SparkSession, invalid: float | None) -> None:
    """Null and non-finite elements fail in Spark before Arrow digest execution.

    Args:
        spark: Local Spark session.
        invalid: Invalid vector element under test.
    """
    values: list[float | None] = vector(1.0)
    values[3] = invalid
    source = spark.createDataFrame(
        [("tenant1", "namespace1", "org1", "record1", "upsert", TIMESTAMP, {"vector": values}, {}, {})],
        (
            "tenant_id string, namespace string, org_id string, record_id string, op string, ts timestamp, "
            "vectors map<string,array<float>>, texts map<string,string>, metadata map<string,string>"
        ),
    )
    runner: DistributedIngestRunner = DistributedIngestRunner(spark, TelemetryConfig())

    with pytest.raises(ValueError, match="contains null or non-finite elements"):
        runner.validate_source_profile(source, local_spec())


@pytest.mark.integration
@pytest.mark.parametrize("invalid", [None, float("nan"), float("inf"), float("-inf")])
def test_baseline_profile_rejects_nonfinite_vectors_before_arrow(spark: SparkSession, invalid: float | None) -> None:
    """Baseline canonicalization cannot turn deterministic bad vector elements into Spark retries.

    Args:
        spark: Local Spark session.
        invalid: Non-finite vector element under test.
    """
    values: list[float | None] = vector(1.0)
    values[3] = invalid
    source_frame = spark.createDataFrame(
        [("tenant1", "namespace1", "org1", "record1", "upsert", TIMESTAMP, {"vector": values}, {}, {})],
        (
            "tenant_id string, namespace string, org_id string, record_id string, op string, ts timestamp, "
            "vectors map<string,array<float>>, texts map<string,string>, metadata map<string,string>"
        ),
    )

    with pytest.raises(ValueError, match="null or non-finite vector element"):
        BaselineQualifier(spark).validate_contract(source_frame)


@pytest.mark.integration
def test_distributed_source_digest_matches_frozen_v1_contract(spark: SparkSession) -> None:
    """Range-sorted chunks preserve the durable digest across input layouts.

    Args:
        spark: Local Spark session.
    """
    suffixes: tuple[str, ...] = ("", "\x00", "e\u0301", "é", "ä", "🧭", "𐀀")
    digest_rows: list[tuple[str, int, bytes]] = [
        (
            f"{index:04d}-{suffixes[index % len(suffixes)]}",
            index * 7,
            hashlib.sha256(str(index).encode("utf-8")).digest(),
        )
        for index in range(257)
    ]
    terminal = spark.createDataFrame(
        list(reversed(digest_rows)),
        f"record_id string, {SOURCE_SEQUENCE_COLUMN} long, {EVENT_DIGEST_COLUMN} binary",
    )
    expected: bytes = canonical_source_digest(digest_rows)
    runner: DistributedIngestRunner = DistributedIngestRunner(spark, TelemetryConfig())

    for input_partitions in (2, 5, 9):
        assert runner.compute_source_digest(terminal.repartition(input_partitions)) == (expected, len(digest_rows))

    empty = spark.createDataFrame(
        [],
        f"record_id string, {SOURCE_SEQUENCE_COLUMN} long, {EVENT_DIGEST_COLUMN} binary",
    )
    assert runner.compute_source_digest(empty) == (canonical_source_digest([]), 0)


@pytest.mark.integration
def test_source_digest_spool_avoids_driver_partition_buffering_and_cleans_up(
    spark: SparkSession,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Digest consumption never uses ``toLocalIterator`` and removes its dedicated local spool.

    Args:
        spark: Local Spark session.
        tmp_path: Isolated parent for the driver-created spool directory.
        monkeypatch: Pytest monkeypatch used to reject the old iterator boundary.
    """
    digest_rows: list[tuple[str, int, bytes]] = [
        (f"record-{index:04d}", index, hashlib.sha256(str(index).encode("utf-8")).digest()) for index in range(129)
    ]
    terminal = spark.createDataFrame(
        list(reversed(digest_rows)),
        f"record_id string, {SOURCE_SEQUENCE_COLUMN} long, {EVENT_DIGEST_COLUMN} binary",
    )
    old_iterator: MagicMock = MagicMock(side_effect=AssertionError("driver iterator buffering is forbidden"))
    monkeypatch.setattr(DataFrame, "toLocalIterator", old_iterator)
    monkeypatch.setattr(reconciler_workers.tempfile, "tempdir", str(tmp_path))

    runner: DistributedIngestRunner = DistributedIngestRunner(spark, TelemetryConfig())
    assert runner.compute_source_digest(terminal) == (canonical_source_digest(digest_rows), len(digest_rows))
    old_iterator.assert_not_called()
    assert list(tmp_path.iterdir()) == []

    failed_consumer: MagicMock = MagicMock(side_effect=RuntimeError("digest consumption failed"))
    monkeypatch.setattr(reconciler_workers, "consume_source_digest_spool", failed_consumer)
    with pytest.raises(RuntimeError, match="digest consumption failed"):
        runner.compute_source_digest(terminal)
    assert list(tmp_path.iterdir()) == []


@pytest.mark.integration
def test_source_digest_plan_has_no_single_partition_exchange(
    spark: SparkSession,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Digest ordering uses a multi-partition range exchange.

    Args:
        spark: Local Spark session.
        capsys: Captured Spark plan output.
    """
    terminal = spark.createDataFrame(
        [("b", 2, b"b" * 32), ("a", 1, b"a" * 32)],
        f"record_id string, {SOURCE_SEQUENCE_COLUMN} long, {EVENT_DIGEST_COLUMN} binary",
    )
    chunks = source_digest_chunks(terminal, 4)

    chunks.explain(mode="simple")
    plan: str = capsys.readouterr().out.lower()
    assert "rangepartitioning" in plan
    assert "singlepartition" not in plan
    assert chunks.rdd.getNumPartitions() > 1


@pytest.mark.integration
def test_candidate_counts_use_one_exact_distributed_aggregation(spark: SparkSession, tmp_path: Path) -> None:
    """The publication gate counts duplicates and tombstones without caching Python row objects.

    Args:
        spark: Local Spark session.
        tmp_path: Isolated candidate roots.
    """
    candidate: lance.LanceDataset = lance.write_dataset(
        terminal_table(
            [
                terminal_row("a", 1, b"a"),
                terminal_row("a", 2, b"b", value=2.0),
                terminal_row("b", 1, b"c", deleted=True),
                terminal_row("c", 1, b"d"),
            ]
        ),
        str(tmp_path / "count-candidate.lance"),
        max_rows_per_file=1,
    )
    runner: ConfiguredPublicationRunner = ConfiguredPublicationRunner(spark, TelemetryConfig(), MagicMock())

    assert runner.candidate_counts(candidate.uri, candidate.version, local_spec()) == (4, 3, 3, 2)

    empty: lance.LanceDataset = lance.write_dataset(
        terminal_table([]),
        str(tmp_path / "empty-count-candidate.lance"),
    )
    assert runner.candidate_counts(empty.uri, empty.version, local_spec()) == (0, 0, 0, 0)


@pytest.mark.integration
def test_fragment_tasks_keep_high_fragment_inventory_off_driver(spark: SparkSession, tmp_path: Path) -> None:
    """A high-fragment version becomes bounded executor scan rows with exact coverage.

    Args:
        spark: Local Spark session.
        tmp_path: Isolated candidate root.
    """
    row_count: int = 257
    candidate: lance.LanceDataset = lance.write_dataset(
        terminal_table([terminal_row(f"record-{index:04d}", index, b"a") for index in range(row_count)]),
        str(tmp_path / "high-fragment-candidate.lance"),
        max_rows_per_file=1,
    )
    runner: ConfiguredPublicationRunner = ConfiguredPublicationRunner(spark, TelemetryConfig(), MagicMock())
    tasks = runner.fragment_task_frame(
        candidate.uri,
        candidate.version,
        4,
        ("record_id", DELETED_COLUMN),
    )
    summary = tasks.selectExpr(
        "count(*) AS task_count",
        "count(DISTINCT fragment_id) AS fragment_count",
    ).first()

    assert summary is not None
    assert int(summary["task_count"]) == row_count
    assert int(summary["fragment_count"]) == row_count
    assert runner.candidate_counts(candidate.uri, candidate.version, local_spec()) == (
        row_count,
        row_count,
        row_count,
        row_count,
    )
    rebuilt_uri: str = str(tmp_path / "high-fragment-rebuilt.lance")
    context: WorkExecutionContext = rebuild_context(candidate.uri, candidate.version, rebuilt_uri)
    assert runner.canonical_rebuild(context) == rebuilt_uri
    assert lance.dataset(rebuilt_uri).count_rows() == row_count


@pytest.mark.integration
def test_canonical_rebuild_collapses_duplicates_and_preserves_newest_tombstone(
    spark: SparkSession,
    tmp_path: Path,
) -> None:
    """Recovery emits exactly one maximum-sequence terminal row per record ID.

    Args:
        spark: Local Spark session.
        tmp_path: Isolated source and candidate root.
    """
    duplicate_b: dict[str, object] = terminal_row("b", 3, b"c", value=3.0)
    source: lance.LanceDataset = lance.write_dataset(
        terminal_table(
            [
                terminal_row("a", 1, b"a", value=1.0),
                terminal_row("a", 2, b"b", deleted=True, value=2.0),
                duplicate_b,
                duplicate_b,
                terminal_row("c", 1, b"d", value=1.0),
                terminal_row("c", 4, b"e", value=4.0),
            ]
        ),
        str(tmp_path / "source.lance"),
        max_rows_per_file=2,
    )
    candidate_uri: str = str(tmp_path / "candidate.lance")
    context: WorkExecutionContext = rebuild_context(source.uri, source.version, candidate_uri)
    runner: ConfiguredPublicationRunner = ConfiguredPublicationRunner(spark, TelemetryConfig(), MagicMock())
    assert runner.canonical_rebuild(context) == candidate_uri
    first_version: int = lance.dataset(candidate_uri).version
    assert runner.canonical_rebuild(context) == candidate_uri
    candidate: lance.LanceDataset = lance.dataset(candidate_uri)
    assert candidate.version == first_version
    rows: dict[str, dict[str, object]] = {row["record_id"]: row for row in candidate.to_table().to_pylist()}
    assert set(rows) == {"a", "b", "c"}
    assert rows["a"]["is_deleted"] is True
    assert rows["a"]["lance_etl_source_sequence"] == 2
    assert rows["b"]["lance_etl_source_sequence"] == 3
    assert rows["c"]["lance_etl_source_sequence"] == 4


@pytest.mark.integration
def test_canonical_rebuild_blocks_equal_sequence_conflicts(spark: SparkSession, tmp_path: Path) -> None:
    """Different mutations at one maximum sequence never receive an arbitrary winner.

    Args:
        spark: Local Spark session.
        tmp_path: Isolated source and candidate root.
    """
    source: lance.LanceDataset = lance.write_dataset(
        terminal_table(
            [
                terminal_row("conflict", 7, b"a", value=1.0),
                terminal_row("conflict", 7, b"b", value=2.0),
            ]
        ),
        str(tmp_path / "source-conflict.lance"),
    )
    candidate_uri: str = str(tmp_path / "candidate-conflict.lance")
    context: WorkExecutionContext = rebuild_context(source.uri, source.version, candidate_uri)
    result: str | WorkResult = ConfiguredPublicationRunner(spark, TelemetryConfig(), MagicMock()).canonical_rebuild(
        context
    )
    assert not isinstance(result, str)
    assert result.kind is ResultKind.BLOCKED
    assert result.error_code == "REBUILD_EQUAL_SEQUENCE_CONFLICT"
    assert lance.dataset(candidate_uri).count_rows() == 0
