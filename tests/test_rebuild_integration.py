"""Real local Spark and Lance tests for canonical dataset rebuilds."""

from __future__ import annotations

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
from pyspark.sql import SparkSession

from lance_etl.reconciler import ResultKind, WorkResult
from lance_etl.reconciler.workers import ConfiguredPublicationRunner
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
            pa.field("vector_id", pa.string(), nullable=False),
            pa.field("event_timestamp", pa.timestamp("us", tz="UTC")),
            pa.field("vector", pa.list_(pa.float32(), DIMENSION)),
            pa.field("text", pa.string()),
            pa.field("cluster", pa.string()),
            pa.field("ttl", pa.int64()),
            pa.field("lance_etl_window_seq", pa.int64(), nullable=False),
            pa.field("lance_etl_source_sequence", pa.int64(), nullable=False),
            pa.field("lance_etl_event_digest", pa.binary(32), nullable=False),
            pa.field("is_deleted", pa.bool_(), nullable=False),
        ]
    )
    return pa.Table.from_pylist(rows, schema=schema)


def terminal_row(
    vector_id: str,
    sequence: int,
    digest_byte: bytes,
    *,
    deleted: bool = False,
    value: float = 1.0,
) -> dict[str, object]:
    """Build one persisted terminal mutation fixture.

    Args:
        vector_id: Logical vector key.
        sequence: Internal Iceberg arrival sequence.
        digest_byte: Single byte repeated into the canonical digest.
        deleted: Tombstone state.
        value: Vector and payload discriminator.

    Returns:
        Complete terminal row.
    """
    return {
        "vector_id": vector_id,
        "event_timestamp": None if deleted else TIMESTAMP,
        "vector": None if deleted else vector(value),
        "text": None if deleted else f"payload-{value}",
        "cluster": None if deleted else "cluster-a",
        "ttl": None if deleted else 0,
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
def test_canonical_rebuild_collapses_duplicates_and_preserves_newest_tombstone(
    spark: SparkSession,
    tmp_path: Path,
) -> None:
    """Recovery emits exactly one maximum-sequence terminal row per vector ID.

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
    rows: dict[str, dict[str, object]] = {row["vector_id"]: row for row in candidate.to_table().to_pylist()}
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
