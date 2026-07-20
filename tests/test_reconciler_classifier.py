"""Retry-classifier gates in the reconciler publication runner.

``ConfiguredPublicationRunner.run`` (workers.py) turns a failed compaction into a transient
``MAINTENANCE_FAILED`` RETRY (workers.py:651) and a failed index build into a transient
``INDEX_BUILD_FAILED`` RETRY (workers.py:662), rather than a terminal BLOCKED result. The gate logic
is exercised in place: a PUBLISH claim over a real candidate dataset with no publication pin flows
through the maintenance and indexing boundaries, both stubbed to inject a failure.

The gates run the driver-side control flow only: Spark is a small in-process fake and the maintenance
and indexing collaborators are replaced, so no real Spark job, Lance index build, or PostgreSQL is
required.
"""

from __future__ import annotations

import uuid
from datetime import timedelta
from pathlib import Path
from unittest.mock import MagicMock

import lance
import pyarrow as pa
import pytest
from conftest import FakeSpark

import lance_etl.reconciler.workers as workers
from lance_etl.reconciler import ResultKind, WorkResult
from lance_etl.reconciler.workers import ConfiguredPublicationRunner
from lance_etl.state import (
    DatasetSpecRevision,
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


class FakeMaintenanceJob:
    """Maintenance-job stand-in returning a preset per-dataset result list."""

    result: list[dict[str, object]] = [{}]
    """The per-dataset result list the fake returns, patched per test."""

    def __init__(self, config: object) -> None:
        """Ignore the real maintenance configuration.

        Args:
            config: The maintenance configuration, unused by the fake.
        """
        del config

    def run(self, spark: object, uris: list[str]) -> list[dict[str, object]]:
        """Return the preset result list.

        Args:
            spark: Ignored Spark session.
            uris: Ignored dataset URIs.

        Returns:
            The preset per-dataset result list.
        """
        del spark, uris
        return FakeMaintenanceJob.result


def source_registration(spec: DatasetSpecRevision) -> IcebergSource:
    """Return a valid source registration bound to the specification.

    Args:
        spec: The owning dataset specification revision.

    Returns:
        A validated source fixture.
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


def publish_context(candidate_uri: str) -> WorkExecutionContext:
    """Build one live PUBLISH execution context over an existing candidate dataset.

    Args:
        candidate_uri: URI of the ingest candidate dataset the publication runner qualifies.

    Returns:
        A PUBLISH context whose ingest URI is the candidate dataset.
    """
    spec: DatasetSpecRevision = production_default_spec_revision()
    claim: WorkClaim = WorkClaim(
        work_id=uuid.uuid4(),
        dataset_id=uuid.uuid4(),
        kind=WorkKind.PUBLISH,
        phase=WorkPhase.PUBLISH,
        lease_token=uuid.uuid4(),
        fence_epoch=1,
        attempt_count=1,
        source_snapshot_seq=1,
        spec_revision_id=spec.spec_revision_id,
        ingest_lance_uri=candidate_uri,
        ingest_lance_version=lance.dataset(candidate_uri).version,
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
        candidate_lance_uri=None,
        candidate_lance_version=None,
        artifact_manifest_uri=None,
        artifact_digest=None,
    )


def make_candidate(tmp_path: Path) -> str:
    """Write a tiny candidate dataset carrying no publication pin.

    Args:
        tmp_path: Pytest-provided temporary directory.

    Returns:
        The candidate dataset URI.
    """
    uri: str = str(tmp_path / "candidate.lance")
    lance.write_dataset(pa.table({"id": pa.array([1, 2, 3], pa.int64())}), uri)
    return uri


def make_runner() -> ConfiguredPublicationRunner:
    """Build a publication runner over the in-process fake Spark session.

    Returns:
        A configured publication runner with a stub prewarmer.
    """
    return ConfiguredPublicationRunner(FakeSpark(), TelemetryConfig(service="lance-etl-tests", env="test"), MagicMock())


def test_maintenance_failure_classifies_as_retry(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """A failed compaction becomes a transient MAINTENANCE_FAILED retry, not a terminal block."""
    candidate_uri: str = make_candidate(tmp_path)
    monkeypatch.setattr(FakeMaintenanceJob, "result", [{"error": "compaction exploded"}])
    monkeypatch.setattr(workers, "MaintenanceJob", FakeMaintenanceJob)

    runner: ConfiguredPublicationRunner = make_runner()
    result: WorkResult = runner.run(publish_context(candidate_uri))

    assert result.kind is ResultKind.RETRY
    assert result.error_code == "MAINTENANCE_FAILED"
    assert "compaction exploded" in str(result.error_message)


def test_index_build_failure_classifies_as_retry(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """A failed index build becomes a transient INDEX_BUILD_FAILED retry after clean compaction."""
    candidate_uri: str = make_candidate(tmp_path)
    monkeypatch.setattr(FakeMaintenanceJob, "result", [{"fragments_removed": 0}])
    monkeypatch.setattr(workers, "MaintenanceJob", FakeMaintenanceJob)

    runner: ConfiguredPublicationRunner = make_runner()

    def failing_indexing(
        self: ConfiguredPublicationRunner, candidate: str, spec: DatasetSpecRevision
    ) -> list[dict[str, object]]:
        """Return one indexing result carrying a per-index error.

        Args:
            self: The bound publication runner, unused.
            candidate: Ignored candidate URI.
            spec: Ignored dataset specification.

        Returns:
            A single result whose ``indexes`` list holds one errored index.
        """
        del self, candidate, spec
        return [{"uri": candidate_uri, "indexes": [{"index": "vector_idx", "error": "shard build failed"}]}]

    monkeypatch.setattr(ConfiguredPublicationRunner, "run_indexing", failing_indexing)
    result: WorkResult = runner.run(publish_context(candidate_uri))

    assert result.kind is ResultKind.RETRY
    assert result.error_code == "INDEX_BUILD_FAILED"
    assert "shard build failed" in str(result.error_message)
