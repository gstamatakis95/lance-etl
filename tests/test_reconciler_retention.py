"""Tests for bounded publication and PostgreSQL audit retention."""

from __future__ import annotations

import uuid
from datetime import timedelta
from pathlib import Path
from unittest.mock import MagicMock

import lance
import pyarrow as pa

from lance_etl.publication.manifest import candidate_pin_name, tag_version
from lance_etl.reconciler import ReconcilerSettings, ReconcileSummary
from lance_etl.reconciler.retention import PublicationRetentionSweep
from lance_etl.state import PublicationCleanup
from lance_etl.telemetry import TelemetryConfig


def eager_spark() -> MagicMock:
    """Return a Spark stand-in that executes map tasks synchronously.

    Returns:
        Spark fixture supporting the retention fan-out.
    """
    spark: MagicMock = MagicMock()

    def parallelize(items: list[PublicationCleanup], partitions: int) -> MagicMock:
        """Capture cleanup tasks and execute their mapper eagerly.

        Args:
            items: Bounded cleanup identities.
            partitions: Requested one-task-per-cleanup partition count.

        Returns:
            RDD fixture.
        """
        assert partitions == len(items)
        rdd: MagicMock = MagicMock()

        def map_items(function: object) -> MagicMock:
            """Apply one executor closure to every cleanup.

            Args:
                function: Cleanup mapper.

            Returns:
                Collectable result fixture.
            """
            mapped: MagicMock = MagicMock()
            mapped.collect.return_value = [function(item) for item in items]
            return mapped

        rdd.map.side_effect = map_items
        return rdd

    spark.sparkContext.parallelize.side_effect = parallelize
    return spark


def retention_settings() -> ReconcilerSettings:
    """Return bounded audit settings for retention tests.

    Returns:
        Validated operational settings.
    """
    return ReconcilerSettings(audit_retention=timedelta(days=30), cleanup_batch_size=16).validate()


def cleanup_claim(
    dataset_uri: str,
    lance_version: int,
    manifest_uri: str,
    work_id: uuid.UUID,
) -> PublicationCleanup:
    """Return one retired immutable publication cleanup identity.

    Args:
        dataset_uri: Candidate Lance dataset URI.
        lance_version: Exact retired version.
        manifest_uri: Immutable manifest URI.
        work_id: Work-derived pin identity.

    Returns:
        Cleanup fixture.
    """
    return PublicationCleanup(
        publication_id=uuid.uuid4(),
        work_id=work_id,
        dataset_id=uuid.uuid4(),
        lance_uri=dataset_uri,
        lance_version=lance_version,
        manifest_uri=manifest_uri,
        pin_name=candidate_pin_name(work_id),
    )


def test_retention_removes_exact_pin_and_artifact_before_finalizing(tmp_path: Path) -> None:
    """External evidence disappears before its durable publication row is finalized.

    Args:
        tmp_path: Local Lance and artifact root.
    """
    dataset_uri: str = str(tmp_path / "candidate.lance")
    dataset: lance.LanceDataset = lance.write_dataset(pa.table({"id": [1]}), dataset_uri)
    work_id: uuid.UUID = uuid.uuid4()
    dataset.tags.create(candidate_pin_name(work_id), dataset.version)
    artifact: Path = tmp_path / "manifest.json"
    artifact.write_text("{}", encoding="utf-8")
    cleanup: PublicationCleanup = cleanup_claim(dataset_uri, dataset.version, str(artifact), work_id)
    repository: MagicMock = MagicMock()
    repository.claim_publication_cleanup.return_value = [cleanup]
    repository.finalize_publication_cleanup.return_value = True
    repository.delete_completed_audit.return_value = (2, 1)
    sweep: PublicationRetentionSweep = PublicationRetentionSweep(
        repository, eager_spark(), retention_settings(), TelemetryConfig()
    )
    summary: ReconcileSummary = sweep.reconcile()
    assert tag_version(lance.dataset(dataset_uri), cleanup.pin_name) is None
    assert not artifact.exists()
    repository.finalize_publication_cleanup.assert_called_once_with(cleanup)
    assert summary.inspected == 1
    assert summary.reconciled == 4
    assert summary.deferred == 0


def test_retention_defers_a_mismatched_immutable_pin(tmp_path: Path) -> None:
    """A pin naming unexpected evidence stays durable for investigation.

    Args:
        tmp_path: Local Lance and artifact root.
    """
    dataset_uri: str = str(tmp_path / "candidate.lance")
    dataset: lance.LanceDataset = lance.write_dataset(pa.table({"id": [1]}), dataset_uri)
    work_id: uuid.UUID = uuid.uuid4()
    dataset.tags.create(candidate_pin_name(work_id), dataset.version)
    artifact: Path = tmp_path / "manifest.json"
    artifact.write_text("{}", encoding="utf-8")
    cleanup: PublicationCleanup = cleanup_claim(dataset_uri, dataset.version + 1, str(artifact), work_id)
    repository: MagicMock = MagicMock()
    repository.claim_publication_cleanup.return_value = [cleanup]
    repository.delete_completed_audit.return_value = (0, 0)
    sweep: PublicationRetentionSweep = PublicationRetentionSweep(
        repository, eager_spark(), retention_settings(), TelemetryConfig()
    )
    summary: ReconcileSummary = sweep.reconcile()
    assert tag_version(lance.dataset(dataset_uri), cleanup.pin_name) == dataset.version
    assert artifact.exists()
    repository.finalize_publication_cleanup.assert_not_called()
    assert summary == type(summary)(inspected=1, reconciled=0, deferred=1)


def test_retention_prunes_old_audit_without_external_claims() -> None:
    """Old work and source rows are pruned when no publication is eligible."""
    repository: MagicMock = MagicMock()
    repository.claim_publication_cleanup.return_value = []
    repository.delete_completed_audit.return_value = (7, 5)
    spark: MagicMock = MagicMock()
    sweep: PublicationRetentionSweep = PublicationRetentionSweep(
        repository, spark, retention_settings(), TelemetryConfig()
    )
    summary: ReconcileSummary = sweep.reconcile()
    spark.sparkContext.parallelize.assert_not_called()
    assert summary.inspected == 0
    assert summary.reconciled == 12
    assert summary.deferred == 0
