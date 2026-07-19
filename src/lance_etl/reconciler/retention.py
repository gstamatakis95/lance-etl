"""Bounded crash-resumable retention for publication pins and control-plane audit rows."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Protocol

import lance
import pyarrow.fs as pa_fs
from pyspark.sql import SparkSession

from lance_etl.cloud_storage import resolve_filesystem
from lance_etl.publication.manifest import candidate_pin_name, tag_version
from lance_etl.reconciler.config import ReconcilerSettings
from lance_etl.reconciler.results import ReconcileSummary
from lance_etl.state import PublicationCleanup
from lance_etl.telemetry import Telemetry, TelemetryConfig


class RetentionRepository(Protocol):
    """Durable operations required by the bounded retention sweep."""

    def claim_publication_cleanup(
        self,
        current: datetime,
        limit: int,
    ) -> list[PublicationCleanup]:
        """Retire a bounded batch before external deletion.

        Args:
            current: Fixed transaction clock used with each revision's retention policy.
            limit: Maximum claims.

        Returns:
            Durable external cleanup claims.
        """
        ...

    def finalize_publication_cleanup(self, cleanup: PublicationCleanup) -> bool:
        """Delete one retired publication audit row.

        Args:
            cleanup: Completed external cleanup identity.

        Returns:
            Whether the row is absent afterward.
        """
        ...

    def delete_completed_audit(self, completed_before: datetime, limit: int) -> tuple[int, int]:
        """Prune old source-application audit rows.

        Args:
            completed_before: Fixed audit horizon.
            limit: Per-table deletion bound.

        Returns:
            Deleted work and source-snapshot counts.
        """
        ...


@dataclass(frozen=True, slots=True)
class PublicationRetentionSweep:
    """Remove old external evidence before pruning its durable audit identity."""

    repository: RetentionRepository
    spark: SparkSession
    settings: ReconcilerSettings
    telemetry_config: TelemetryConfig

    def reconcile(self) -> ReconcileSummary:
        """Run one bounded idempotent retention sweep.

        Returns:
            Inspected, reconciled, and deferred cleanup counts.
        """
        current: datetime = datetime.now(UTC)
        audit_cutoff: datetime = current - self.settings.audit_retention
        claims: list[PublicationCleanup] = self.repository.claim_publication_cleanup(
            current,
            self.settings.cleanup_batch_size,
        )
        telemetry_config: TelemetryConfig = self.telemetry_config

        def remove_external(cleanup: PublicationCleanup) -> tuple[PublicationCleanup, str | None]:
            """Delete one immutable pin and artifact on an executor.

            Args:
                cleanup: Durable retired publication.

            Returns:
                Cleanup identity and optional bounded error text.
            """
            telemetry: Telemetry = Telemetry.create(telemetry_config)
            try:
                dataset: lance.LanceDataset = lance.dataset(cleanup.lance_uri)
                pin: str = cleanup.pin_name or candidate_pin_name(cleanup.work_id)
                pinned_version: int | None = tag_version(dataset, pin)
                if pinned_version is not None and int(pinned_version) != cleanup.lance_version:
                    raise RuntimeError("retired publication pin names a different exact version")
                if pinned_version is not None:
                    dataset.tags.delete(pin)
                    telemetry.incr("publication.pin_deleted")
                filesystem: pa_fs.FileSystem
                path: str
                filesystem, path = resolve_filesystem(cleanup.manifest_uri, None)
                if filesystem.get_file_info(path).type != pa_fs.FileType.NotFound:
                    filesystem.delete_file(path)
                    telemetry.incr("publication.artifact_deleted")
            except Exception as error:
                return cleanup, str(error)[:1000]
            return cleanup, None

        if claims:
            outcomes: list[tuple[PublicationCleanup, str | None]] = (
                self.spark.sparkContext.parallelize(claims, len(claims)).map(remove_external).collect()
            )
        else:
            outcomes = []
        reconciled: int = 0
        deferred: int = 0
        cleanup: PublicationCleanup
        error: str | None
        for cleanup, error in outcomes:
            if error is not None:
                deferred += 1
            elif self.repository.finalize_publication_cleanup(cleanup):
                reconciled += 1
            else:
                deferred += 1
        work_deleted: int
        windows_deleted: int
        work_deleted, windows_deleted = self.repository.delete_completed_audit(
            audit_cutoff,
            self.settings.cleanup_batch_size,
        )
        return ReconcileSummary(
            inspected=len(claims),
            reconciled=reconciled + work_deleted + windows_deleted,
            deferred=deferred,
        )
