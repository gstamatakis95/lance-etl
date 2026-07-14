"""Versioned deployment policy and secret-backed reconciler runtime settings."""

from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import timedelta

SYSTEMIC_RETRIES: int = 24
"""Fixed Airflow retry count for scheduler or cluster-level failures."""


@dataclass(frozen=True, slots=True)
class DeploymentProfile:
    """Release-owned reconciler policy that is never supplied by an Airflow run."""

    profile_id: str = "production-v1"
    schedule: str = "*/5 * * * *"
    claim_batch_size: int = 1
    max_drain_batches: int = 64
    max_windows_per_plan: int = 32
    lease_duration: timedelta = timedelta(minutes=15)
    lease_heartbeat_interval: timedelta = timedelta(minutes=5)
    retry_base_delay: timedelta = timedelta(seconds=30)
    retry_max_delay: timedelta = timedelta(minutes=30)
    max_due_work: int = 10_000
    max_open_work_age: timedelta = timedelta(hours=1)
    max_retention_age: timedelta = timedelta(hours=24)
    vector_fields: tuple[tuple[str, int], ...] = (("vector", 128),)
    text_fields: tuple[str, ...] = ("text",)
    metadata_fields: tuple[str, ...] = ("cluster",)
    include_ttl: bool = True
    ingest_shuffle_partitions: int = 32
    scalar_index_fields: tuple[str, ...] = ("cluster", "event_timestamp")
    bitmap_index_fields: tuple[str, ...] = ("is_deleted",)
    zonemap_index_fields: tuple[str, ...] = ("event_timestamp",)
    vector_metric: str = "cosine"
    spark_conf: tuple[tuple[str, str], ...] = (
        ("spark.executor.instances", "8"),
        ("spark.executor.memory", "8g"),
        ("spark.driver.memory", "8g"),
        ("spark.executor.memoryOverheadFactor", "0.3"),
        ("spark.speculation", "false"),
    )

    def retry_delay(self, attempt_count: int) -> timedelta:
        """Return bounded exponential target-work retry delay.

        Args:
            attempt_count: Durable claim attempt count, starting at one.

        Returns:
            Code-owned delay capped by ``retry_max_delay``.
        """
        exponent = max(0, min(attempt_count - 1, 10))
        seconds = self.retry_base_delay.total_seconds() * (2**exponent)
        return min(timedelta(seconds=seconds), self.retry_max_delay)

    def spark_configuration(self) -> dict[str, str]:
        """Return a mutable Spark-submit configuration copy.

        Returns:
            Release-owned Spark configuration.
        """
        return dict(self.spark_conf)


@dataclass(frozen=True, slots=True)
class RuntimeSettings:
    """Deployment identity and secret-manager values consumed by the reconciler process."""

    database_url: str
    lance_base_uri: str
    source_table: str
    datadog_service: str
    datadog_env: str
    canonical_baseline_snapshot_id: int | None

    @classmethod
    def from_environment(cls) -> RuntimeSettings:
        """Load the small deployment-owned runtime contract from process environment.

        Returns:
            Validated runtime settings.

        Raises:
            ValueError: If a required deployment value is absent.
        """
        required = {
            "database_url": os.environ.get("LANCE_ETL_DATABASE_URL", "").strip(),
            "lance_base_uri": os.environ.get("LANCE_ETL_LANCE_BASE_URI", "").strip(),
            "source_table": os.environ.get("LANCE_ETL_SOURCE_TABLE", "").strip(),
        }
        missing = sorted(name for name, value in required.items() if not value)
        if missing:
            raise ValueError(f"missing reconciler runtime settings: {', '.join(missing)}")
        baseline_value = os.environ.get("LANCE_ETL_CANONICAL_BASELINE_SNAPSHOT_ID", "").strip()
        baseline_snapshot_id = int(baseline_value) if baseline_value else None
        if baseline_snapshot_id is not None and baseline_snapshot_id < 0:
            raise ValueError("canonical baseline snapshot id must be non-negative")
        return cls(
            database_url=required["database_url"],
            lance_base_uri=required["lance_base_uri"],
            source_table=required["source_table"],
            datadog_service=os.environ.get("DD_SERVICE", "lance-reconciler"),
            datadog_env=os.environ.get("DD_ENV", "prod"),
            canonical_baseline_snapshot_id=baseline_snapshot_id,
        )


def production_profile() -> DeploymentProfile:
    """Return the sole scheduled production policy bundled with this release.

    Returns:
        Immutable production deployment profile.
    """
    return DeploymentProfile()
