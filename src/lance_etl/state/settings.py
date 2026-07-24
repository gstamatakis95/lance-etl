"""Typed operational settings sourced from process environment bootstrap."""

from __future__ import annotations

import hashlib
import os
import uuid
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import timedelta


@dataclass(frozen=True, slots=True)
class ReconcilerSettings:
    """Operational queue, retry, retention, and SLO settings from process bootstrap."""

    poll_interval: timedelta = timedelta(seconds=30)
    """Delay between control-loop polls when no work is due."""
    claim_batch_size: int = 1
    """Maximum dataset-disjoint claims taken per drain batch."""
    max_drain_batches: int = 64
    """Maximum drain batches processed per control-loop cycle."""
    max_snapshots_per_plan: int = 32
    """Maximum source snapshots enqueued from one planning pass."""
    lease_duration: timedelta = timedelta(minutes=15)
    """Duration a claimed work lease remains valid."""
    lease_heartbeat_interval: timedelta = timedelta(minutes=5)
    """Interval between lease heartbeats, shorter than the lease duration."""
    retry_base_delay: timedelta = timedelta(seconds=30)
    """Base exponential-backoff delay for transient retries."""
    retry_max_delay: timedelta = timedelta(minutes=30)
    """Upper bound on the backoff delay for transient retries."""
    max_attempts: int = 10
    """Maximum durable attempts before a work item is blocked."""
    max_due_work: int = 10_000
    """Maximum due work items considered in one scan."""
    max_open_work_age: timedelta = timedelta(hours=1)
    """Age at which open work is reported as an SLO breach."""
    max_retention_age: timedelta = timedelta(hours=24)
    """Age at which retained publications become eligible for cleanup."""
    audit_retention: timedelta = timedelta(days=30)
    """Retention horizon for audit rows before cleanup."""
    cleanup_batch_size: int = 128
    """Maximum rows removed per retention cleanup batch."""

    @classmethod
    def from_environment(cls, environment: Mapping[str, str] | None = None) -> ReconcilerSettings:
        """Build validated settings from environment overrides with code defaults.

        Args:
            environment: Process environment mapping, defaulting to ``os.environ``.

        Returns:
            Validated settings sourced from bootstrap configuration.

        Raises:
            ValueError: If an override is malformed or violates the control-loop contract.
        """
        source: Mapping[str, str] = environment if environment is not None else os.environ
        defaults: ReconcilerSettings = cls()
        return cls(
            poll_interval=env_seconds(source, "LANCE_ETL_POLL_INTERVAL_SECONDS", defaults.poll_interval),
            claim_batch_size=env_int(source, "LANCE_ETL_CLAIM_BATCH_SIZE", defaults.claim_batch_size),
            max_drain_batches=env_int(source, "LANCE_ETL_MAX_DRAIN_BATCHES", defaults.max_drain_batches),
            max_snapshots_per_plan=env_int(source, "LANCE_ETL_MAX_SNAPSHOTS_PER_PLAN", defaults.max_snapshots_per_plan),
            lease_duration=env_seconds(source, "LANCE_ETL_LEASE_DURATION_SECONDS", defaults.lease_duration),
            lease_heartbeat_interval=env_seconds(
                source, "LANCE_ETL_LEASE_HEARTBEAT_SECONDS", defaults.lease_heartbeat_interval
            ),
            retry_base_delay=env_seconds(source, "LANCE_ETL_RETRY_BASE_DELAY_SECONDS", defaults.retry_base_delay),
            retry_max_delay=env_seconds(source, "LANCE_ETL_RETRY_MAX_DELAY_SECONDS", defaults.retry_max_delay),
            max_attempts=env_int(source, "LANCE_ETL_MAX_ATTEMPTS", defaults.max_attempts),
            max_due_work=env_int(source, "LANCE_ETL_MAX_DUE_WORK", defaults.max_due_work),
            max_open_work_age=env_seconds(source, "LANCE_ETL_MAX_OPEN_WORK_AGE_SECONDS", defaults.max_open_work_age),
            max_retention_age=env_seconds(source, "LANCE_ETL_MAX_RETENTION_AGE_SECONDS", defaults.max_retention_age),
            audit_retention=env_seconds(source, "LANCE_ETL_AUDIT_RETENTION_SECONDS", defaults.audit_retention),
            cleanup_batch_size=env_int(source, "LANCE_ETL_CLEANUP_BATCH_SIZE", defaults.cleanup_batch_size),
        ).validate()

    def validate(self) -> ReconcilerSettings:
        """Validate queue, timing, retry, retention, and SLO bounds.

        Returns:
            This validated value.

        Raises:
            ValueError: If a count or duration violates the local control-loop contract.
        """
        positive_counts: tuple[int, ...] = (
            self.claim_batch_size,
            self.max_drain_batches,
            self.max_snapshots_per_plan,
            self.max_attempts,
            self.max_due_work,
            self.cleanup_batch_size,
        )
        if min(positive_counts) < 1:
            raise ValueError("reconciler queue and planning counts must be positive")
        positive_durations: tuple[timedelta, ...] = (
            self.poll_interval,
            self.lease_duration,
            self.lease_heartbeat_interval,
            self.max_open_work_age,
            self.max_retention_age,
            self.audit_retention,
        )
        if any(duration <= timedelta(0) for duration in positive_durations):
            raise ValueError("reconciler timing and SLO durations must be positive")
        if self.lease_heartbeat_interval >= self.lease_duration:
            raise ValueError("lease heartbeat interval must be shorter than the lease duration")
        if self.retry_base_delay <= timedelta(0) or self.retry_max_delay < self.retry_base_delay:
            raise ValueError("retry delays must be positive and monotonically bounded")
        return self

    def retry_delay(self, attempt_count: int, work_id: uuid.UUID | None = None) -> timedelta:
        """Return a bounded reproducibly jittered exponential retry delay.

        Args:
            attempt_count: Durable claim attempt count, starting at one.
            work_id: Stable work identity used for deterministic jitter.

        Returns:
            Delay capped by ``retry_max_delay``.

        Raises:
            ValueError: If ``attempt_count`` is not positive.
        """
        if attempt_count < 1:
            raise ValueError("attempt_count must be positive")
        exponent: int = max(0, min(attempt_count - 1, 10))
        cap_seconds: float = min(
            self.retry_base_delay.total_seconds() * (2**exponent),
            self.retry_max_delay.total_seconds(),
        )
        seed: bytes = f"{work_id or 'reconciler'}:{attempt_count}".encode()
        sample: float = int.from_bytes(hashlib.sha256(seed).digest()[:8], "big") / float(2**64 - 1)
        return timedelta(seconds=cap_seconds * sample)


def default_reconciler_settings() -> ReconcilerSettings:
    """Return validated local operational defaults.

    Returns:
        Immutable default settings.
    """
    return ReconcilerSettings().validate()


def env_int(source: Mapping[str, str], name: str, default: int) -> int:
    """Read one integer environment override.

    Args:
        source: Environment mapping.
        name: Environment variable name.
        default: Value used when the variable is absent or blank.

    Returns:
        Parsed integer or the default.

    Raises:
        ValueError: If a present value is not a valid integer.
    """
    raw: str | None = source.get(name)
    if raw is None or not raw.strip():
        return default
    return int(raw)


def env_seconds(source: Mapping[str, str], name: str, default: timedelta) -> timedelta:
    """Read one whole-second duration environment override.

    Args:
        source: Environment mapping.
        name: Environment variable name.
        default: Value used when the variable is absent or blank.

    Returns:
        Parsed duration or the default.

    Raises:
        ValueError: If a present value is not a valid integer number of seconds.
    """
    raw: str | None = source.get(name)
    if raw is None or not raw.strip():
        return default
    return timedelta(seconds=int(raw))
