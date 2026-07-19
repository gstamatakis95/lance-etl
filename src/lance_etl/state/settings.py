"""Typed operational settings loaded from the PostgreSQL singleton."""

from __future__ import annotations

import hashlib
import uuid
from dataclasses import dataclass
from datetime import timedelta


@dataclass(frozen=True, slots=True)
class ReconcilerSettings:
    """Operational queue, retry, retention, and SLO settings."""

    poll_interval: timedelta = timedelta(seconds=30)
    claim_batch_size: int = 1
    max_drain_batches: int = 64
    max_snapshots_per_plan: int = 32
    lease_duration: timedelta = timedelta(minutes=15)
    lease_heartbeat_interval: timedelta = timedelta(minutes=5)
    retry_base_delay: timedelta = timedelta(seconds=30)
    retry_max_delay: timedelta = timedelta(minutes=30)
    max_attempts: int = 10
    max_due_work: int = 10_000
    max_open_work_age: timedelta = timedelta(hours=1)
    max_retention_age: timedelta = timedelta(hours=24)
    audit_retention: timedelta = timedelta(days=30)
    cleanup_batch_size: int = 128

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
