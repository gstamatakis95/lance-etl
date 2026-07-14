"""Errors raised while planning immutable Iceberg source windows."""

from __future__ import annotations


class SourcePlanningError(RuntimeError):
    """Base class for deterministic source-planning failures."""


class SourceContractError(SourcePlanningError):
    """Indicate that table identity or partition metadata violates the source contract."""


class SourceLineageError(SourcePlanningError):
    """Indicate that the pinned Iceberg ancestry cannot be replayed safely."""


class SourceSnapshotBlockedError(SourcePlanningError):
    """Indicate that a snapshot contains an unsupported or untrusted logical change.

    Attributes:
        snapshot_id: Snapshot that must be classified by an operator.
        error_code: Bounded machine-readable reason.
    """

    snapshot_id: int
    error_code: str


def blocked_snapshot_error(snapshot_id: int, error_code: str, message: str) -> SourceSnapshotBlockedError:
    """Create a blocked-snapshot exception without a custom dunder constructor.

    Args:
        snapshot_id: Snapshot that must be classified by an operator.
        error_code: Bounded machine-readable reason.
        message: Human-readable diagnostic.

    Returns:
        Exception carrying both bounded state fields and the diagnostic message.
    """
    error = SourceSnapshotBlockedError(message)
    error.snapshot_id = snapshot_id
    error.error_code = error_code
    return error


class SourceBaselineError(SourcePlanningError):
    """Indicate that a requested initial baseline lacks a valid canonical proof."""
