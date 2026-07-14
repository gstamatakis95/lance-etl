"""Typed target-work outcomes reconciled through fenced repository transitions."""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum

from lance_etl.state import WorkClaim, WorkPhase


class ResultKind(StrEnum):
    """Closed set of worker outcomes understood by the reconciler."""

    INGEST_SUCCEEDED = "INGEST_SUCCEEDED"
    SERVE_SUCCEEDED = "SERVE_SUCCEEDED"
    PHASE_ADVANCED = "PHASE_ADVANCED"
    RETRY = "RETRY"
    BLOCKED = "BLOCKED"


@dataclass(frozen=True, slots=True)
class WorkResult:
    """One claim outcome with exact durable completion evidence."""

    claim: WorkClaim
    kind: ResultKind
    next_phase: WorkPhase | None = None
    data_lance_version: int | None = None
    indexed_lance_version: int | None = None
    source_row_count: int | None = None
    source_digest: bytes | None = None
    candidate_lance_uri: str | None = None
    artifact_manifest_uri: str | None = None
    artifact_digest: bytes | None = None
    error_code: str | None = None
    error_message: str | None = None

    def validate(self) -> WorkResult:
        """Validate fields required by this result kind.

        Returns:
            This result after validation.

        Raises:
            ValueError: If required exact outputs are absent or malformed.
        """
        if self.kind is ResultKind.INGEST_SUCCEEDED:
            accepted = (
                self.data_lance_version is not None
                and self.data_lance_version > 0
                and self.source_row_count is not None
                and self.source_row_count >= 0
                and self.source_digest is not None
                and len(self.source_digest) == 32
            )
            if not accepted:
                raise ValueError("INGEST success requires exact Lance version, row count, and 32-byte source digest")
        elif self.kind is ResultKind.SERVE_SUCCEEDED:
            accepted = (
                self.indexed_lance_version is not None
                and self.indexed_lance_version > 0
                and bool(self.candidate_lance_uri)
                and bool(self.artifact_manifest_uri)
                and self.artifact_digest is not None
                and len(self.artifact_digest) == 32
            )
            if not accepted:
                raise ValueError(
                    "SERVE success requires candidate URI, exact indexed version, and immutable artifact evidence"
                )
        elif self.kind is ResultKind.PHASE_ADVANCED and self.next_phase is None:
            raise ValueError("phase advancement requires next_phase")
        elif self.kind in (ResultKind.RETRY, ResultKind.BLOCKED) and not self.error_code:
            raise ValueError("failure result requires a bounded error_code")
        return self


@dataclass(frozen=True, slots=True)
class DispatchSummary:
    """Constant-size summary of one bounded queue drain."""

    claimed: int
    succeeded: int
    advanced: int
    retried: int
    blocked: int
    stale: int


@dataclass(frozen=True, slots=True)
class ReconcileSummary:
    """Constant-size summary of an external result reconciliation sweep."""

    inspected: int
    reconciled: int
    deferred: int
