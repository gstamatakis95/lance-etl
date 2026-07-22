"""Typed target-work outcomes reconciled through fenced repository transitions."""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum

from lance_etl.state import PublicationEvidence, WorkClaim


class ResultKind(StrEnum):
    """Closed set of worker outcomes understood by the reconciler."""

    INGEST_SUCCEEDED = "INGEST_SUCCEEDED"
    PUBLISH_SUCCEEDED = "PUBLISH_SUCCEEDED"
    RETRY = "RETRY"
    BLOCKED = "BLOCKED"


@dataclass(frozen=True, slots=True)
class WorkResult:
    """One claim outcome with exact durable completion evidence."""

    claim: WorkClaim
    kind: ResultKind
    data_lance_version: int | None = None
    indexed_lance_version: int | None = None
    source_row_count: int | None = None
    source_digest: bytes | None = None
    candidate_lance_uri: str | None = None
    manifest_uri: str | None = None
    manifest_digest: bytes | None = None
    publication_evidence: PublicationEvidence | None = None
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
            accepted: bool = (
                self.data_lance_version is not None
                and self.data_lance_version > 0
                and self.source_row_count is not None
                and self.source_row_count >= 0
                and self.source_digest is not None
                and len(self.source_digest) == 32
            )
            if not accepted:
                raise ValueError("INGEST success requires exact Lance version, row count, and 32-byte source digest")
        elif self.kind is ResultKind.PUBLISH_SUCCEEDED:
            accepted: bool = (
                self.indexed_lance_version is not None
                and self.indexed_lance_version > 0
                and bool(self.candidate_lance_uri)
                and bool(self.manifest_uri)
                and self.manifest_digest is not None
                and len(self.manifest_digest) == 32
                and self.publication_evidence is not None
            )
            if not accepted:
                raise ValueError(
                    "PUBLISH success requires candidate URI, exact version, manifest, and publication evidence"
                )
            self.publication_evidence.validate()
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
