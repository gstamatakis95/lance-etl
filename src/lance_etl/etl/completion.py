"""Monotonic Lance dataset completion marker and ambiguous-result reconciliation."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass

import lance

from lance_etl.telemetry import Telemetry, commit_with_retries

LAST_APPLIED_WINDOW_KEY: str = "lance_etl.last_applied_window_seq"
LAST_APPLIED_DIGEST_KEY: str = "lance_etl.last_applied_source_digest"


class CompletionConflict(RuntimeError):
    """Signal incompatible completion state for one durable source-window identity."""


@dataclass(frozen=True)
class CompletionMarker:
    """Reconciled dataset completion state.

    Attributes:
        window_seq: Greatest durably completed source window.
        source_digest: Raw 32-byte digest for that target and window.
        lance_version: Exact Lance version carrying or following the marker.
    """

    window_seq: int
    source_digest: bytes
    lance_version: int


def parse_completion_marker(dataset: lance.LanceDataset) -> CompletionMarker | None:
    """Read and validate a completion marker from an open dataset.

    Args:
        dataset: Open dataset handle.

    Returns:
        Parsed marker or null when both fields are absent.

    Raises:
        CompletionConflict: If the marker is partial or malformed.
    """
    config = dataset.config()
    raw_window = config.get(LAST_APPLIED_WINDOW_KEY)
    raw_digest = config.get(LAST_APPLIED_DIGEST_KEY)
    if raw_window is None and raw_digest is None:
        return None
    if raw_window is None or raw_digest is None:
        raise CompletionConflict("dataset completion marker is partial")
    try:
        window_seq = int(raw_window)
        source_digest = bytes.fromhex(raw_digest)
    except (TypeError, ValueError) as error:
        raise CompletionConflict("dataset completion marker is malformed") from error
    if window_seq < 0 or len(source_digest) != 32:
        raise CompletionConflict("dataset completion marker is malformed")
    return CompletionMarker(window_seq=window_seq, source_digest=source_digest, lance_version=dataset.version)


def completion_is_desired(marker: CompletionMarker | None, window_seq: int, source_digest: bytes) -> bool:
    """Return whether stored completion proves the requested work is durable.

    Args:
        marker: Stored completion marker.
        window_seq: Requested durable work sequence.
        source_digest: Requested source digest.

    Returns:
        True when the exact marker exists or a later window supersedes it.

    Raises:
        CompletionConflict: If the same window sequence has a different digest.
    """
    if marker is None:
        return False
    if marker.window_seq == window_seq and marker.source_digest != source_digest:
        raise CompletionConflict("same window sequence carries a different source digest")
    return marker.window_seq >= window_seq


def completion_payload(window_seq: int, source_digest: bytes) -> dict[str, str]:
    """Build the exact dataset config payload for a completion marker.

    Args:
        window_seq: Durable source-window sequence.
        source_digest: Raw 32-byte source digest.

    Returns:
        Dataset config update payload.

    Raises:
        ValueError: If marker values are invalid.
    """
    if window_seq < 0:
        raise ValueError("window sequence must be non-negative")
    if len(source_digest) != 32:
        raise ValueError(f"source digest must contain 32 bytes, got {len(source_digest)}")
    return {
        LAST_APPLIED_WINDOW_KEY: str(window_seq),
        LAST_APPLIED_DIGEST_KEY: source_digest.hex(),
    }


def finalize_completion_marker(
    uri: str,
    window_seq: int,
    source_digest: bytes,
    telemetry: Telemetry,
    storage_options: Mapping[str, str] | None = None,
    conflict_retries: int = 10,
    retry_backoff_seconds: float = 0.25,
) -> CompletionMarker:
    """Commit or reconcile one monotonic target completion marker idempotently.

    Args:
        uri: Target Lance dataset URI.
        window_seq: Durable source-window sequence.
        source_digest: Raw 32-byte target digest.
        telemetry: Executor-local telemetry facade.
        storage_options: Lance object-store options.
        conflict_retries: Commit-conflict retry budget.
        retry_backoff_seconds: Base conflict retry delay.

    Returns:
        Reopened marker proving the requested work is complete.

    Raises:
        CompletionConflict: If the same window carries a different digest or reconciliation fails.
    """
    payload = completion_payload(window_seq, source_digest)
    options = dict(storage_options or {})

    def update_attempt() -> None:
        """Reopen and conditionally advance the completion marker."""
        dataset = lance.dataset(uri, storage_options=options)
        marker = parse_completion_marker(dataset)
        if completion_is_desired(marker, window_seq, source_digest):
            return
        dataset.update_config(payload)

    try:
        with telemetry.timed("dataset.completion_marker_ms"):
            commit_with_retries(
                update_attempt,
                retries=conflict_retries,
                backoff_seconds=retry_backoff_seconds,
                on_conflict=lambda: telemetry.incr("dataset.completion_conflict_retries"),
            )
    except Exception:
        reconciled_dataset = lance.dataset(uri, storage_options=options)
        reconciled = parse_completion_marker(reconciled_dataset)
        if completion_is_desired(reconciled, window_seq, source_digest):
            telemetry.incr("dataset.completion_ambiguous_success")
            return reconciled
        raise
    reopened = lance.dataset(uri, storage_options=options)
    marker = parse_completion_marker(reopened)
    if not completion_is_desired(marker, window_seq, source_digest) or marker is None:
        raise CompletionConflict("completion marker is not durable after commit")
    return marker
