"""Deterministic snapshot mutation collapse and complete post-image handling."""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from lance_etl.etl.digest import canonical_event_digest, canonical_source_digest

UPSERT_OPERATIONS: frozenset[str] = frozenset({"insert", "update", "upsert", "i", "u"})
DELETE_OPERATIONS: frozenset[str] = frozenset({"delete", "d"})


class MutationConflict(ValueError):
    """Signal that one Iceberg snapshot contains unordered distinct mutations for a key."""


@dataclass(frozen=True)
class MutationInput:
    """Normalized source row before snapshot-local conflict detection.

    Attributes:
        tenant_id: Tenant routing component.
        namespace: Namespace routing component.
        org_id: Organization routing component.
        record_id: Logical record identity.
        operation: Source operation spelling.
        ts: Query and retention timestamp.
        payload: Complete source payload excluding delivery metadata.
    """

    tenant_id: str
    namespace: str
    org_id: str
    record_id: str
    operation: str
    ts: datetime
    payload: Mapping[str, Any]

    @property
    def target(self) -> tuple[str, str, str]:
        """Return the canonical tenant, namespace, and organization target tuple."""
        return self.tenant_id, self.namespace, self.org_id


@dataclass(frozen=True)
class TerminalMutation:
    """One deterministic terminal mutation for a key in one source window.

    Attributes:
        target: Canonical routing tuple.
        record_id: Logical record identity.
        operation: Normalized `upsert` or `delete` operation.
        ts: Query and retention timestamp.
        payload: Complete normalized payload.
        window_seq: Durable source-window sequence.
        source_sequence: Iceberg sequence number for the source snapshot.
        event_digest: Frozen 32-byte mutation identity.
    """

    target: tuple[str, str, str]
    record_id: str
    operation: str
    ts: datetime
    payload: Mapping[str, Any]
    window_seq: int
    source_sequence: int
    event_digest: bytes

    @property
    def is_deleted(self) -> bool:
        """Return whether this terminal mutation is a tombstone."""
        return self.operation == "delete"


def normalize_operation(operation: str) -> str:
    """Normalize a source operation to `upsert` or `delete`.

    Args:
        operation: Source operation spelling.

    Returns:
        Normalized operation.

    Raises:
        ValueError: If the operation is unsupported.
    """
    normalized: Any = operation.strip().lower()
    if normalized in UPSERT_OPERATIONS:
        return "upsert"
    if normalized in DELETE_OPERATIONS:
        return "delete"
    raise ValueError(f"unsupported mutation operation: {operation!r}")


def materialize_post_image(payload: Mapping[str, Any], allowed_fields: Sequence[str]) -> dict[str, Any]:
    """Materialize every allowed payload field and reject release-unknown fields.

    Args:
        payload: Incoming complete post-image fields.
        allowed_fields: Release-owned payload field names.

    Returns:
        Dictionary containing every allowed field, with omitted fields set to null.

    Raises:
        ValueError: If the payload contains a field outside the dataset specification.
    """
    allowed: Any = frozenset(allowed_fields)
    unknown: Any = sorted(set(payload) - allowed)
    if unknown:
        raise ValueError(f"payload contains fields outside the dataset specification: {unknown}")
    return {field: payload.get(field) for field in allowed_fields}


def collapse_snapshot_mutations(
    rows: Iterable[MutationInput],
    window_seq: int,
    source_sequence: int,
) -> list[TerminalMutation]:
    """Collapse exact duplicates and reject unordered distinct mutations in one snapshot.

    Args:
        rows: Normalized rows from exactly one Iceberg snapshot.
        window_seq: Durable source-window sequence.
        source_sequence: Iceberg sequence number attached to every resulting mutation.

    Returns:
        Deterministically ordered terminal mutations.

    Raises:
        MutationConflict: If one key has more than one distinct event digest.
        ValueError: If a sequence is invalid or a row carries an unsupported operation.
    """
    if window_seq < 0 or window_seq >= 1 << 63:
        raise ValueError(f"window sequence is outside non-negative signed 64-bit range: {window_seq}")
    if source_sequence < 0 or source_sequence >= 1 << 63:
        raise ValueError(f"source sequence is outside non-negative signed 64-bit range: {source_sequence}")
    terminal_by_key: dict[tuple[tuple[str, str, str], str], TerminalMutation] = {}
    row: Any
    for row in rows:
        operation: Any = normalize_operation(row.operation)
        digest: Any = canonical_event_digest(row.target, row.record_id, operation, row.ts, row.payload)
        terminal: Any = TerminalMutation(
            target=row.target,
            record_id=row.record_id,
            operation=operation,
            ts=row.ts,
            payload=dict(row.payload),
            window_seq=window_seq,
            source_sequence=source_sequence,
            event_digest=digest,
        )
        key: Any = row.target, row.record_id
        previous: Any = terminal_by_key.get(key)
        if previous is not None and previous.event_digest != digest:
            raise MutationConflict(
                f"source snapshot has distinct unordered mutations for target={row.target!r}, "
                f"record_id={row.record_id!r}"
            )
        terminal_by_key[key] = terminal
    return [terminal_by_key[key] for key in sorted(terminal_by_key)]


def terminal_source_digest(rows: Iterable[TerminalMutation]) -> bytes:
    """Compute the durable target digest for terminal mutations.

    Args:
        rows: Terminal mutations for one target and source window.

    Returns:
        Raw 32-byte source digest.

    Raises:
        ValueError: If rows from different targets are mixed.
    """
    materialized: Any = list(rows)
    targets: Any = {row.target for row in materialized}
    if len(targets) > 1:
        raise ValueError("source digest rows must belong to one target")
    return canonical_source_digest((row.record_id, row.source_sequence, row.event_digest) for row in materialized)
