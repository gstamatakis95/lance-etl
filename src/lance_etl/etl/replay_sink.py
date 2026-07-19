"""Replay-safe Lance tombstone merge for source-sequenced terminal mutations."""

from __future__ import annotations

import logging
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

import lance
import pyarrow as pa
import pyarrow.compute as pc

from lance_etl.etl.sink import DATA_STORAGE_VERSION, dataset_absent
from lance_etl.telemetry import DEFAULT_RETRY_TIMEOUT, Telemetry, commit_with_retries

WINDOW_SEQUENCE_COLUMN: str = "lance_etl_window_seq"
SOURCE_SEQUENCE_COLUMN: str = "lance_etl_source_sequence"
EVENT_DIGEST_COLUMN: str = "lance_etl_event_digest"
DELETED_COLUMN: str = "is_deleted"
VERIFY_KEY_BATCH: int = 512

logger: logging.Logger = logging.getLogger(__name__)


class ReplayConflict(RuntimeError):
    """Signal a same-source-sequence digest conflict or failed post-write reconciliation."""


@dataclass(frozen=True)
class ReplayMergeResult:
    """Logical outcome of one replay-safe merge attempt.

    Attributes:
        rows: Terminal rows presented to the merge.
        tombstones: Terminal delete rows presented to the merge.
        lance_version: Exact reconciled Lance version after the merge.
    """

    rows: int
    tombstones: int
    lance_version: int


def replay_update_condition() -> str:
    """Return the Lance v8 source-watermark conditional update expression.

    A later exact duplicate must still advance the stored source sequence. Otherwise an expired
    worker carrying a distinct mutation from an intermediate snapshot could overwrite the logical
    state after the duplicate window completed.

    Returns:
        Source-sequence ordering predicate.
    """
    return f"target.{SOURCE_SEQUENCE_COLUMN} < source.{SOURCE_SEQUENCE_COLUMN}"


def validate_replay_table(table: pa.Table, key_column: str) -> list[str]:
    """Validate system fields, tombstones, and the release-fixed table shape.

    Args:
        table: Terminal mutation table to validate.
        key_column: Logical record key column.

    Returns:
        Payload columns that tombstones must clear.

    Raises:
        ValueError: If a required field, type, nullability invariant, or tombstone is invalid.
    """
    required: dict[str, pa.DataType] = {
        key_column: pa.string(),
        WINDOW_SEQUENCE_COLUMN: pa.int64(),
        SOURCE_SEQUENCE_COLUMN: pa.int64(),
        EVENT_DIGEST_COLUMN: pa.binary(32),
        DELETED_COLUMN: pa.bool_(),
    }
    name: Any
    expected_type: Any
    for name, expected_type in required.items():
        if name not in table.column_names:
            raise ValueError(f"terminal mutation table is missing required column {name!r}")
        field: pa.Field = table.schema.field(name)
        if field.type != expected_type:
            raise ValueError(f"terminal mutation column {name!r} must be {expected_type}, got {field.type}")
        if table[name].null_count:
            raise ValueError(f"terminal mutation column {name!r} must not contain nulls")
    payload_columns: list[str] = [name for name in table.column_names if name not in required]
    tombstones: pa.ChunkedArray = table[DELETED_COLUMN]
    for name in payload_columns:
        invalid: pa.Array | pa.ChunkedArray = pc.and_(tombstones, pc.is_valid(table[name]))
        if bool(pc.any(invalid).as_py()):
            raise ValueError(f"tombstone rows must explicitly clear payload column {name!r}")
    return payload_columns


def quote_filter_value(value: str) -> str:
    """Quote one string literal for a Lance scanner filter.

    Args:
        value: String literal value.

    Returns:
        Single-quoted value with embedded quotes doubled.
    """
    return "'" + value.replace("'", "''") + "'"


def key_batches(values: list[str]) -> list[list[str]]:
    """Split keys into bounded scanner-filter batches.

    Args:
        values: Keys to split.

    Returns:
        Bounded batches preserving input order.
    """
    return [values[offset : offset + VERIFY_KEY_BATCH] for offset in range(0, len(values), VERIFY_KEY_BATCH)]


def load_key_states(dataset: lance.LanceDataset, key_column: str, keys: list[str]) -> dict[str, tuple[int, bytes]]:
    """Load source sequence and digest for a bounded affected-key set.

    Args:
        dataset: Open dataset handle.
        key_column: Logical record key column.
        keys: Affected key values.

    Returns:
        Mapping from key to stored source sequence and event digest.
    """
    states: dict[str, tuple[int, bytes]] = {}
    batch: Any
    for batch in key_batches(keys):
        if not batch:
            continue
        literals: str = ", ".join(quote_filter_value(value) for value in batch)
        table: pa.Table = dataset.to_table(
            columns=[key_column, SOURCE_SEQUENCE_COLUMN, EVENT_DIGEST_COLUMN],
            filter=f"{key_column} IN ({literals})",
        )
        row: Any
        for row in table.to_pylist():
            states[str(row[key_column])] = (int(row[SOURCE_SEQUENCE_COLUMN]), bytes(row[EVENT_DIGEST_COLUMN]))
    return states


def expected_key_states(table: pa.Table, key_column: str) -> dict[str, tuple[int, bytes]]:
    """Extract the unique expected terminal state for every incoming key.

    Args:
        table: Validated terminal mutation table.
        key_column: Logical record key column.

    Returns:
        Mapping from key to source sequence and event digest.

    Raises:
        ReplayConflict: If the table itself contains inconsistent duplicate keys.
    """
    states: dict[str, tuple[int, bytes]] = {}
    columns: pa.Table = table.select([key_column, SOURCE_SEQUENCE_COLUMN, EVENT_DIGEST_COLUMN])
    row: Any
    for row in columns.to_pylist():
        key: str = str(row[key_column])
        candidate: tuple[int, bytes] = int(row[SOURCE_SEQUENCE_COLUMN]), bytes(row[EVENT_DIGEST_COLUMN])
        previous: tuple[int, bytes] | None = states.get(key)
        if previous is not None and previous != candidate:
            raise ReplayConflict(f"terminal merge input contains inconsistent duplicate key {key!r}")
        states[key] = candidate
    return states


def detect_same_sequence_conflicts(
    existing: Mapping[str, tuple[int, bytes]], incoming: Mapping[str, tuple[int, bytes]]
) -> None:
    """Reject equal source sequences that carry different immutable event digests.

    Args:
        existing: Stored states by key.
        incoming: Incoming states by key.

    Raises:
        ReplayConflict: If an equal source sequence has a different digest.
    """
    key: Any
    incoming_sequence: Any
    incoming_digest: Any
    for key, (incoming_sequence, incoming_digest) in incoming.items():
        current: tuple[int, bytes] | None = existing.get(key)
        if current is None:
            continue
        stored_sequence: Any
        stored_digest: Any
        stored_sequence, stored_digest = current
        if stored_sequence == incoming_sequence and stored_digest != incoming_digest:
            raise ReplayConflict(f"same source sequence carries a different digest for key {key!r}")


def requires_replay_merge(existing: Mapping[str, tuple[int, bytes]], incoming: Mapping[str, tuple[int, bytes]]) -> bool:
    """Return whether any incoming terminal state can advance stored state.

    Args:
        existing: Stored source sequence and digest by key.
        incoming: Incoming source sequence and digest by key.

    Returns:
        True only for a missing key or a greater incoming sequence.
    """
    return any(key not in existing or incoming_state[0] > existing[key][0] for key, incoming_state in incoming.items())


def verify_reconciled_states(
    stored: Mapping[str, tuple[int, bytes]], incoming: Mapping[str, tuple[int, bytes]]
) -> None:
    """Verify every mutation is stored, exactly duplicated, or superseded.

    Args:
        stored: Reopened states after the merge.
        incoming: Expected incoming states.

    Raises:
        ReplayConflict: If a key is absent, regressed, or conflicts at equal sequence.
    """
    key: Any
    incoming_sequence: Any
    incoming_digest: Any
    for key, (incoming_sequence, incoming_digest) in incoming.items():
        current: tuple[int, bytes] | None = stored.get(key)
        if current is None:
            raise ReplayConflict(f"terminal mutation is absent after merge for key {key!r}")
        stored_sequence: Any
        stored_digest: Any
        stored_sequence, stored_digest = current
        if stored_sequence > incoming_sequence:
            continue
        if stored_sequence < incoming_sequence:
            raise ReplayConflict(f"stored source sequence regressed after merge for key {key!r}")
        if stored_digest != incoming_digest:
            raise ReplayConflict(f"same source sequence carries a different digest after merge for key {key!r}")


def open_or_create_replay_dataset(
    uri: str,
    table: pa.Table,
    storage_options: Mapping[str, str] | None,
    max_rows_per_file: int = 1_048_576,
) -> lance.LanceDataset:
    """Open a replay-safe dataset or atomically bootstrap its exact schema.

    Args:
        uri: Dataset URI.
        table: Terminal table supplying the initial release-fixed schema.
        storage_options: Lance object-store options.
        max_rows_per_file: Positive fragment row limit used for dataset creation.

    Returns:
        Open dataset handle.
    """
    if max_rows_per_file < 1:
        raise ValueError("max_rows_per_file must be positive")
    options: dict[str, str] = dict(storage_options or {})
    try:
        return lance.dataset(uri, storage_options=options)
    except (FileNotFoundError, ValueError) as error:
        if not dataset_absent(error):
            raise
        try:
            return lance.write_dataset(
                table.schema.empty_table(),
                uri,
                mode="append",
                storage_options=options,
                enable_v2_manifest_paths=True,
                data_storage_version=DATA_STORAGE_VERSION,
                max_rows_per_file=max_rows_per_file,
            )
        except OSError:
            return lance.dataset(uri, storage_options=options)


def ensure_fixed_schema(dataset: lance.LanceDataset, schema: pa.Schema) -> None:
    """Require an exact release-fixed schema before mutation.

    Args:
        dataset: Open dataset handle.
        schema: Incoming terminal table schema.

    Raises:
        ValueError: If the stored and release schemas differ.
    """
    if dataset.schema != schema:
        raise ValueError(
            f"dataset schema differs from the dataset specification: stored={dataset.schema}, incoming={schema}"
        )


def replay_safe_merge(
    uri: str,
    table: pa.Table,
    telemetry: Telemetry,
    storage_options: Mapping[str, str] | None = None,
    key_column: str = "vector_id",
    conflict_retries: int = 10,
    retry_backoff_seconds: float = 0.25,
    max_rows_per_file: int = 1_048_576,
) -> ReplayMergeResult:
    """Apply terminal mutations idempotently with source-sequence tombstone semantics.

    Args:
        uri: Dataset URI.
        table: Terminal mutation table from exactly one target and source window.
        telemetry: Executor-local telemetry facade.
        storage_options: Lance object-store options.
        key_column: Logical record key column.
        conflict_retries: Commit-conflict retry budget.
        retry_backoff_seconds: Base conflict backoff.
        max_rows_per_file: Positive fragment row limit used for dataset creation.

    Returns:
        Reconciled logical result and exact dataset version.
    """
    validate_replay_table(table, key_column)
    incoming: dict[str, tuple[int, bytes]] = expected_key_states(table, key_column)
    options: dict[str, str] = dict(storage_options or {})

    def merge_attempt() -> dict[str, Any]:
        """Reopen, preflight, and execute one conditional merge attempt."""
        dataset: lance.LanceDataset = open_or_create_replay_dataset(uri, table, options, max_rows_per_file)
        ensure_fixed_schema(dataset, table.schema)
        existing: dict[str, tuple[int, bytes]] = load_key_states(dataset, key_column, list(incoming))
        detect_same_sequence_conflicts(existing, incoming)
        if not requires_replay_merge(existing, incoming):
            telemetry.incr("dataset.replay_noop")
            return {"num_inserted_rows": 0, "num_updated_rows": 0, "num_deleted_rows": 0}
        return (
            dataset.merge_insert(on=[key_column])
            .when_matched_update_all(condition=replay_update_condition())
            .when_not_matched_insert_all()
            .conflict_retries(conflict_retries)
            .retry_timeout(DEFAULT_RETRY_TIMEOUT)
            .execute(table)
        )

    with telemetry.timed("dataset.replay_merge_ms"):
        commit_with_retries(
            merge_attempt,
            retries=conflict_retries,
            backoff_seconds=retry_backoff_seconds,
            on_conflict=lambda: telemetry.incr("dataset.merge_conflict_retries"),
        )
    reopened: lance.LanceDataset = lance.dataset(uri, storage_options=options)
    stored: dict[str, tuple[int, bytes]] = load_key_states(reopened, key_column, list(incoming))
    verify_reconciled_states(stored, incoming)
    tombstones: int = int(pc.sum(pc.cast(table[DELETED_COLUMN], pa.int64())).as_py() or 0)
    telemetry.distribution("dataset.terminal_rows", table.num_rows)
    telemetry.distribution("dataset.tombstones", tombstones)
    return ReplayMergeResult(rows=table.num_rows, tombstones=tombstones, lance_version=reopened.version)


def replay_table_chunks(table: pa.Table, max_rows: int, max_bytes: int) -> list[pa.Table]:
    """Split one terminal table by both row and byte limits.

    Args:
        table: Terminal mutation table to split without reordering rows.
        max_rows: Positive maximum rows per replay merge.
        max_bytes: Positive approximate byte budget per replay merge.

    Returns:
        Ordered non-empty table slices. A single oversized row remains one slice.

    Raises:
        ValueError: If either limit is not positive.
    """
    if max_rows < 1 or max_bytes < 1:
        raise ValueError("replay merge row and byte limits must be positive")
    chunks: list[pa.Table] = []
    offset: int = 0
    while offset < table.num_rows:
        length: int = min(max_rows, table.num_rows - offset)
        candidate: pa.Table = table.slice(offset, length)
        while candidate.nbytes > max_bytes and length > 1:
            scaled_length: int = max(1, int(length * max_bytes / candidate.nbytes))
            length = min(length - 1, scaled_length)
            candidate = table.slice(offset, length)
        chunks.append(candidate)
        offset += length
    return chunks
