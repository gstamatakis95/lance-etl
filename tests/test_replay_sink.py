"""Real-Lance tests for source-sequenced replay-safe tombstone merging."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import MagicMock

import lance
import pyarrow as pa
import pyarrow.compute as pc
import pytest

from lance_etl.etl.replay_sink import (
    DELETED_COLUMN,
    EVENT_DIGEST_COLUMN,
    SOURCE_SEQUENCE_COLUMN,
    WINDOW_SEQUENCE_COLUMN,
    ReplayConflict,
    load_key_states,
    replay_safe_merge,
    replay_table_chunks,
    replay_update_condition,
)
from lance_etl.telemetry import Telemetry


def terminal_table(
    record_id: str,
    source_sequence: int,
    digest_byte: bytes,
    text: str | None,
    deleted: bool = False,
) -> pa.Table:
    """Build one release-shaped terminal mutation table.

    Args:
        record_id: Logical key.
        source_sequence: Iceberg source sequence.
        digest_byte: One byte repeated to create a test digest.
        text: Payload text or null.
        deleted: Tombstone marker.

    Returns:
        One-row terminal table.
    """
    return pa.table(
        {
            "record_id": pa.array([record_id], type=pa.string()),
            "text": pa.array([text], type=pa.string()),
            WINDOW_SEQUENCE_COLUMN: pa.array([source_sequence], type=pa.int64()),
            SOURCE_SEQUENCE_COLUMN: pa.array([source_sequence], type=pa.int64()),
            EVENT_DIGEST_COLUMN: pa.array([digest_byte * 32], type=pa.binary(32)),
            DELETED_COLUMN: pa.array([deleted], type=pa.bool_()),
        }
    )


def live_rows(uri: str) -> list[dict[str, object]]:
    """Read logical live rows from a test dataset.

    Args:
        uri: Dataset URI.

    Returns:
        Live row dictionaries.
    """
    return lance.dataset(uri).to_table(filter=f"{DELETED_COLUMN} = false").to_pylist()


def test_update_condition_matches_frozen_contract() -> None:
    """The sink advances its source watermark without using event time."""
    condition: str = replay_update_condition()
    assert f"target.{SOURCE_SEQUENCE_COLUMN} < source.{SOURCE_SEQUENCE_COLUMN}" in condition
    assert EVENT_DIGEST_COLUMN not in condition
    assert "ts" not in condition


def test_replay_table_chunks_applies_row_and_byte_limits() -> None:
    """Replay chunking preserves rows while applying both PostgreSQL-owned limits."""
    table: pa.Table = pa.table({"value": ["a" * 40, "b" * 40, "c", "d"]})
    chunks: list[pa.Table] = replay_table_chunks(table, max_rows=3, max_bytes=50)
    restored: pa.Table = pa.concat_tables(chunks)
    assert restored.equals(table)
    assert all(chunk.num_rows <= 3 for chunk in chunks)
    assert all(chunk.nbytes <= 50 or chunk.num_rows == 1 for chunk in chunks)


def test_one_hundred_retries_converge_without_duplicates(tmp_path: Path, telemetry: Telemetry) -> None:
    """Repeating one work item one hundred times leaves one physical logical key."""
    uri: str = str(tmp_path / "retry.lance")
    table: pa.Table = terminal_table("id", 1, b"a", "first")
    replay_safe_merge(uri, table, telemetry, retry_backoff_seconds=0)
    first_version: int = lance.dataset(uri).version
    replay_table: pa.Table
    for replay_table in [table] * 99:
        replay_safe_merge(uri, replay_table, telemetry, retry_backoff_seconds=0)
    dataset: lance.LanceDataset = lance.dataset(uri)
    rows: list[dict[str, object]] = dataset.to_table().to_pylist()
    assert dataset.version == first_version
    assert len(rows) == 1
    assert rows[0]["text"] == "first"


def test_duplicate_terminal_input_is_rejected_before_bootstrap(tmp_path: Path, telemetry: Telemetry) -> None:
    """Even exact duplicate keys cannot create duplicate physical rows in a new dataset."""
    path: Path = tmp_path / "duplicate-input.lance"
    row: pa.Table = terminal_table("id", 1, b"a", "first")
    duplicated: pa.Table = pa.concat_tables([row, row])
    with pytest.raises(ReplayConflict, match="duplicate key"):
        replay_safe_merge(str(path), duplicated, telemetry)
    assert not path.exists()


def test_preexisting_duplicate_keys_fail_reconciliation(tmp_path: Path, telemetry: Telemetry) -> None:
    """A corrupted dataset cannot be mistaken for one reconciled logical key."""
    uri: str = str(tmp_path / "duplicate-stored.lance")
    row: pa.Table = terminal_table("id", 1, b"a", "first")
    lance.write_dataset(pa.concat_tables([row, row]), uri)
    with pytest.raises(ReplayConflict, match="duplicate stored key"):
        replay_safe_merge(uri, row, telemetry)


def test_key_state_lookup_uses_typed_arrow_filter() -> None:
    """Record ids are bound as typed values instead of interpolated into SQL text."""
    key: str = "id' OR record_id IS NOT NULL"
    stored: pa.Table = pa.table(
        {
            "record_id": pa.array([key], type=pa.string()),
            SOURCE_SEQUENCE_COLUMN: pa.array([3], type=pa.int64()),
            EVENT_DIGEST_COLUMN: pa.array([b"a" * 32], type=pa.binary(32)),
        }
    )
    dataset: MagicMock = MagicMock()
    dataset.to_table.return_value = stored

    assert load_key_states(dataset, "record_id", [key]) == {key: (3, b"a" * 32)}
    assert isinstance(dataset.to_table.call_args.kwargs["filter"], pc.Expression)


@pytest.mark.parametrize("column", [WINDOW_SEQUENCE_COLUMN, SOURCE_SEQUENCE_COLUMN])
def test_negative_replay_sequences_are_rejected(column: str, tmp_path: Path, telemetry: Telemetry) -> None:
    """Replay ordering metadata must fit its non-negative source contract.

    Args:
        column: System sequence column made invalid.
        tmp_path: Temporary dataset root.
        telemetry: Offline telemetry facade.
    """
    path: Path = tmp_path / f"negative-{column}.lance"
    table: pa.Table = terminal_table("id", 1, b"a", "first")
    index: int = table.schema.get_field_index(column)
    table = table.set_column(index, column, pa.array([-1], type=pa.int64()))
    with pytest.raises(ValueError, match="non-negative"):
        replay_safe_merge(str(path), table, telemetry)
    assert not path.exists()


def test_older_source_work_cannot_overwrite_newer_state(tmp_path: Path, telemetry: Telemetry) -> None:
    """A lower Iceberg source sequence becomes a logical no-op."""
    uri: str = str(tmp_path / "stale.lance")
    replay_safe_merge(uri, terminal_table("id", 2, b"b", "new"), telemetry)
    replay_safe_merge(uri, terminal_table("id", 1, b"a", "old"), telemetry)
    row: dict[str, object] = lance.dataset(uri).to_table().to_pylist()[0]
    assert row["text"] == "new"
    assert row[SOURCE_SEQUENCE_COLUMN] == 2


def test_later_exact_duplicate_advances_watermark_against_intermediate_zombie(
    tmp_path: Path, telemetry: Telemetry
) -> None:
    """A later redelivery prevents an expired intermediate worker from changing state."""
    uri: str = str(tmp_path / "duplicate-watermark.lance")
    replay_safe_merge(uri, terminal_table("id", 10, b"a", "stable"), telemetry)
    replay_safe_merge(uri, terminal_table("id", 12, b"a", "stable"), telemetry)
    replay_safe_merge(uri, terminal_table("id", 11, b"b", "zombie"), telemetry)
    row: dict[str, object] = lance.dataset(uri).to_table().to_pylist()[0]
    assert row["text"] == "stable"
    assert row[SOURCE_SEQUENCE_COLUMN] == 12


def test_same_sequence_different_digest_blocks_before_write(tmp_path: Path, telemetry: Telemetry) -> None:
    """An unordered equal-sequence conflict cannot alter stored state."""
    uri: str = str(tmp_path / "conflict.lance")
    replay_safe_merge(uri, terminal_table("id", 4, b"a", "first"), telemetry)
    with pytest.raises(ReplayConflict, match="same source sequence"):
        replay_safe_merge(uri, terminal_table("id", 4, b"b", "second"), telemetry)
    assert lance.dataset(uri).to_table().to_pylist()[0]["text"] == "first"


def test_tombstone_blocks_old_replay_and_later_recreate_wins(tmp_path: Path, telemetry: Telemetry) -> None:
    """Delete, stale retry, and later recreate follow Iceberg arrival order."""
    uri: str = str(tmp_path / "tombstone.lance")
    replay_safe_merge(uri, terminal_table("id", 1, b"a", "first"), telemetry)
    replay_safe_merge(uri, terminal_table("id", 2, b"b", None, deleted=True), telemetry)
    assert live_rows(uri) == []
    replay_safe_merge(uri, terminal_table("id", 1, b"a", "first"), telemetry)
    assert live_rows(uri) == []
    replay_safe_merge(uri, terminal_table("id", 3, b"c", "recreated"), telemetry)
    assert live_rows(uri)[0]["text"] == "recreated"


def test_tombstone_requires_all_payload_fields_null(tmp_path: Path, telemetry: Telemetry) -> None:
    """A delete cannot retain stale payload data."""
    uri: str = str(tmp_path / "bad-tombstone.lance")
    with pytest.raises(ValueError, match="explicitly clear"):
        replay_safe_merge(uri, terminal_table("id", 1, b"a", "not-cleared", deleted=True), telemetry)


def test_tombstone_carries_delete_time_ts(tmp_path: Path, telemetry: Telemetry) -> None:
    """A tombstone may carry the delete mutation's event ts, which the retention pass expires on.

    The event-time column is not a payload column a tombstone must clear, so a delete stamped with
    a real ts merges without raising and stores that ts, while true payload fields stay null.
    """
    uri: str = str(tmp_path / "ts-tombstone.lance")
    delete_ts: datetime = datetime(2025, 6, 1, 12, 0, 0, tzinfo=UTC)
    table: pa.Table = pa.table(
        {
            "record_id": pa.array(["id"], type=pa.string()),
            "ts": pa.array([delete_ts], type=pa.timestamp("us", tz="UTC")),
            "text": pa.array([None], type=pa.string()),
            WINDOW_SEQUENCE_COLUMN: pa.array([2], type=pa.int64()),
            SOURCE_SEQUENCE_COLUMN: pa.array([2], type=pa.int64()),
            EVENT_DIGEST_COLUMN: pa.array([b"a" * 32], type=pa.binary(32)),
            DELETED_COLUMN: pa.array([True], type=pa.bool_()),
        }
    )
    replay_safe_merge(uri, table, telemetry)
    stored: dict[str, object] = lance.dataset(uri).to_table().to_pylist()[0]
    assert stored[DELETED_COLUMN] is True
    assert stored["ts"] == delete_ts
    assert stored["text"] is None


def test_schema_growth_is_rejected(tmp_path: Path, telemetry: Telemetry) -> None:
    """A target cannot grow arbitrary fields outside its frozen dataset specification."""
    uri: str = str(tmp_path / "schema.lance")
    replay_safe_merge(uri, terminal_table("id", 1, b"a", "first"), telemetry)
    changed: pa.Table = terminal_table("id", 2, b"b", "second").append_column("surprise", pa.array(["value"]))
    with pytest.raises(ValueError, match="dataset specification"):
        replay_safe_merge(uri, changed, telemetry)
