"""Tests for replay-safe mutation identity and snapshot-local collapse."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from lance_etl.etl.digest import canonical_event_digest, canonical_source_digest, encode_value
from lance_etl.etl.mutation import (
    MutationConflict,
    MutationInput,
    TerminalMutation,
    collapse_snapshot_mutations,
    materialize_post_image,
    terminal_source_digest,
)


def mutation(payload: dict[str, object], operation: str = "update") -> MutationInput:
    """Build a normalized mutation fixture.

    Args:
        payload: Payload for the fixture.
        operation: Mutation operation.

    Returns:
        Mutation fixture with stable routing and time.
    """
    return MutationInput(
        tenant_id="tenant",
        namespace="namespace",
        org_id="org",
        record_id="vector-1",
        operation=operation,
        ts=datetime(2026, 7, 14, 10, 11, 12, 123456, tzinfo=UTC),
        payload=payload,
    )


def test_event_digest_is_stable_across_mapping_order() -> None:
    """Mapping insertion order does not change immutable event identity."""
    first: MutationInput = mutation({"text": "hello", "metadata": {"b": "2", "a": "1"}})
    second: MutationInput = mutation({"metadata": {"a": "1", "b": "2"}, "text": "hello"})
    first_digest: bytes = canonical_event_digest(first.target, first.record_id, "upsert", first.ts, first.payload)
    second_digest: bytes = canonical_event_digest(second.target, second.record_id, "upsert", second.ts, second.payload)
    assert first_digest == second_digest
    assert len(first_digest) == 32


def test_event_digest_rejects_naive_timestamp_and_non_finite_vector() -> None:
    """Ambiguous timestamps and non-finite numeric values fail before mutation writes."""
    row: MutationInput = mutation({"vector": [1.0]})
    with pytest.raises(ValueError, match="timezone-aware"):
        canonical_event_digest(row.target, row.record_id, "upsert", row.ts.replace(tzinfo=None), row.payload)
    with pytest.raises(ValueError, match="non-finite"):
        encode_value([1.0, float("nan")])


def test_exact_duplicates_collapse_across_one_hundred_retries() -> None:
    """Repeated exact delivery converges to one terminal mutation and one source digest."""
    rows: list[MutationInput] = [mutation({"text": "same"}) for attempt in range(100)]
    terminal: list[TerminalMutation] = collapse_snapshot_mutations(rows, window_seq=7, source_sequence=91)
    assert len(terminal) == 1
    assert terminal[0].source_sequence == 91
    expected: bytes = terminal_source_digest(terminal)
    replay_rows: list[MutationInput]
    for replay_rows in [rows] * 100:
        replay: list[TerminalMutation] = collapse_snapshot_mutations(replay_rows, window_seq=7, source_sequence=91)
        assert terminal_source_digest(replay) == expected


def test_distinct_same_snapshot_mutations_block() -> None:
    """Distinct mutations for one key in one snapshot have no order and block."""
    with pytest.raises(MutationConflict, match="distinct unordered"):
        collapse_snapshot_mutations(
            [mutation({"text": "first"}), mutation({"text": "second"})],
            window_seq=8,
            source_sequence=92,
        )


def test_later_snapshot_source_sequence_is_delivery_order() -> None:
    """A later snapshot produces a greater source sequence even with older event time."""
    first: TerminalMutation = collapse_snapshot_mutations([mutation({"text": "first"})], 1, 10)[0]
    older_time: MutationInput = MutationInput(
        tenant_id=first.target[0],
        namespace=first.target[1],
        org_id=first.target[2],
        record_id=first.record_id,
        operation="update",
        ts=datetime(2020, 1, 1, tzinfo=UTC),
        payload={"text": "correction"},
    )
    later: TerminalMutation = collapse_snapshot_mutations([older_time], 2, 11)[0]
    assert later.source_sequence > first.source_sequence
    assert later.ts < first.ts
    assert later.event_digest != first.event_digest


def test_complete_post_image_nulls_omissions_and_rejects_unknown_fields() -> None:
    """Omitted allowed fields clear while arbitrary schema growth fails closed."""
    assert materialize_post_image({"text": "value"}, ["text", "category"]) == {
        "text": "value",
        "category": None,
    }
    with pytest.raises(ValueError, match="outside the dataset specification"):
        materialize_post_image({"surprise": 1}, ["text"])


def test_source_digest_is_partition_and_input_order_independent() -> None:
    """Terminal tuple ordering does not change the durable source digest."""
    first: bytes = b"a" * 32
    second: bytes = b"b" * 32
    forward: bytes = canonical_source_digest([("z", 2, second), ("a", 2, first)])
    reverse: bytes = canonical_source_digest([("a", 2, first), ("z", 2, second)])
    assert forward == reverse


def test_source_digest_rejects_duplicate_keys_and_negative_sequences() -> None:
    """Invalid terminal identities cannot produce an order-dependent durable digest."""
    digest: bytes = b"a" * 32
    with pytest.raises(ValueError, match="duplicate record id"):
        canonical_source_digest([("duplicate", 1, digest), ("duplicate", 1, digest)])
    with pytest.raises(ValueError, match="non-negative"):
        canonical_source_digest([("record", -1, digest)])


def test_mutation_collapse_rejects_invalid_sequence_metadata() -> None:
    """Terminal rows always fit the non-negative signed int64 storage contract."""
    with pytest.raises(ValueError, match="window sequence"):
        collapse_snapshot_mutations([mutation({"text": "value"})], -1, 1)
    with pytest.raises(ValueError, match="source sequence"):
        collapse_snapshot_mutations([mutation({"text": "value"})], 1, 1 << 63)


def test_source_digest_rejects_mixed_targets() -> None:
    """One target digest cannot accidentally include another target's mutation."""
    first: TerminalMutation = collapse_snapshot_mutations([mutation({"text": "first"})], 1, 1)[0]
    other: MutationInput = MutationInput(
        tenant_id="other",
        namespace="namespace",
        org_id="org",
        record_id="vector-2",
        operation="delete",
        ts=datetime(2026, 7, 14, tzinfo=UTC),
        payload={},
    )
    second: TerminalMutation = collapse_snapshot_mutations([other], 1, 1)[0]
    with pytest.raises(ValueError, match="one target"):
        terminal_source_digest([first, second])
